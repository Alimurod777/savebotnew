"""
/group command - Forum Topic Analyzer for Telegram Supergroups

Features:
- Accepts supergroup ID, t.me invite link, or t.me/c/ internal link
- Uses USER session (never bot session)
- Detects forum-enabled supergroups
- Enumerates all topics with accurate statistics
- Generates correct topic-scoped links: /c/GROUP/TOPIC/MSG

Link Format (CRITICAL):
    https://t.me/c/<GROUP_ID>/<TOPIC_ID>/<MESSAGE_ID>
    
    - GROUP_ID: supergroup ID without -100 prefix
    - TOPIC_ID: topic root message ID (forum topic identifier)
    - MESSAGE_ID: actual message ID within the topic

Message Ordering:
    - First/Last determined by message.date (NOT message_id)
    - Message IDs are non-sequential within topics
    - Deleted messages are skipped
"""

import asyncio
import re
import logging
from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.raw import functions, types
from pyrogram.errors import (
    FloodWait,
    ChannelPrivate,
    ChannelInvalid,
    PeerIdInvalid,
    ChatAdminRequired,
    RPCError,
)

from config import API_ID, API_HASH
from database.async_db import async_db
from core.reply_compat import build_reply_kwargs_from_message

from TechVJ.session_handler import (
    create_user_session,
    SessionInvalidError,
    SessionConnectionError,
)
from core.structured_log import op_log

logger = logging.getLogger(__name__)

# Timeouts
RPC_TIMEOUT = 30.0
TOPIC_FETCH_TIMEOUT = 60.0
MESSAGE_SCAN_TIMEOUT = 120.0

# Limits
MAX_TOPICS_TO_SHOW = 50
MAX_MESSAGES_PER_TOPIC = 10000


# ==================== ERROR HANDLING ====================

@dataclass
class GroupError:
    """Classified error with user message."""
    error_type: str
    user_message: str
    is_fatal: bool = False


def classify_group_error(e: Exception) -> GroupError:
    """Classify exception into user-friendly error."""
    error_str = str(e).upper()
    error_name = type(e).__name__
    
    if isinstance(e, SessionInvalidError):
        return GroupError("session_invalid", "Session expired. Please /login again.", True)
    
    if isinstance(e, SessionConnectionError):
        return GroupError("connection", f"Connection error: {e}", False)
    
    if isinstance(e, ChannelPrivate) or "CHANNEL_PRIVATE" in error_str:
        return GroupError("private", "This group is private. You need to be a member.", False)
    
    if isinstance(e, ChannelInvalid) or "CHANNEL_INVALID" in error_str:
        return GroupError("invalid", "Invalid group. It may not exist.", False)
    
    if isinstance(e, PeerIdInvalid) or "PEER_ID_INVALID" in error_str:
        return GroupError("peer", "Cannot access this group. Make sure you're a member.", False)
    
    if isinstance(e, ChatAdminRequired) or "ADMIN" in error_str:
        return GroupError("admin", "Admin privileges required for this operation.", False)
    
    if isinstance(e, asyncio.TimeoutError) or "TIMEOUT" in error_str:
        return GroupError("timeout", "Request timed out. The group may be too large.", False)
    
    if isinstance(e, FloodWait):
        wait = getattr(e, 'value', getattr(e, 'x', 30))
        return GroupError("flood", f"Rate limited. Please wait {wait} seconds.", False)
    
    return GroupError("unknown", f"Error: {error_name} - {e}", False)


# ==================== INPUT PARSING ====================

@dataclass
class ParsedGroupInput:
    """Parsed group identifier."""
    identifier: Any  # int (ID) or str (username/invite)
    input_type: str  # "id", "username", "invite", "internal"


def parse_group_input(text: str) -> Tuple[Optional[ParsedGroupInput], Optional[str]]:
    """
    Parse group input from user.
    
    Supported formats:
    - Numeric ID: -1001234567890 or 1234567890
    - Username: @groupname or groupname
    - Invite link: https://t.me/+xxxxx or https://t.me/joinchat/xxxxx
    - Internal link: https://t.me/c/1234567890/123
    
    Returns:
        (ParsedGroupInput or None, error_message or None)
    """
    text = text.strip()
    
    # Numeric ID
    if text.lstrip('-').isdigit():
        group_id = int(text)
        # Ensure it has -100 prefix for supergroups
        if group_id > 0:
            group_id = int(f"-100{group_id}")
        elif not str(group_id).startswith("-100"):
            group_id = int(f"-100{abs(group_id)}")
        return ParsedGroupInput(group_id, "id"), None
    
    # Internal link: https://t.me/c/1234567890/...
    internal_match = re.match(r'^https?://t\.me/c/(\d+)', text)
    if internal_match:
        group_id = int(f"-100{internal_match.group(1)}")
        return ParsedGroupInput(group_id, "internal"), None
    
    # Invite link: https://t.me/+xxx or https://t.me/joinchat/xxx
    invite_match = re.match(r'^https?://t\.me/(\+[a-zA-Z0-9_-]+|joinchat/[a-zA-Z0-9_-]+)', text)
    if invite_match:
        return ParsedGroupInput(text, "invite"), None
    
    # Username link: https://t.me/groupname
    username_link = re.match(r'^https?://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/?$', text)
    if username_link:
        return ParsedGroupInput(username_link.group(1), "username"), None
    
    # Plain username: @groupname or groupname
    if text.startswith('@'):
        text = text[1:]
    if re.match(r'^[a-zA-Z][a-zA-Z0-9_]{3,}$', text):
        return ParsedGroupInput(text, "username"), None
    
    return None, (
        "Invalid input format.\n\n"
        "Supported formats:\n"
        "• Group ID: `-1001234567890`\n"
        "• Username: `@groupname`\n"
        "• Link: `https://t.me/groupname`\n"
        "• Internal: `https://t.me/c/1234567890/...`"
    )


# ==================== TOPIC DATA STRUCTURES ====================

@dataclass
class TopicMessage:
    """A message within a topic."""
    message_id: int
    date: datetime
    is_topic_starter: bool = False


@dataclass
class TopicInfo:
    """Complete topic information."""
    topic_id: int
    title: str
    icon_emoji: Optional[str] = None
    message_count: int = 0
    first_message: Optional[TopicMessage] = None
    last_message: Optional[TopicMessage] = None
    is_closed: bool = False
    is_hidden: bool = False


@dataclass
class GroupAnalysis:
    """Complete group analysis result."""
    group_id: int
    group_id_short: str  # Without -100 prefix
    title: str
    username: Optional[str] = None
    is_forum: bool = False
    member_count: int = 0
    topics: List[TopicInfo] = field(default_factory=list)
    total_topics: int = 0
    error: Optional[GroupError] = None


# ==================== LINK GENERATION ====================

def build_topic_message_link(group_id_short: str, topic_id: int, message_id: int) -> str:
    """
    Build correct topic message link.
    
    Format: https://t.me/c/<GROUP_ID>/<TOPIC_ID>/<MESSAGE_ID>
    
    CRITICAL: This is the ONLY correct format for topic messages.
    """
    return f"https://t.me/c/{group_id_short}/{topic_id}/{message_id}"


def build_topic_link(group_id_short: str, topic_id: int) -> str:
    """
    Build link to topic itself (shows topic header).
    
    Format: https://t.me/c/<GROUP_ID>/<TOPIC_ID>
    """
    return f"https://t.me/c/{group_id_short}/{topic_id}"


# ==================== TOPIC ENUMERATION ====================

async def get_forum_topics_raw(client: Client, chat_id: int) -> List[dict]:
    """
    Get forum topics using raw MTProto API.
    
    Uses channels.getForumTopics which returns all topics.
    """
    topics = []
    offset_date = 0
    offset_id = 0
    offset_topic = 0
    
    try:
        # Resolve peer
        peer = await client.resolve_peer(chat_id)
        
        while True:
            try:
                result = await asyncio.wait_for(
                    client.invoke(
                        functions.channels.GetForumTopics(
                            channel=peer,
                            offset_date=offset_date,
                            offset_id=offset_id,
                            offset_topic=offset_topic,
                            limit=100,
                            q=""  # Empty search query = all topics
                        )
                    ),
                    timeout=TOPIC_FETCH_TIMEOUT
                )
                
                if not result.topics:
                    break
                
                for topic in result.topics:
                    if isinstance(topic, types.ForumTopic):
                        topics.append({
                            'id': topic.id,
                            'title': topic.title,
                            'icon_emoji_id': getattr(topic, 'icon_emoji_id', None),
                            'closed': getattr(topic, 'closed', False),
                            'hidden': getattr(topic, 'hidden', False),
                            'top_message': getattr(topic, 'top_message', 0),
                        })
                
                # Check if we have more
                if len(result.topics) < 100:
                    break
                
                # Update offsets for pagination
                last_topic = result.topics[-1]
                if isinstance(last_topic, types.ForumTopic):
                    offset_topic = last_topic.id
                    offset_id = getattr(last_topic, 'top_message', 0)
                else:
                    break
                    
            except FloodWait as e:
                wait = getattr(e, 'value', getattr(e, 'x', 30))
                logger.warning(f"FloodWait {wait}s in get_forum_topics")
                await asyncio.sleep(min(wait, 60))
                continue
                
    except Exception as e:
        logger.warning(f"get_forum_topics_raw error: {e}")
        raise
    
    return topics


async def get_topic_messages_by_date(
    client: Client,
    chat_id: int,
    topic_id: int,
    limit: int = MAX_MESSAGES_PER_TOPIC
) -> Tuple[int, Optional[TopicMessage], Optional[TopicMessage]]:
    """
    Get topic message statistics sorted by DATE.
    
    CRITICAL:
    - Iterates messages where reply_to_top_message_id == topic_id
    - Finds first/last by message.date (NOT message_id)
    - Message IDs are non-sequential in topics
    
    Returns:
        (message_count, first_message, last_message)
    """
    messages_by_date: List[TopicMessage] = []
    count = 0
    
    try:
        # Iterate topic messages using search or history
        # Pyrogram's get_chat_history doesn't filter by topic directly,
        # so we use search_messages with reply_to filter or iterate and filter
        
        # Method: Use iter_history and filter by reply_to_top_message_id
        # This is the most reliable method for forum topics
        async for msg in client.get_chat_history(chat_id, limit=limit):
            # Check if message belongs to this topic
            msg_topic_id = None
            
            # Check reply_to_top_message_id (primary method)
            if hasattr(msg, 'reply_to_top_message_id') and msg.reply_to_top_message_id:
                msg_topic_id = msg.reply_to_top_message_id
            # Check reply_to_message_id for direct topic replies
            elif hasattr(msg, 'reply_to_message_id') and msg.reply_to_message_id:
                # Could be a reply to the topic starter
                msg_topic_id = msg.reply_to_message_id
            # Check if this IS the topic starter message
            elif msg.id == topic_id:
                msg_topic_id = topic_id
            
            if msg_topic_id == topic_id:
                count += 1
                if msg.date:
                    messages_by_date.append(TopicMessage(
                        message_id=msg.id,
                        date=msg.date,
                        is_topic_starter=(msg.id == topic_id)
                    ))
            
            # Early exit if we've scanned enough
            if count >= limit:
                break
        
        if not messages_by_date:
            return count, None, None
        
        # CRITICAL: Sort by DATE, not message_id
        messages_by_date.sort(key=lambda m: m.date)
        
        first_msg = messages_by_date[0]
        last_msg = messages_by_date[-1]
        
        return count, first_msg, last_msg
        
    except asyncio.TimeoutError:
        logger.warning(f"Timeout scanning topic {topic_id}")
        return count, None, None
    except Exception as e:
        logger.warning(f"Error scanning topic {topic_id}: {e}")
        return count, None, None


async def scan_topic_with_raw_api(
    client: Client,
    chat_id: int,
    topic_id: int,
    limit: int = MAX_MESSAGES_PER_TOPIC
) -> Tuple[int, Optional[TopicMessage], Optional[TopicMessage]]:
    """
    Scan topic messages using raw MTProto GetReplies.
    
    CRITICAL: Chronology is derived from message.date, NOT message IDs.
    Only tracks first/last by date to minimize memory usage.
    """
    first_msg: Optional[TopicMessage] = None
    last_msg: Optional[TopicMessage] = None
    total_count = 0
    
    try:
        peer = await client.resolve_peer(chat_id)
        
        offset_id = 0
        offset_date = 0
        consecutive_empty = 0  # Track empty pages to detect end
        
        while total_count < limit:
            try:
                result = await asyncio.wait_for(
                    client.invoke(
                        functions.messages.GetReplies(
                            peer=peer,
                            msg_id=topic_id,
                            offset_id=offset_id,
                            offset_date=offset_date,
                            add_offset=0,
                            limit=min(100, limit - total_count),
                            max_id=0,
                            min_id=0,
                            hash=0
                        )
                    ),
                    timeout=RPC_TIMEOUT
                )
                
                if not result.messages:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        break  # Two consecutive empty pages = done
                    continue
                
                consecutive_empty = 0
                
                for msg in result.messages:
                    if isinstance(msg, types.Message):
                        total_count += 1
                        if msg.date:
                            # Convert Unix timestamp to datetime safely
                            try:
                                msg_date = datetime.utcfromtimestamp(msg.date)
                            except (ValueError, OSError):
                                continue
                            
                            tm = TopicMessage(
                                message_id=msg.id,
                                date=msg_date,
                                is_topic_starter=False
                            )
                            # Track first/last by date in-place (no list needed)
                            if first_msg is None or msg_date < first_msg.date:
                                first_msg = tm
                            if last_msg is None or msg_date > last_msg.date:
                                last_msg = tm
                
                if len(result.messages) < 100:
                    break
                
                # Update offset for pagination
                last_raw = result.messages[-1]
                if isinstance(last_raw, types.Message):
                    offset_id = last_raw.id
                    offset_date = last_raw.date
                else:
                    break
                    
            except FloodWait as e:
                wait = getattr(e, 'value', getattr(e, 'x', 30))
                logger.info(f"FloodWait {wait}s scanning topic {topic_id}")
                await asyncio.sleep(min(wait, 60))
                continue
            except asyncio.TimeoutError:
                logger.warning(f"Timeout scanning topic {topic_id}, returning partial")
                break
            except Exception as e:
                error_str = str(e).upper()
                if "MSG_ID_INVALID" in error_str:
                    break  # Topic starter doesn't exist or no replies
                logger.warning(f"Error in GetReplies for topic {topic_id}: {e}")
                break
        
        # Include topic starter message itself
        try:
            starter = await asyncio.wait_for(
                client.get_messages(chat_id, topic_id),
                timeout=10.0
            )
            if starter and not starter.empty and starter.date:
                starter_tm = TopicMessage(
                    message_id=starter.id,
                    date=starter.date,
                    is_topic_starter=True
                )
                total_count += 1
                if first_msg is None or starter.date < first_msg.date:
                    first_msg = starter_tm
                if last_msg is None or starter.date > last_msg.date:
                    last_msg = starter_tm
        except Exception:
            pass
        
        return total_count, first_msg, last_msg
        
    except Exception as e:
        logger.warning(f"scan_topic_with_raw_api error for topic {topic_id}: {e}")
        return 0, None, None


async def analyze_group_topics(client: Client, chat_id: int) -> GroupAnalysis:
    """
    Analyze a forum group's topics.
    
    Returns complete GroupAnalysis with all topic data.
    """
    # Initialize result
    group_id_abs = abs(chat_id)
    group_id_short = str(group_id_abs)
    if group_id_short.startswith("100"):
        group_id_short = group_id_short[3:]
    
    analysis = GroupAnalysis(
        group_id=chat_id,
        group_id_short=group_id_short,
        title="",
    )
    
    try:
        # Get chat info
        chat = await asyncio.wait_for(
            client.get_chat(chat_id),
            timeout=RPC_TIMEOUT
        )
        
        if not chat:
            analysis.error = GroupError("not_found", "Group not found.", True)
            return analysis
        
        analysis.title = chat.title or "Unknown"
        analysis.username = chat.username
        analysis.member_count = chat.members_count or 0
        
        # Check if forum
        is_forum = getattr(chat, 'is_forum', False)
        analysis.is_forum = is_forum
        
        if not is_forum:
            # Not a forum - return early with clear message
            return analysis
        
        # Get forum topics
        raw_topics = await get_forum_topics_raw(client, chat_id)
        analysis.total_topics = len(raw_topics)
        
        if not raw_topics:
            return analysis
        
        # Process each topic (limit for performance)
        topics_to_process = raw_topics[:MAX_TOPICS_TO_SHOW]
        
        for idx, raw_topic in enumerate(topics_to_process):
            topic_id = raw_topic['id']
            
            # Create topic info
            topic_info = TopicInfo(
                topic_id=topic_id,
                title=raw_topic['title'],
                is_closed=raw_topic.get('closed', False),
                is_hidden=raw_topic.get('hidden', False),
            )
            
            # Scan topic messages
            try:
                count, first_msg, last_msg = await scan_topic_with_raw_api(
                    client, chat_id, topic_id
                )
                
                topic_info.message_count = count
                topic_info.first_message = first_msg
                topic_info.last_message = last_msg
                
            except Exception as e:
                logger.warning(f"Error scanning topic {topic_id}: {e}")
                # Skip topics we can't scan
                continue
            
            # Skip topics with no visible messages
            if topic_info.message_count == 0:
                continue
            
            analysis.topics.append(topic_info)
            
            # Rate limiting between topics
            if idx < len(topics_to_process) - 1:
                await asyncio.sleep(0.5)
        
        return analysis
        
    except Exception as e:
        analysis.error = classify_group_error(e)
        return analysis


# ==================== OUTPUT FORMATTING ====================

def format_group_analysis(analysis: GroupAnalysis) -> str:
    """Format GroupAnalysis as human-readable report."""
    lines = []
    
    # Error
    if analysis.error:
        return f"**Error:** {analysis.error.user_message}"
    
    # Header
    lines.append("**Group Analysis**\n")
    lines.append(f"**Title:** {analysis.title}")
    if analysis.username:
        lines.append(f"**Username:** @{analysis.username}")
    lines.append(f"**ID:** `{analysis.group_id}`")
    lines.append(f"**Members:** {analysis.member_count:,}")
    
    # Forum status
    if not analysis.is_forum:
        lines.append("\n**Forum Topics:** Disabled")
        lines.append("\n_This group does not use forum topics._")
        return "\n".join(lines)
    
    lines.append(f"\n**Forum Topics:** Enabled")
    lines.append(f"**Total Topics:** {analysis.total_topics}")
    
    if not analysis.topics:
        lines.append("\n_No accessible topics found._")
        return "\n".join(lines)
    
    # Topics
    lines.append(f"\n**Showing {len(analysis.topics)} topics:**\n")
    
    for topic in analysis.topics:
        # Topic header
        status = ""
        if topic.is_closed:
            status = " [CLOSED]"
        if topic.is_hidden:
            status = " [HIDDEN]"
        
        lines.append(f"**{topic.title}**{status}")
        lines.append(f"  • Topic ID: `{topic.topic_id}`")
        lines.append(f"  • Messages: {topic.message_count}")
        
        # First message link (by date)
        if topic.first_message:
            first_link = build_topic_message_link(
                analysis.group_id_short,
                topic.topic_id,
                topic.first_message.message_id
            )
            first_date = topic.first_message.date.strftime("%Y-%m-%d %H:%M")
            lines.append(f"  • First: [{first_date}]({first_link})")
        
        # Last message link (by date)
        if topic.last_message:
            last_link = build_topic_message_link(
                analysis.group_id_short,
                topic.topic_id,
                topic.last_message.message_id
            )
            last_date = topic.last_message.date.strftime("%Y-%m-%d %H:%M")
            lines.append(f"  • Last: [{last_date}]({last_link})")
        
        # Topic link
        topic_link = build_topic_link(analysis.group_id_short, topic.topic_id)
        lines.append(f"  • Topic: {topic_link}")
        lines.append("")  # Empty line between topics
    
    # Truncation notice
    if analysis.total_topics > len(analysis.topics):
        lines.append(f"_... and {analysis.total_topics - len(analysis.topics)} more topics_")
    
    return "\n".join(lines)


# ==================== COMMAND HANDLER ====================

def get(obj, key, default=None):
    """Safe dict access."""
    try:
        return obj.get(key, default) if hasattr(obj, 'get') else obj[key]
    except:
        return default


@Client.on_message(filters.command("group") & filters.private)
async def group_command_handler(client: Client, message: Message):
    """
    /group command handler.
    
    Analyzes forum groups and returns topic-scoped analytics.
    
    Usage:
        /group -1001234567890
        /group @groupname
        /group https://t.me/groupname
        /group https://t.me/c/1234567890/123
    """
    user_id = message.chat.id
    op_log("group_command", user_id=user_id, status="received")
    
    # Parse command
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "**Usage:** `/group <group_identifier>`\n\n"
            "**Supported formats:**\n"
            "• Group ID: `/group -1001234567890`\n"
            "• Username: `/group @groupname`\n"
            "• Link: `/group https://t.me/groupname`\n"
            "• Internal: `/group https://t.me/c/1234567890/123`\n\n"
            "**Returns:**\n"
            "• Group info and member count\n"
            "• Forum topic list (if enabled)\n"
            "• Message count per topic\n"
            "• First/last message links (by date)",
            **build_reply_kwargs_from_message(message)
        )
        return
    
    input_text = parts[1].strip()
    
    # Parse input
    parsed, error = parse_group_input(input_text)
    if error:
        await message.reply(f"**Error:** {error}", **build_reply_kwargs_from_message(message))
        return
    
    # Check user session
    user_data = await async_db.find_user(user_id)
    if not get(user_data, 'logged_in', False) or not user_data.get('session'):
        await message.reply(
            "You need to /login first to analyze groups.\n"
            "The bot uses your session to access group data.",
            **build_reply_kwargs_from_message(message)
        )
        return
    
    # Status message
    status_msg = await message.reply("Analyzing group...")
    
    try:
        async with create_user_session(user_data['session'], user_id, timeout=RPC_TIMEOUT) as acc:
            # Resolve group identifier
            group_id = parsed.identifier
            
            if parsed.input_type == "invite":
                # Join via invite link to resolve
                try:
                    chat = await acc.join_chat(parsed.identifier)
                    group_id = chat.id
                except Exception as e:
                    if "ALREADY" in str(e).upper():
                        # Already a member - need to resolve differently
                        await status_msg.edit_text(
                            "**Error:** Cannot resolve invite link. "
                            "Try using the group username or ID instead."
                        )
                        return
                    raise
            
            elif parsed.input_type == "username":
                # Resolve username to ID
                try:
                    chat = await acc.get_chat(parsed.identifier)
                    group_id = chat.id
                except Exception as e:
                    await status_msg.edit_text(f"**Error:** Cannot find @{parsed.identifier}")
                    return
            
            # Update status
            await status_msg.edit_text("Fetching group info and topics...")
            
            # Analyze group
            analysis = await analyze_group_topics(acc, group_id)
            
            # Format and send result
            report = format_group_analysis(analysis)
            
            # Split if too long
            if len(report) > 4000:
                # Send in chunks
                chunks = []
                current = ""
                for line in report.split("\n"):
                    if len(current) + len(line) + 1 > 4000:
                        chunks.append(current)
                        current = line
                    else:
                        current += "\n" + line if current else line
                if current:
                    chunks.append(current)
                
                await status_msg.edit_text(chunks[0])
                for chunk in chunks[1:]:
                    await message.reply(chunk)
            else:
                await status_msg.edit_text(report)
            
    except SessionInvalidError as e:
        await status_msg.edit_text(f"**Session Error:** {e.user_message}")
    except SessionConnectionError as e:
        await status_msg.edit_text(f"**Connection Error:** {e}")
    except asyncio.TimeoutError:
        await status_msg.edit_text("**Error:** Request timed out. The group may be too large.")
    except Exception as e:
        error = classify_group_error(e)
        await status_msg.edit_text(f"**Error:** {error.user_message}")
        logger.exception(f"/group error for user {user_id}: {e}")
