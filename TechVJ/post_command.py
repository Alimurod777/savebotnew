"""
/post command - Analyze Telegram posts with structured metadata.

Features:
- Uses authenticated user session (not bot session)
- Parses private and public Telegram links
- Extracts comprehensive post metadata:
  - Chat ID, Message ID
  - Post type (text/photo/video/album/document)
  - Album detection with media_group_id
  - Caption/text with entities analysis
  - Media size and download info
  - Telegram limit validation (1024/4096)
  - Entity-safe splitting requirements

Architecture:
- Task-scoped session (no cross-event-loop reuse)
- Fail-fast on invalid/expired sessions
- Deterministic timeouts on RPC calls
- Human-readable error diagnostics
"""

import asyncio
import re
import logging
from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass, field

from pyrogram import Client, filters
from pyrogram.types import Message, MessageEntity
from pyrogram.enums import MessageMediaType, MessageEntityType
from pyrogram.errors import (
    FloodWait,
    ChannelPrivate,
    ChannelInvalid,
    PeerIdInvalid,
    MsgIdInvalid,
    RPCError,
)

from config import API_ID, API_HASH
from database.async_db import async_db
from core.reply_compat import build_reply_kwargs_from_message

# Import task-scoped session handler
from TechVJ.session_handler import (
    create_user_session,
    SessionInvalidError,
    SessionConnectionError,
    FATAL_SESSION_ERRORS,
)

# Import entity validation
from core.entity_validator import (
    utf16_length,
    validate_entities,
    CAPTION_LIMIT,
    MESSAGE_LIMIT,
)

logger = logging.getLogger(__name__)

# RPC timeout constants
RPC_TIMEOUT = 30.0
ALBUM_FETCH_TIMEOUT = 45.0


# ==================== ERROR CLASSIFICATION ====================

@dataclass
class PostError:
    """Classified error with human-readable message."""
    error_type: str
    user_message: str
    technical_detail: str = ""
    is_fatal: bool = False


def classify_error(e: Exception) -> PostError:
    """Classify exception into user-friendly error."""
    error_str = str(e).upper()
    error_name = type(e).__name__
    
    # Session errors
    if isinstance(e, SessionInvalidError):
        return PostError(
            error_type="session_invalid",
            user_message="Session expired. Please /login again.",
            technical_detail=str(e),
            is_fatal=True
        )
    
    if isinstance(e, SessionConnectionError):
        return PostError(
            error_type="session_connection",
            user_message=f"Connection error: {e}",
            technical_detail=str(e),
            is_fatal=False
        )
    
    # Entity errors
    if "ENTITY_BOUNDS_INVALID" in error_str:
        return PostError(
            error_type="entity_bounds",
            user_message="Entity bounds error in post. This post may have malformed formatting.",
            technical_detail=str(e),
            is_fatal=False
        )
    
    # Timeout errors
    if isinstance(e, asyncio.TimeoutError) or "TIMEOUT" in error_str:
        return PostError(
            error_type="timeout",
            user_message="Request timed out. The server may be slow or the post is too large.",
            technical_detail=str(e),
            is_fatal=False
        )
    
    # Access errors
    if isinstance(e, ChannelPrivate) or "CHANNEL_PRIVATE" in error_str:
        return PostError(
            error_type="channel_private",
            user_message="This channel is private. You need to be a member to access it.",
            technical_detail=str(e),
            is_fatal=False
        )
    
    if isinstance(e, ChannelInvalid) or "CHANNEL_INVALID" in error_str:
        return PostError(
            error_type="channel_invalid",
            user_message="Invalid channel. The channel may not exist or is inaccessible.",
            technical_detail=str(e),
            is_fatal=False
        )
    
    if isinstance(e, PeerIdInvalid) or "PEER_ID_INVALID" in error_str:
        return PostError(
            error_type="peer_invalid",
            user_message="Cannot access this chat. Make sure you're a member.",
            technical_detail=str(e),
            is_fatal=False
        )
    
    if isinstance(e, MsgIdInvalid) or "MSG_ID_INVALID" in error_str:
        return PostError(
            error_type="msg_invalid",
            user_message="Message not found. It may have been deleted.",
            technical_detail=str(e),
            is_fatal=False
        )
    
    # Ping/connection errors
    if "PINGTASK" in error_str or "PING" in error_str:
        return PostError(
            error_type="ping_stopped",
            user_message="Connection interrupted. Please try again.",
            technical_detail=str(e),
            is_fatal=False
        )
    
    if "DISCONNECT" in error_str or "CONNECTION" in error_str:
        return PostError(
            error_type="disconnected",
            user_message="Session disconnected. Please try again.",
            technical_detail=str(e),
            is_fatal=False
        )
    
    # FloodWait
    if isinstance(e, FloodWait):
        wait = getattr(e, 'value', getattr(e, 'x', 30))
        return PostError(
            error_type="flood_wait",
            user_message=f"Rate limited. Please wait {wait} seconds.",
            technical_detail=f"FloodWait: {wait}s",
            is_fatal=False
        )
    
    # Generic RPC error
    if isinstance(e, RPCError):
        return PostError(
            error_type="rpc_error",
            user_message=f"Telegram API error: {error_name}",
            technical_detail=str(e),
            is_fatal=False
        )
    
    # Unknown
    return PostError(
        error_type="unknown",
        user_message=f"Unexpected error: {error_name}",
        technical_detail=str(e),
        is_fatal=False
    )


# ==================== LINK PARSING ====================

@dataclass
class ParsedLink:
    """Parsed Telegram link."""
    chat_id: Any  # int or str (username)
    message_id: int
    link_type: str  # "private", "public"
    topic_id: Optional[int] = None
    
    @property
    def is_private(self) -> bool:
        return self.link_type == "private"


def parse_post_link(url: str) -> Tuple[Optional[ParsedLink], Optional[str]]:
    """
    Parse Telegram post link.
    
    Supported formats:
    - https://t.me/c/<chat_id>/<message_id> (private)
    - https://t.me/<channel>/<message_id> (public)
    - https://t.me/c/<chat_id>/<topic_id>/<message_id> (topic)
    
    Returns:
        (ParsedLink or None, error_message or None)
    """
    url = url.strip()
    
    if not url.startswith(('https://t.me/', 'http://t.me/')):
        return None, "Invalid URL: Must start with https://t.me/"
    
    # Topic link: /c/CHAT/TOPIC/MSG
    topic_match = re.match(r'^https?://t\.me/c/(\d+)/(\d+)/(\d+)$', url)
    if topic_match:
        chat_id = int("-100" + topic_match.group(1))
        topic_id = int(topic_match.group(2))
        message_id = int(topic_match.group(3))
        return ParsedLink(
            chat_id=chat_id,
            message_id=message_id,
            link_type="private",
            topic_id=topic_id
        ), None
    
    # Private channel: /c/CHAT/MSG
    private_match = re.match(r'^https?://t\.me/c/(\d+)/(\d+)(?:\?.*)?$', url)
    if private_match:
        chat_id = int("-100" + private_match.group(1))
        message_id = int(private_match.group(2))
        return ParsedLink(
            chat_id=chat_id,
            message_id=message_id,
            link_type="private"
        ), None
    
    # Public channel: /@username/MSG or /username/MSG
    public_match = re.match(r'^https?://t\.me/@?([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)$', url)
    if public_match:
        username = public_match.group(1)
        message_id = int(public_match.group(2))
        return ParsedLink(
            chat_id=username,
            message_id=message_id,
            link_type="public"
        ), None
    
    return None, (
        "Invalid link format.\n\n"
        "Supported formats:\n"
        "• `https://t.me/c/123456/101` (private)\n"
        "• `https://t.me/channelname/101` (public)"
    )


# ==================== POST ANALYSIS ====================

@dataclass
class EntityAnalysis:
    """Analysis of text/caption entities."""
    total_count: int = 0
    hyperlinks: int = 0
    bold: int = 0
    italic: int = 0
    underline: int = 0
    strikethrough: int = 0
    code: int = 0
    mentions: int = 0
    hashtags: int = 0
    cashtags: int = 0
    urls: int = 0
    custom_emoji: int = 0
    spoilers: int = 0
    
    @property
    def has_hyperlinks(self) -> bool:
        return self.hyperlinks > 0
    
    @property
    def requires_entity_send(self) -> bool:
        """Whether entity-based sending is mandatory (hyperlinks present)."""
        return self.hyperlinks > 0 or self.custom_emoji > 0


def analyze_entities(entities: Optional[List[MessageEntity]]) -> EntityAnalysis:
    """Analyze MessageEntity list."""
    analysis = EntityAnalysis()
    
    if not entities:
        return analysis
    
    analysis.total_count = len(entities)
    
    for entity in entities:
        entity_type = getattr(entity, 'type', None)
        
        if entity_type == MessageEntityType.TEXT_LINK:
            analysis.hyperlinks += 1
        elif entity_type == MessageEntityType.BOLD:
            analysis.bold += 1
        elif entity_type == MessageEntityType.ITALIC:
            analysis.italic += 1
        elif entity_type == MessageEntityType.UNDERLINE:
            analysis.underline += 1
        elif entity_type == MessageEntityType.STRIKETHROUGH:
            analysis.strikethrough += 1
        elif entity_type == MessageEntityType.CODE:
            analysis.code += 1
        elif entity_type == MessageEntityType.PRE:
            analysis.code += 1
        elif entity_type == MessageEntityType.MENTION:
            analysis.mentions += 1
        elif entity_type == MessageEntityType.TEXT_MENTION:
            analysis.mentions += 1
        elif entity_type == MessageEntityType.HASHTAG:
            analysis.hashtags += 1
        elif entity_type == MessageEntityType.CASHTAG:
            analysis.cashtags += 1
        elif entity_type == MessageEntityType.URL:
            analysis.urls += 1
        elif entity_type == MessageEntityType.CUSTOM_EMOJI:
            analysis.custom_emoji += 1
        elif entity_type == MessageEntityType.SPOILER:
            analysis.spoilers += 1
    
    return analysis


@dataclass
class PostAnalysis:
    """Complete post analysis result."""
    # Basic info
    chat_id: int
    message_id: int
    chat_title: str = ""
    chat_username: Optional[str] = None
    
    # Post type
    post_type: str = "unknown"  # text, photo, video, album, document, audio, voice, video_note, sticker, poll, etc.
    
    # Album info
    is_album: bool = False
    media_group_id: Optional[str] = None
    album_count: int = 0
    album_types: List[str] = field(default_factory=list)
    
    # Text/Caption
    text: Optional[str] = None
    text_length: int = 0
    text_utf16_length: int = 0
    entities: Optional[List[MessageEntity]] = None
    entity_analysis: EntityAnalysis = field(default_factory=EntityAnalysis)
    
    # Limit validation
    limit_type: str = "message"  # "message" (4096) or "caption" (1024)
    applicable_limit: int = MESSAGE_LIMIT
    exceeds_limit: bool = False
    requires_split: bool = False
    split_count_estimate: int = 1
    
    # Entity safety
    entity_valid: bool = True
    entity_errors: List[str] = field(default_factory=list)
    markdown_safe: bool = True
    entity_send_mandatory: bool = False
    
    # Media info
    media_size_bytes: int = 0
    media_size_formatted: str = ""
    media_duration: Optional[int] = None
    media_dimensions: Optional[Tuple[int, int]] = None
    media_filename: Optional[str] = None
    
    # Download info
    can_download: bool = True
    download_resumable: bool = False
    
    # Forward info
    safe_to_forward: bool = True
    forward_restriction: Optional[str] = None
    
    # Error
    error: Optional[PostError] = None


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable size."""
    if size_bytes == 0:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    unit_idx = 0
    size = float(size_bytes)
    
    while size >= 1024 and unit_idx < len(units) - 1:
        size /= 1024
        unit_idx += 1
    
    if unit_idx == 0:
        return f"{int(size)} B"
    return f"{size:.2f} {units[unit_idx]}"


def get_media_type_str(media_type: MessageMediaType) -> str:
    """Convert MessageMediaType to string."""
    type_map = {
        MessageMediaType.PHOTO: "photo",
        MessageMediaType.VIDEO: "video",
        MessageMediaType.DOCUMENT: "document",
        MessageMediaType.AUDIO: "audio",
        MessageMediaType.VOICE: "voice",
        MessageMediaType.VIDEO_NOTE: "video_note",
        MessageMediaType.STICKER: "sticker",
        MessageMediaType.ANIMATION: "animation",
        MessageMediaType.POLL: "poll",
        MessageMediaType.CONTACT: "contact",
        MessageMediaType.LOCATION: "location",
        MessageMediaType.VENUE: "venue",
        MessageMediaType.DICE: "dice",
        MessageMediaType.GAME: "game",
        MessageMediaType.WEB_PAGE_PREVIEW: "web_page",
    }
    return type_map.get(media_type, "unknown")


async def analyze_single_post(
    client: Client,
    chat_id: Any,
    message_id: int,
    chat_info: dict = None
) -> PostAnalysis:
    """
    Analyze a single post message.
    
    Args:
        client: Connected user client
        chat_id: Chat ID or username
        message_id: Message ID
        chat_info: Pre-fetched chat info (optional)
    
    Returns:
        PostAnalysis object
    """
    analysis = PostAnalysis(
        chat_id=chat_id if isinstance(chat_id, int) else 0,
        message_id=message_id
    )
    
    try:
        # Fetch message with timeout
        msg = await asyncio.wait_for(
            client.get_messages(chat_id, message_id),
            timeout=RPC_TIMEOUT
        )
        
        if not msg or msg.empty:
            analysis.error = PostError(
                error_type="not_found",
                user_message="Post not found or deleted.",
                is_fatal=False
            )
            return analysis
        
        # Chat info
        if chat_info:
            analysis.chat_title = chat_info.get('title', '')
            analysis.chat_username = chat_info.get('username')
        
        if isinstance(chat_id, int):
            analysis.chat_id = chat_id
        elif hasattr(msg, 'chat') and msg.chat:
            analysis.chat_id = msg.chat.id
            analysis.chat_title = msg.chat.title or ""
            analysis.chat_username = msg.chat.username
        
        # Check forward restriction
        if hasattr(msg, 'has_protected_content') and msg.has_protected_content:
            analysis.forward_restriction = "content_protected"
            analysis.safe_to_forward = False
        
        # Determine post type and extract text/caption
        if msg.media:
            media_type = msg.media
            analysis.post_type = get_media_type_str(media_type)

            # WEB_PAGE_PREVIEW: text is in msg.text, not msg.caption
            if media_type == MessageMediaType.WEB_PAGE_PREVIEW:
                if msg.text:
                    analysis.text = msg.text
                    analysis.entities = list(msg.entities) if msg.entities else None
                    analysis.limit_type = "message"
                    analysis.applicable_limit = MESSAGE_LIMIT
            # Caption for other media
            elif msg.caption:
                analysis.text = msg.caption
                analysis.entities = list(msg.caption_entities) if msg.caption_entities else None
                analysis.limit_type = "caption"
                analysis.applicable_limit = CAPTION_LIMIT
            
            # Extract media info
            media_obj = None
            if media_type == MessageMediaType.PHOTO:
                if msg.photo:
                    media_obj = msg.photo
                    analysis.media_size_bytes = media_obj.file_size or 0
                    if hasattr(media_obj, 'width') and hasattr(media_obj, 'height'):
                        analysis.media_dimensions = (media_obj.width, media_obj.height)
            
            elif media_type == MessageMediaType.VIDEO:
                if msg.video:
                    media_obj = msg.video
                    analysis.media_size_bytes = media_obj.file_size or 0
                    analysis.media_duration = media_obj.duration
                    if hasattr(media_obj, 'width') and hasattr(media_obj, 'height'):
                        analysis.media_dimensions = (media_obj.width, media_obj.height)
                    analysis.media_filename = media_obj.file_name
            
            elif media_type == MessageMediaType.DOCUMENT:
                if msg.document:
                    media_obj = msg.document
                    analysis.media_size_bytes = media_obj.file_size or 0
                    analysis.media_filename = media_obj.file_name
            
            elif media_type == MessageMediaType.AUDIO:
                if msg.audio:
                    media_obj = msg.audio
                    analysis.media_size_bytes = media_obj.file_size or 0
                    analysis.media_duration = media_obj.duration
                    analysis.media_filename = media_obj.file_name
            
            elif media_type == MessageMediaType.VOICE:
                if msg.voice:
                    media_obj = msg.voice
                    analysis.media_size_bytes = media_obj.file_size or 0
                    analysis.media_duration = media_obj.duration
            
            elif media_type == MessageMediaType.VIDEO_NOTE:
                if msg.video_note:
                    media_obj = msg.video_note
                    analysis.media_size_bytes = media_obj.file_size or 0
                    analysis.media_duration = media_obj.duration
            
            elif media_type == MessageMediaType.ANIMATION:
                if msg.animation:
                    media_obj = msg.animation
                    analysis.media_size_bytes = media_obj.file_size or 0
                    analysis.media_duration = getattr(media_obj, 'duration', None)
            
            elif media_type == MessageMediaType.STICKER:
                if msg.sticker:
                    media_obj = msg.sticker
                    analysis.media_size_bytes = media_obj.file_size or 0
            
            analysis.media_size_formatted = format_size(analysis.media_size_bytes)
            
            # Check if download is resumable (files > 10MB)
            if analysis.media_size_bytes > 10 * 1024 * 1024:
                analysis.download_resumable = True
        
        else:
            # Text message
            if msg.text:
                analysis.post_type = "text"
                analysis.text = msg.text
                analysis.entities = list(msg.entities) if msg.entities else None
                analysis.limit_type = "message"
                analysis.applicable_limit = MESSAGE_LIMIT
            else:
                analysis.post_type = "empty"
        
        # Album detection
        if msg.media_group_id:
            analysis.is_album = True
            analysis.media_group_id = msg.media_group_id
            
            # Fetch album members
            try:
                album_msgs = await asyncio.wait_for(
                    client.get_media_group(chat_id, message_id),
                    timeout=ALBUM_FETCH_TIMEOUT
                )
                if album_msgs:
                    analysis.album_count = len(album_msgs)
                    analysis.post_type = "album"
                    
                    # Collect album media types
                    for amsg in album_msgs:
                        if amsg.media:
                            analysis.album_types.append(get_media_type_str(amsg.media))
                            # Add up total media size
                            if amsg.photo:
                                analysis.media_size_bytes += amsg.photo.file_size or 0
                            elif amsg.video:
                                analysis.media_size_bytes += amsg.video.file_size or 0
                            elif amsg.document:
                                analysis.media_size_bytes += amsg.document.file_size or 0
                    
                    analysis.media_size_formatted = format_size(analysis.media_size_bytes)
            except asyncio.TimeoutError:
                analysis.album_count = 1  # At least 1 (current message)
            except Exception as e:
                logger.debug(f"Album fetch error: {e}")
                analysis.album_count = 1
        
        # Text/caption analysis
        if analysis.text:
            analysis.text_length = len(analysis.text)
            analysis.text_utf16_length = utf16_length(analysis.text)
            
            # Entity analysis
            analysis.entity_analysis = analyze_entities(analysis.entities)
            
            # Limit check
            if analysis.text_utf16_length > analysis.applicable_limit:
                analysis.exceeds_limit = True
                analysis.requires_split = True
                analysis.split_count_estimate = (
                    (analysis.text_utf16_length + analysis.applicable_limit - 1) 
                    // analysis.applicable_limit
                )
            
            # Entity validation
            if analysis.entities:
                is_valid, errors = validate_entities(analysis.text, analysis.entities)
                analysis.entity_valid = is_valid
                analysis.entity_errors = errors
                
                # Check if markdown is safe or entity-send is mandatory
                analysis.entity_send_mandatory = analysis.entity_analysis.requires_entity_send
                
                # Markdown is NOT safe for:
                # - Long captions with hyperlinks
                # - Multiple hyperlinks
                # - Complex nested formatting
                if analysis.entity_analysis.hyperlinks > 0:
                    analysis.markdown_safe = False
                if analysis.exceeds_limit and analysis.entity_analysis.total_count > 0:
                    analysis.markdown_safe = False
        
        return analysis
        
    except asyncio.TimeoutError:
        analysis.error = PostError(
            error_type="timeout",
            user_message="Request timed out fetching post.",
            is_fatal=False
        )
        return analysis
    except FATAL_SESSION_ERRORS as e:
        analysis.error = classify_error(e)
        return analysis
    except Exception as e:
        analysis.error = classify_error(e)
        return analysis


# ==================== OUTPUT FORMATTING ====================

def format_post_analysis(analysis: PostAnalysis) -> str:
    """Format PostAnalysis as human-readable report."""
    lines = ["**Post Analysis**\n"]
    
    # Error
    if analysis.error:
        lines.append(f"**Error:** {analysis.error.user_message}")
        if analysis.error.technical_detail:
            lines.append(f"_Detail: {analysis.error.technical_detail}_")
        return "\n".join(lines)
    
    # Basic info
    lines.append(f"• **Chat ID:** `{analysis.chat_id}`")
    lines.append(f"• **Message ID:** `{analysis.message_id}`")
    if analysis.chat_title:
        lines.append(f"• **Chat:** {analysis.chat_title}")
    if analysis.chat_username:
        lines.append(f"• **Username:** @{analysis.chat_username}")
    
    # Type
    type_display = analysis.post_type.replace('_', ' ').title()
    if analysis.is_album:
        # Determine album composition
        unique_types = list(set(analysis.album_types))
        if len(unique_types) == 1:
            type_display = f"{unique_types[0].title()} Album"
        else:
            type_display = "Mixed Album"
    lines.append(f"\n• **Type:** {type_display}")
    
    # Album info
    if analysis.is_album:
        lines.append(f"• **Album Items:** {analysis.album_count}")
        lines.append(f"• **Media Group ID:** `{analysis.media_group_id}`")
        if analysis.album_types:
            type_counts = {}
            for t in analysis.album_types:
                type_counts[t] = type_counts.get(t, 0) + 1
            types_str = ", ".join(f"{k}: {v}" for k, v in type_counts.items())
            lines.append(f"• **Album Composition:** {types_str}")
    
    # Caption/text
    if analysis.text:
        limit_name = "Caption" if analysis.limit_type == "caption" else "Message"
        limit_val = analysis.applicable_limit
        
        status = "OK"
        if analysis.exceeds_limit:
            status = f"**split required** (~{analysis.split_count_estimate} parts)"
        
        lines.append(f"\n• **{limit_name} Length:** {analysis.text_utf16_length} UTF-16 units ({status})")
        lines.append(f"  - Limit: {limit_val} UTF-16 units")
        
        # Entities
        if analysis.entity_analysis.total_count > 0:
            ea = analysis.entity_analysis
            entity_parts = []
            if ea.hyperlinks > 0:
                entity_parts.append(f"hyperlinks: {ea.hyperlinks}")
            if ea.bold > 0:
                entity_parts.append(f"bold: {ea.bold}")
            if ea.italic > 0:
                entity_parts.append(f"italic: {ea.italic}")
            if ea.underline > 0:
                entity_parts.append(f"underline: {ea.underline}")
            if ea.code > 0:
                entity_parts.append(f"code: {ea.code}")
            if ea.mentions > 0:
                entity_parts.append(f"mentions: {ea.mentions}")
            if ea.urls > 0:
                entity_parts.append(f"urls: {ea.urls}")
            if ea.spoilers > 0:
                entity_parts.append(f"spoilers: {ea.spoilers}")
            if ea.custom_emoji > 0:
                entity_parts.append(f"custom_emoji: {ea.custom_emoji}")
            
            lines.append(f"• **Entities:** {analysis.entity_analysis.total_count} ({', '.join(entity_parts)})")
        
        # Entity safety
        if analysis.entity_analysis.hyperlinks > 0:
            safety = "entity-safe" if analysis.entity_valid else "INVALID"
            lines.append(f"• **Hyperlinks:** {analysis.entity_analysis.hyperlinks} ({safety})")
        
        # Parsing recommendation
        if analysis.entity_send_mandatory:
            lines.append("• **Parsing:** `entities=[]` **required** (Markdown unsafe)")
        elif not analysis.markdown_safe:
            lines.append("• **Parsing:** `entities=[]` recommended")
        else:
            lines.append("• **Parsing:** Markdown safe")
        
        # Entity validation errors
        if analysis.entity_errors:
            lines.append(f"• **Entity Errors:** {len(analysis.entity_errors)}")
            for err in analysis.entity_errors[:3]:  # Show first 3
                lines.append(f"  - {err}")
    else:
        lines.append("\n• **Text/Caption:** None")
    
    # Media info
    if analysis.media_size_bytes > 0:
        lines.append(f"\n• **Media Size:** {analysis.media_size_formatted}")
        if analysis.media_dimensions:
            lines.append(f"• **Dimensions:** {analysis.media_dimensions[0]}x{analysis.media_dimensions[1]}")
        if analysis.media_duration:
            mins = analysis.media_duration // 60
            secs = analysis.media_duration % 60
            lines.append(f"• **Duration:** {mins}:{secs:02d}")
        if analysis.media_filename:
            lines.append(f"• **Filename:** {analysis.media_filename}")
        
        lines.append(f"• **Download Resumable:** {'Yes' if analysis.download_resumable else 'No'}")
    
    # Forward status
    if not analysis.safe_to_forward:
        lines.append(f"\n• **Safe to Forward:** No ({analysis.forward_restriction})")
    else:
        lines.append(f"\n• **Safe to Forward:** Yes")
    
    return "\n".join(lines)


# ==================== COMMAND HANDLER ====================

def get(obj, key, default=None):
    """Safe dict access."""
    try:
        return obj.get(key, default) if hasattr(obj, 'get') else obj[key]
    except:
        return default


@Client.on_message(filters.command("post") & filters.private)
async def post_command_handler(client: Client, message: Message):
    """
    /post command handler.
    
    Usage:
        /post https://t.me/c/1234567890/123
        /post https://t.me/channelname/123
    
    Returns detailed post analysis.
    """
    user_id = message.chat.id
    
    # Parse command
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "**Usage:** `/post <telegram_link>`\n\n"
            "**Examples:**\n"
            "• `/post https://t.me/c/1234567890/123` (private)\n"
            "• `/post https://t.me/channelname/123` (public)\n"
            "• `/post https://t.me/c/1234567890/5/123` (topic)\n\n"
            "Returns detailed post analysis including:\n"
            "• Post type and album detection\n"
            "• Caption/text length and entity analysis\n"
            "• Media size and download info\n"
            "• Telegram limit validation",
            **build_reply_kwargs_from_message(message)
        )
        return
    
    url = parts[1].strip()
    
    # Parse link
    parsed, error = parse_post_link(url)
    if error:
        await message.reply(
            f"**Error:** {error}",
            **build_reply_kwargs_from_message(message)
        )
        return
    
    # Check if user has session
    user_data = await async_db.find_user(user_id)
    if not get(user_data, 'logged_in', False) or not user_data.get('session'):
        await message.reply(
            "You need to /login first to analyze posts.\n"
            "The bot uses your session to access the post.",
            **build_reply_kwargs_from_message(message)
        )
        return
    
    # Status message
    status_msg = await message.reply("Analyzing post...")
    
    try:
        # Create task-scoped session
        async with create_user_session(user_data['session'], user_id, timeout=RPC_TIMEOUT) as acc:
            # Get chat info first (for public channels)
            chat_info = {}
            try:
                chat = await asyncio.wait_for(
                    acc.get_chat(parsed.chat_id),
                    timeout=RPC_TIMEOUT
                )
                if chat:
                    chat_info = {
                        'title': chat.title or chat.first_name or '',
                        'username': chat.username,
                        'id': chat.id
                    }
            except Exception as e:
                logger.debug(f"Could not get chat info: {e}")
            
            # Analyze post
            analysis = await analyze_single_post(
                acc,
                parsed.chat_id,
                parsed.message_id,
                chat_info
            )
            
            # Format and send result
            report = format_post_analysis(analysis)
            await status_msg.edit_text(report)
            
    except SessionInvalidError as e:
        await status_msg.edit_text(f"**Session Error:** {e.user_message}")
    except SessionConnectionError as e:
        await status_msg.edit_text(f"**Connection Error:** {e}")
    except asyncio.TimeoutError:
        await status_msg.edit_text("**Error:** Request timed out. Please try again.")
    except Exception as e:
        error = classify_error(e)
        await status_msg.edit_text(f"**Error:** {error.user_message}")
        logger.exception(f"/post error for user {user_id}: {e}")
