"""
Message Utilities for Pyrofork (MTProto Layer >= 220)

Production-grade utilities for:
- Message length enforcement (4096 chars)
- Caption length enforcement (1024 chars)
- Poll text normalization (300/100 char limits)
- Markdown escaping for user input
- Universal splitter for all text types

TELEGRAM LIMITS (enforced):
- Message text: 4096 UTF-8 characters
- Caption: 1024 UTF-8 characters
- Poll question: 300 UTF-8 characters
- Poll option: 100 UTF-8 characters
"""

import re
import logging
from typing import List, Optional, Tuple

from core.entity_validator import (
    utf16_length,
    utf16_index_to_char_index,
)

logger = logging.getLogger(__name__)

# Telegram hard limits
MAX_MESSAGE_LENGTH = 4096
MAX_CAPTION_LENGTH = 1024
MAX_POLL_QUESTION_LENGTH = 300
MAX_POLL_OPTION_LENGTH = 100


# Characters that need escaping in Markdown
MARKDOWN_ESCAPE_CHARS = r'\_*[]()~`>#+-=|{}.!'


def escape_markdown(text: str, version: int = 1) -> str:
    """
    Escape special characters in text for Markdown parsing.
    
    Args:
        text: Raw text to escape
        version: Markdown version (1 = basic, 2 = MarkdownV2)
    
    Returns:
        Escaped text safe for Markdown parsing
    
    Example:
        user_input = "Hello *world* [test]"
        safe_text = escape_markdown(user_input)
        # Result: "Hello \\*world\\* \\[test\\]"
    """
    if not text:
        return ""
    
    if version == 1:
        # Basic Markdown: escape * _ ` [ ]
        escape_chars = r'*_`['
        for char in escape_chars:
            text = text.replace(char, '\\' + char)
    else:
        # MarkdownV2: more characters need escaping
        for char in MARKDOWN_ESCAPE_CHARS:
            text = text.replace(char, '\\' + char)
    
    return text


def sanitize_markdown(text: str) -> str:
    """
    Fix common Markdown issues in text.
    
    Handles:
    - Unclosed brackets in links
    - Escaped parentheses
    - Unbalanced formatting markers
    
    Args:
        text: Text with potential Markdown issues
    
    Returns:
        Sanitized text with fixed Markdown
    """
    if not text:
        return text
    
    # Fix unclosed links: [text](url -> [text](url)
    pattern = r'\[([^\]]+)\]\(([^)]*[^)])'
    matches = re.findall(pattern, text)
    for link_text, url in matches:
        old = f'[{link_text}]({url}'
        new = f'[{link_text}]({url})'
        text = text.replace(old, new)
    
    # Fix escaped parentheses from bad parsing
    text = re.sub(r'\\\(', '(', text)
    text = re.sub(r'\\\)', ')', text)
    
    # Balance code markers (odd number of backticks)
    backtick_count = text.count('`')
    if backtick_count % 2 == 1:
        text = text.replace('`', '', 1)
    
    # Balance bold markers
    bold_count = len(re.findall(r'(?<!\*)\*\*(?!\*)', text))
    if bold_count % 2 == 1:
        text = text + '**'
    
    # Balance italic markers (single asterisk not part of bold)
    italic_pattern = r'(?<!\*)\*(?!\*)'
    italic_count = len(re.findall(italic_pattern, text))
    if italic_count % 2 == 1:
        text = text + '*'
    
    return text


def split_text(
    text: str,
    max_length: int = MAX_MESSAGE_LENGTH,
    preserve_formatting: bool = True,
    split_mode: str = "smart"
) -> List[str]:
    """
    Universal text splitter for Telegram messages.
    
    Handles all text types with proper UTF-8 character counting.
    
    Args:
        text: Text to split
        max_length: Maximum length per chunk (4096 for messages, 1024 for captions)
        preserve_formatting: Try to preserve Markdown formatting
        split_mode: "smart" (paragraph/line/word) or "hard" (exact cut)
    
    Returns:
        List of text chunks, each <= max_length characters
    
    Split Priority (smart mode):
        1. Paragraph break (double newline)
        2. Line break (single newline)
        3. Sentence end (. ! ?)
        4. Word boundary (space)
        5. Hard cut at limit
    """
    if not text:
        return []
    
    # Already within limit
    if utf16_length(text) <= max_length:
        return [text]
    
    chunks = []
    remaining = text
    
    while remaining:
        if utf16_length(remaining) <= max_length:
            chunks.append(remaining)
            break
        
        if split_mode == "hard":
            split_point = utf16_index_to_char_index(remaining, max_length)
        else:
            split_point = _find_smart_split_point(remaining, max_length, preserve_formatting)

        if split_point <= 0:
            split_point = utf16_index_to_char_index(remaining, max_length)
        
        chunk = remaining[:split_point].rstrip()
        remaining = remaining[split_point:].lstrip()
        
        if chunk:
            chunks.append(chunk)
    
    return chunks


def _find_hyperlink_at_position(text: str, position: int) -> Optional[Tuple[int, int]]:
    """
    Check if position falls inside a markdown hyperlink.
    Returns (start, end) of the hyperlink if found, None otherwise.
    """
    # Match markdown links: [text](url)
    pattern = r'\[([^\]]+)\]\(([^)]+)\)'
    for match in re.finditer(pattern, text):
        if match.start() < position < match.end():
            return (match.start(), match.end())
    return None


def _find_smart_split_point(text: str, max_length: int, preserve_formatting: bool) -> int:
    """Find the best split point for smart splitting, preserving hyperlinks."""
    if utf16_length(text) <= max_length:
        return len(text)
    
    max_char = utf16_index_to_char_index(text, max_length)
    search_text = text[:max_char]
    min_split = max_char // 3  # Don't split too early
    
    # Priority 1: Paragraph break
    para_pos = search_text.rfind('\n\n')
    if para_pos > min_split:
        split_pos = para_pos + 2
        # Check if this breaks a hyperlink
        hyperlink = _find_hyperlink_at_position(text, split_pos)
        if hyperlink is None:
            return split_pos
    
    # Priority 2: Line break
    line_pos = search_text.rfind('\n')
    if line_pos > min_split:
        split_pos = line_pos + 1
        hyperlink = _find_hyperlink_at_position(text, split_pos)
        if hyperlink is None:
            return split_pos
    
    # Priority 3: Sentence end
    for punct in ['. ', '! ', '? ', '.\n', '!\n', '?\n']:
        punct_pos = search_text.rfind(punct)
        if punct_pos > min_split:
            split_pos = punct_pos + len(punct)
            hyperlink = _find_hyperlink_at_position(text, split_pos)
            if hyperlink is None:
                return split_pos
    
    # Priority 4: Word boundary
    space_pos = search_text.rfind(' ')
    if space_pos > min_split:
        split_pos = space_pos + 1
        hyperlink = _find_hyperlink_at_position(text, split_pos)
        if hyperlink is None:
            return split_pos
    
    # Priority 5: Check if default split breaks a hyperlink
    hyperlink = _find_hyperlink_at_position(text, max_char)
    if hyperlink:
        # Move split point to before the hyperlink
        before_link = text[:hyperlink[0]]
        # Find last space before hyperlink
        last_space = before_link.rfind(' ')
        if last_space > min_split:
            return last_space + 1
        last_newline = before_link.rfind('\n')
        if last_newline > min_split:
            return last_newline + 1
        # Split right before the hyperlink
        if hyperlink[0] > min_split:
            return hyperlink[0]
    
    # Priority 6: Avoid cutting Markdown formatting
    if preserve_formatting:
        # Don't cut inside code blocks
        code_pos = search_text.rfind('```')
        if code_pos > min_split:
            return code_pos
        
        # Don't cut inside inline code
        inline_pos = search_text.rfind('`')
        if inline_pos > min_split:
            preceding = search_text[:inline_pos].count('`')
            if preceding % 2 == 1:  # Inside inline code
                return inline_pos
    
    # Fallback: hard cut
    return max_char


def split_message(text: str, preserve_formatting: bool = True) -> List[str]:
    """Split text for regular messages (4096 char limit)."""
    return split_text(text, MAX_MESSAGE_LENGTH, preserve_formatting)


def split_caption(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Split caption for media (1024 char limit).
    
    Returns:
        (media_caption, overflow_text)
        - media_caption: First 1024 chars for media
        - overflow_text: Remaining text for separate message
    """
    if not text:
        return None, None
    
    if utf16_length(text) <= MAX_CAPTION_LENGTH:
        return text, None
    
    # Find good split point
    search_limit = utf16_index_to_char_index(text, MAX_CAPTION_LENGTH)
    search_text = text[:search_limit]
    split_point = search_limit
    
    # Try newline near end
    newline_pos = search_text.rfind('\n', MAX_CAPTION_LENGTH - 200)
    if newline_pos > 0:
        split_point = newline_pos + 1
    else:
        # Try space near end
        space_pos = search_text.rfind(' ', MAX_CAPTION_LENGTH - 100)
        if space_pos > 0:
            split_point = space_pos + 1
    
    media_caption = text[:split_point].rstrip()
    overflow = text[split_point:].lstrip()
    
    # Ensure media_caption doesn't exceed limit
    if utf16_length(media_caption) > MAX_CAPTION_LENGTH:
        media_caption = _truncate_to_utf16(media_caption, MAX_CAPTION_LENGTH)
    
    return media_caption, overflow if overflow else None


def truncate_poll_question(question: str) -> str:
    """Truncate poll question to 300 char limit."""
    if not question:
        return ""
    
    if len(question) <= MAX_POLL_QUESTION_LENGTH:
        return question
    
    # Truncate with ellipsis
    return question[:MAX_POLL_QUESTION_LENGTH - 3].rstrip() + "..."


def truncate_poll_option(option: str) -> str:
    """Truncate poll option to 100 char limit."""
    if not option:
        return ""
    
    if len(option) <= MAX_POLL_OPTION_LENGTH:
        return option
    
    return option[:MAX_POLL_OPTION_LENGTH - 3].rstrip() + "..."


def normalize_poll_to_text(
    question: str,
    options: List[str],
    correct_option_id: Optional[int] = None,
    voted_option_id: Optional[int] = None,
    explanation: Optional[str] = None,
    is_quiz: bool = False
) -> str:
    """
    Convert poll/quiz to plain text representation.
    
    Args:
        question: Poll question
        options: List of option texts
        correct_option_id: Index of correct answer (for quizzes)
        voted_option_id: Index of user's vote
        explanation: Quiz explanation text
        is_quiz: Whether this is a quiz
    
    Returns:
        Formatted text representation of poll
    """
    option_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    
    poll_type = "Quiz" if is_quiz else "So'rovnoma"
    lines = [f"**📊 {poll_type}**\n"]
    lines.append(f"**Savol:**\n{question}\n")
    lines.append("**Variantlar:**")
    
    for i, opt in enumerate(options):
        letter = option_letters[i] if i < len(option_letters) else str(i + 1)
        marker = ""
        
        if is_quiz and correct_option_id is not None and i == correct_option_id:
            marker = " ✅"
        
        lines.append(f"{letter}) {opt}{marker}")
    
    # Show user's vote
    if voted_option_id is not None and voted_option_id < len(options):
        vote_letter = option_letters[voted_option_id] if voted_option_id < len(option_letters) else str(voted_option_id + 1)
        vote_text = options[voted_option_id]
        lines.append(f"\n**Sizning ovozingiz:** {vote_letter}) {vote_text} ✓")
        
        # Quiz result
        if is_quiz and correct_option_id is not None:
            correct_letter = option_letters[correct_option_id] if correct_option_id < len(option_letters) else "?"
            correct_text = options[correct_option_id] if correct_option_id < len(options) else "?"
            lines.append(f"**To'g'ri javob:** {correct_letter}) {correct_text}")
            
            if voted_option_id == correct_option_id:
                lines.append("\n**Natija:** ✅ TO'G'RI")
            else:
                lines.append("\n**Natija:** ❌ NOTO'G'RI")
    
    # Add explanation
    if explanation:
        lines.append(f"\n**💡 Izoh:** {explanation}")
    
    return "\n".join(lines)


def is_text_safe_length(text: str, text_type: str = "message") -> bool:
    """Check if text is within Telegram limits."""
    if not text:
        return True
    
    limits = {
        "message": MAX_MESSAGE_LENGTH,
        "caption": MAX_CAPTION_LENGTH,
        "poll_question": MAX_POLL_QUESTION_LENGTH,
        "poll_option": MAX_POLL_OPTION_LENGTH,
    }
    
    max_len = limits.get(text_type, MAX_MESSAGE_LENGTH)
    length = utf16_length(text) if text_type in ("message", "caption") else len(text)
    return length <= max_len


def get_text_stats(text: str) -> dict:
    """Get statistics about text length and Telegram compliance."""
    if not text:
        return {"length": 0, "safe_message": True, "safe_caption": True}
    
    length = utf16_length(text)
    return {
        "length": length,
        "safe_message": length <= MAX_MESSAGE_LENGTH,
        "safe_caption": length <= MAX_CAPTION_LENGTH,
        "chunks_needed_message": (length // MAX_MESSAGE_LENGTH) + (1 if length % MAX_MESSAGE_LENGTH else 0),
        "chunks_needed_caption": (length // MAX_CAPTION_LENGTH) + (1 if length % MAX_CAPTION_LENGTH else 0),
    }


def _truncate_to_utf16(text: str, max_utf16: int) -> str:
    """Truncate text to a UTF-16 length limit."""
    if not text:
        return text
    end_char = utf16_index_to_char_index(text, max_utf16)
    return text[:end_char]
