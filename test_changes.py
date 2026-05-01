"""Quick verification test for all the changes made."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_utf16_engine():
    from core.entity_splitter import utf16_len
    
    # ASCII
    assert utf16_len("Hello") == 5, "ASCII failed"
    # Emoji (supplementary plane = 2 UTF-16 units each)
    assert utf16_len("🌍") == 2, f"Emoji failed: got {utf16_len('🌍')}"
    assert utf16_len("Hello 🌍") == 8, f"Mixed failed: got {utf16_len('Hello 🌍')}"
    # Cyrillic (BMP = 1 UTF-16 unit each)
    assert utf16_len("Привет") == 6, "Cyrillic failed"
    print("  [PASS] UTF-16 engine")


def test_entity_builder_utf16():
    from core.entity_builder import EntityBuilder
    from core.entity_splitter import utf16_len
    
    builder = EntityBuilder()
    builder.add_text("Click ")
    builder.add_link("here 🔗", "https://example.com")
    builder.add_text(" for info")
    text, entities = builder.build()
    
    assert text == "Click here 🔗 for info", f"Text mismatch: {text}"
    assert len(entities) == 1, f"Expected 1 entity, got {len(entities)}"
    
    e = entities[0]
    expected_offset = utf16_len("Click ")  # 6
    expected_length = utf16_len("here 🔗")  # 7 (4 + 1 space + 2 emoji)
    
    assert e.offset == expected_offset, f"Offset: expected {expected_offset}, got {e.offset}"
    assert e.length == expected_length, f"Length: expected {expected_length}, got {e.length}"
    print("  [PASS] EntityBuilder UTF-16 offsets")


def test_entity_builder_bold_italic():
    from core.entity_builder import EntityBuilder
    from core.entity_splitter import utf16_len
    
    builder = EntityBuilder()
    builder.add_bold("Important")
    builder.add_text(" note: ")
    builder.add_italic("details")
    text, entities = builder.build()
    
    assert len(entities) == 2, f"Expected 2 entities, got {len(entities)}"
    
    bold = entities[0]
    assert bold.offset == 0
    assert bold.length == utf16_len("Important")
    
    italic = entities[1]
    assert italic.offset == utf16_len("Important note: ")
    assert italic.length == utf16_len("details")
    print("  [PASS] EntityBuilder bold/italic")


def test_split_text_with_entities():
    from core.entity_splitter import split_text_with_entities, utf16_len, MESSAGE_LIMIT
    from pyrogram.types import MessageEntity
    from pyrogram.enums import MessageEntityType
    
    # Create text that exceeds limit
    text = "A" * 4000 + " " + "B" * 4000
    chunks = split_text_with_entities(text, [], MESSAGE_LIMIT)
    
    assert len(chunks) >= 2, f"Expected >=2 chunks, got {len(chunks)}"
    for i, chunk in enumerate(chunks):
        u16len = utf16_len(chunk.text)
        assert u16len <= MESSAGE_LIMIT, f"Chunk {i} exceeds limit: {u16len}"
    print("  [PASS] Text splitting respects UTF-16 limits")


def test_split_with_entity_preservation():
    from core.entity_splitter import split_text_with_entities, utf16_len
    from pyrogram.types import MessageEntity
    from pyrogram.enums import MessageEntityType
    
    # Text with a link near the split boundary
    prefix = "A" * 4050
    link_text = "click here"
    suffix = " " + "B" * 100
    
    text = prefix + link_text + suffix
    entities = [
        MessageEntity(
            type=MessageEntityType.TEXT_LINK,
            offset=utf16_len(prefix),
            length=utf16_len(link_text),
            url="https://example.com"
        )
    ]
    
    chunks = split_text_with_entities(text, entities, 4096)
    
    # The link should NOT be split
    for chunk in chunks:
        for e in chunk.entities:
            end = e.offset + e.length
            chunk_len = utf16_len(chunk.text)
            assert end <= chunk_len, (
                f"Entity exceeds chunk: offset={e.offset}, length={e.length}, "
                f"chunk_len={chunk_len}"
            )
    print("  [PASS] Entities never split across chunks")


def test_environment_detection():
    from core.environment import detect_environment, RuntimeEnvironment, get_base_path
    
    env = detect_environment()
    assert env == RuntimeEnvironment.LOCAL, f"Expected LOCAL, got {env}"
    
    bp = get_base_path()
    assert os.path.isdir(bp), f"Base path not a directory: {bp}"
    print(f"  [PASS] Environment: {env.value}, base: {bp}")


def test_premium_limits():
    from core.premium_logic import (
        get_caption_limit, get_file_size_limit,
        PREMIUM_CAPTION_LIMIT, STANDARD_CAPTION_LIMIT
    )
    
    assert get_caption_limit(True) == 2048
    assert get_caption_limit(False) == 1024
    assert get_file_size_limit(True) > get_file_size_limit(False)
    print("  [PASS] Premium-aware limits")


def test_entity_builder_split_delegation():
    from core.entity_builder import split_text_with_entities
    from core.entity_splitter import utf16_len
    
    # Test that the builder's split_text_with_entities delegates properly
    text = "X" * 5000
    chunks = split_text_with_entities(text, [], 4096)
    
    assert len(chunks) >= 2, f"Expected >=2 chunks, got {len(chunks)}"
    for chunk in chunks:
        assert utf16_len(chunk.text) <= 4096, "Chunk exceeds limit"
    print("  [PASS] EntityBuilder split delegation to entity_splitter")


def test_local_storage_module():
    """Just verify the module imports correctly."""
    from database.local_storage import LocalStorage, is_local_storage_available
    avail = is_local_storage_available()
    print(f"  [PASS] LocalStorage module loads (aiosqlite available: {avail})")


def main():
    print("=" * 60)
    print("Running verification tests...")
    print("=" * 60)
    
    tests = [
        ("UTF-16 Engine", test_utf16_engine),
        ("EntityBuilder UTF-16", test_entity_builder_utf16),
        ("EntityBuilder Bold/Italic", test_entity_builder_bold_italic),
        ("Text Splitting", test_split_text_with_entities),
        ("Entity Preservation", test_split_with_entity_preservation),
        ("Environment Detection", test_environment_detection),
        ("Premium Limits", test_premium_limits),
        ("Builder Split Delegation", test_entity_builder_split_delegation),
        ("LocalStorage Module", test_local_storage_module),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
            failed += 1
    
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
