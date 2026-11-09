from __future__ import annotations

import asyncio
from html.parser import HTMLParser
from pathlib import Path

import pytest
from openpyxl import load_workbook

from TechVJ.save import parse_telegram_url
from TechVJ.userdata_command import DialogRecord, InventoryJob, TopicRecord, _generate_report
from core.userdata_reports import normalize, render_html, render_xlsx, topic_download_link


def forum(chat_id=-100123456, **kwargs):
    return DialogRecord(chat_id, 'group', chat_type='forum', is_forum=True,
                        title='Forum', visibility='PRIVATE', visibility_verified=True,
                        visibility_source='Verified full Channel metadata', **kwargs)


@pytest.mark.parametrize('topic,username,expected,bounds', [
    (TopicRecord(21, 'Audio', first_message_id=21, latest_message_id=832), None,
     'https://t.me/c/123456/21/21-832', (21, 832)),
    (TopicRecord(1, 'General', latest_message_id=837), None,
     'https://t.me/c/123456/1/1-837', (1, 837)),
    (TopicRecord(5, 'Tests', top_message=806), 'example',
     'https://t.me/example/5/5-806', (5, 806)),
    (TopicRecord(7, 'New', latest_message_id=7), None,
     'https://t.me/c/123456/7/7', None),
])
def test_report_link_round_trips_through_bot_topic_parser(topic, username, expected, bounds):
    url = topic_download_link(forum(username=username), topic)
    assert url == expected
    parsed, error = parse_telegram_url(url)
    assert error is None
    assert parsed.topic_id == topic.topic_id
    assert parsed.topic_range_anchor == bounds
    assert parsed.channel_id == ('example' if username else -100123456)


@pytest.mark.parametrize('chat,topic', [
    (forum(), TopicRecord(21, 'Unknown end')),
    (forum(), TopicRecord(0, 'Invalid', latest_message_id=32)),
    (forum(), TopicRecord(True, 'Invalid', latest_message_id=32)),
    (forum(-42), TopicRecord(21, 'Basic', latest_message_id=32)),
    (DialogRecord(42, 'private', chat_type='private'), TopicRecord(21, 'Private', latest_message_id=32)),
])
def test_missing_bounds_or_unsupported_peer_never_gets_fake_link(chat, topic):
    assert topic_download_link(chat, topic) == ''


def test_each_group_owns_its_topic_copy_links_and_workbook_text(tmp_path):
    first = forum(topics=[TopicRecord(21, '<Audio & tests>', latest_message_id=832)])
    second = forum(-100654321, topics=[TopicRecord(21, 'Other', latest_message_id=65)])
    job = InventoryJob('synthetic', 123, records=[first, second], dialogs_discovered=2)
    snapshot = normalize(job)
    expected = ['https://t.me/c/123456/21/21-832', 'https://t.me/c/654321/21/21-65']
    assert [r.topic_download_links[0].url for r in snapshot.records] == expected
    for row, url in zip(snapshot.records, expected):
        assert f'Bot download link: {url}' in row.topics

    class CopyButtons(HTMLParser):
        def __init__(self):
            super().__init__()
            self.urls = []

        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            if tag == 'button' and 'data-value' in attrs:
                self.urls.append(attrs['data-value'])

    html = tmp_path / 'inventory.html'
    render_html(snapshot, html)
    text = html.read_text()
    parser = CopyButtons()
    parser.feed(text)
    assert parser.urls == expected
    assert '&lt;Audio &amp; tests&gt;' in text
    assert '<Audio & tests>' not in text
    assert 'Havolani nusxalash' in text
    assert 'b.dataset.value||b.dataset.id' in text

    xlsx = tmp_path / 'inventory.xlsx'
    render_xlsx(snapshot, xlsx)
    book = load_workbook(xlsx)
    try:
        for index, url in enumerate(expected, 2):
            assert url in book['Private Groups'].cell(index, 8).value
    finally:
        book.close()


def test_legacy_text_report_also_includes_topic_request(monkeypatch, tmp_path):
    from TechVJ import userdata_command
    monkeypatch.setattr(userdata_command, 'TEMP_DIR', str(tmp_path))
    job = InventoryJob('synthetic', 123, records=[forum(topics=[
        TopicRecord(21, 'Audio', latest_message_id=832)])])
    path = asyncio.run(_generate_report(job))
    assert 'Bot download link: https://t.me/c/123456/21/21-832' in Path(path).read_text()
