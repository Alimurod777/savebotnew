from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from TechVJ import save
from core.media_classifier import classify_message
from core.user_upload_worker import make_send_fn


@pytest.mark.parametrize('filename,mime,expected', [
    ('tekshirish.mp4', 'video/mp4', 'Video'),
    ('TEKSHIRISH.MP4', 'application/octet-stream', 'Video'),
    (None, 'VIDEO/MP4; codecs=avc1', 'Video'),
    ('clip.bin', 'video/mp4', 'Video'),
    ('clip.mkv', None, 'Video'),
    ('report.pdf', 'application/pdf', 'Document'),
    ('archive.zip', 'application/zip', 'Document'),
    ('song.m4a', 'audio/mp4', 'Document'),
])
def test_document_upload_uses_video_metadata_and_worker(monkeypatch, filename, mime, expected):
    async def run():
        msg = SimpleNamespace(document=SimpleNamespace(file_name=filename, mime_type=mime))
        meta = {'duration': 12, 'width': 640, 'height': 360}
        probe = AsyncMock(return_value=(meta, '/tmp/thumb.jpg'))
        monkeypatch.setattr(save, '_get_local_video_artifacts', probe)
        monkeypatch.setattr(save, 'get_thumbnail', AsyncMock(return_value=None))
        kind, video_meta, thumb = await save._prepare_local_upload(None, msg, 'Document', '/tmp/download.bin')
        assert kind == expected
        assert classify_message(msg) == 'document'
        bot_caption = {'caption': 'test', 'caption_entities': []}
        uploader = SimpleNamespace(send_video=AsyncMock(), send_document=AsyncMock())
        await make_send_fn(777, kind, '/tmp/download.bin', bot_caption,
                           video_meta=video_meta, thumb_path=thumb)(uploader)
        if expected == 'Video':
            probe.assert_awaited_once()
            uploader.send_document.assert_not_awaited()
            kwargs = uploader.send_video.await_args.kwargs
            assert kwargs['supports_streaming'] is True
            assert kwargs['duration'] == 12
            assert kwargs['width'] == 640
            assert kwargs['thumb'] == '/tmp/thumb.jpg'
            assert kwargs['caption'] == 'test'
        else:
            probe.assert_not_awaited()
            uploader.send_video.assert_not_awaited()
            assert uploader.send_document.await_args.kwargs['force_document'] is True
    asyncio.run(run())


def test_video_document_uses_source_thumb_when_local_thumb_unavailable(monkeypatch):
    async def run():
        msg = SimpleNamespace(document=SimpleNamespace(file_name='tekshirish.mp4', mime_type='video/mp4'))
        monkeypatch.setattr(save, '_get_local_video_artifacts', AsyncMock(return_value=({}, None)))
        fallback = AsyncMock(return_value='/tmp/source.jpg')
        monkeypatch.setattr(save, 'get_thumbnail', fallback)
        assert await save._prepare_local_upload('acc', msg, 'Document', '/tmp/video.mp4') == (
            'Video', {}, '/tmp/source.jpg')
        fallback.assert_awaited_once_with('acc', msg)
    asyncio.run(run())


def test_queued_document_upload_sends_streaming_video(monkeypatch, tmp_path):
    async def run():
        path = tmp_path / 'tekshirish.mp4'
        path.write_bytes(b'test-video')
        thumb = tmp_path / 'thumb.jpg'
        thumb.write_bytes(b'thumb')
        msg = SimpleNamespace(document=SimpleNamespace(file_name=path.name, mime_type='video/mp4'),
                              media=True, reply_markup=None, caption=None, caption_entities=None)
        acc = SimpleNamespace(download_media=AsyncMock(return_value=str(path)),
                              send_video=AsyncMock(), send_document=AsyncMock())
        monkeypatch.setattr(save, '_get_user_session_target_chat_id', lambda *_: 777)
        monkeypatch.setattr(save, '_get_local_video_artifacts', AsyncMock(return_value=(
            {'duration': 7, 'width': 320, 'height': 240}, str(thumb))))
        assert await save._send_post_direct(None, acc, 123, msg, 'Document', str(tmp_path))
        acc.send_document.assert_not_awaited()
        assert acc.send_video.await_args.kwargs['supports_streaming'] is True
        assert acc.send_video.await_args.kwargs['duration'] == 7
        assert not path.exists()
        assert not thumb.exists()
    asyncio.run(run())
