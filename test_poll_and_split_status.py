from __future__ import annotations

import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from wzgram.enums import PollType
from wzgram.errors import FloodWait
from wzgram.types import Message

from TechVJ import lang, save
from core import premium_logic, safe_copy
from core.message_utils import normalize_poll_to_text


@pytest.fixture
def locale(monkeypatch, tmp_path):
    monkeypatch.setattr(lang, 'LANG_DIR', str(tmp_path))
    lang.set_user_lang(123, 'ru')


@pytest.mark.parametrize('code,question_label,vote_label', [
    ('en','Question','Your vote'),('uz','Savol','Sizning ovozingiz'),('ru','Вопрос','Ваш голос'),
])
def test_shared_poll_normalizer_translates_recipient_labels_without_touching_source(locale, code, question_label, vote_label):
    lang.set_user_lang(123, code)
    question='Во сколько проведем эфир? (время локальное по Израилю)\n'
    options=['11:00 ', '12:00', '13:00']
    text=normalize_poll_to_text(question, options, voted_option_id=0, user_id=123)
    assert question in text and 'A) 11:00 ' in text and 'B) 12:00' in text
    assert question_label in text and vote_label in text and '✓' in text
    if code=='ru':
        assert 'So\'rovnoma' not in text and 'Savol' not in text
        assert '📊 Опрос' in text and 'Варианты' in text
    assert options==['11:00 ', '12:00', '13:00']


@pytest.mark.parametrize('voted,result', [(0,'ВЕРНО'),(1,'НЕВЕРНО')])
def test_shared_quiz_labels_and_explanation_follow_locale(locale, voted, result):
    text=normalize_poll_to_text('Original Question', ['Original A','Original B'],
        correct_option_id=0,voted_option_id=voted, explanation='Original Explanation',is_quiz=True,user_id=123)
    assert 'Правильный ответ' in text and result in text and 'Пояснение' in text
    assert all(s in text for s in ['Original Question','Original A','Original B','Original Explanation'])


def test_safe_copy_poll_passes_target_language_to_shared_renderer(locale, monkeypatch):
    async def run():
        poll=NS(question='Во сколько проведем эфир?',options=[NS(text='11:00'),NS(text='12:00')],
                correct_option_id=None, explanation=None, type=PollType.REGULAR)
        source=Message(id=9,poll=poll)
        send=AsyncMock(return_value=NS(id=88))
        monkeypatch.setattr(safe_copy,'_send_text_safe',send)
        result=await safe_copy.safe_forward_or_copy(NS(),123,source)
        assert result.success
        text=send.await_args.args[2]
        assert 'Опрос' in text and 'Вопрос' in text and 'Варианты' in text
        assert poll.question in text and '11:00' in text
    asyncio.run(run())


@pytest.mark.parametrize('video', [False,True])
@pytest.mark.parametrize('outcome', ['success','cleanup_flood','cleanup_error','upload_failure','copy_failure'])
def test_split_success_removes_only_progress_and_cleanup_failure_never_reuploads(locale, monkeypatch, tmp_path, video, outcome):
    async def run():
        source=tmp_path/('source.mp4' if video else 'source.zip')
        source.write_bytes(b'fake source')
        parts=[tmp_path/('part%d.mp4'%i if video else 'part%d.zip'%i) for i in range(2)]
        for part in parts:
            part.write_bytes(b'fake part')
        status=NS(id=33,chat=NS(id=123))
        bot=NS(send_message=AsyncMock(return_value=status),delete_messages=AsyncMock())
        sent=AsyncMock(return_value=NS(id=80,chat=NS(id=777)))
        if outcome=='upload_failure':
            sent.side_effect=[NS(id=80,chat=NS(id=777)),RuntimeError('synthetic upload failure')]
        acc=NS(me=NS(id=123),send_document=sent,send_video=sent)
        media=NS(file_size=100*1024*1024,file_name=source.name)
        msg=NS(id=5,video=media if video else None,document=None if video else media,
               caption=None,caption_entities=[],reply_markup=None)
        request=NS(id=1,chat=NS(id=123),from_user=NS(id=123))
        pipeline=NS(check_cancelled=AsyncMock(return_value=False))
        monkeypatch.setattr(save,'MAX_TELEGRAM_FILE_SIZE',1)
        monkeypatch.setattr(save,'_ensure_media_route_clients',AsyncMock(return_value=(bot,acc,777,123)))
        monkeypatch.setattr(save,'get_engine',AsyncMock(return_value=NS(download=AsyncMock(return_value=str(source)))))
        monkeypatch.setattr(save,'_prepare_local_upload',AsyncMock(return_value=('Video' if video else 'Document',{},None)))
        monkeypatch.setattr(save,'_get_local_video_artifacts',AsyncMock(return_value=({},None)))
        monkeypatch.setattr(premium_logic,'check_user_premium',AsyncMock(return_value=False))
        monkeypatch.setattr(premium_logic,'get_user_upload_setting',lambda _:'auto')
        monkeypatch.setattr(premium_logic,'should_split',lambda *args:True)
        monkeypatch.setattr(save,'split_file',lambda _:[str(p) for p in parts])
        monkeypatch.setattr(save,'split_large_video',lambda _:[str(p) for p in parts])
        monkeypatch.setattr(save,'_get_user_session_account_id',AsyncMock(return_value=123))
        monkeypatch.setattr(save,'_ensure_user_session_bot_peer',AsyncMock(return_value=(777,'the_bot')))
        copy=AsyncMock(return_value=True)
        if outcome=='copy_failure':
            copy.side_effect=[True,False]
        monkeypatch.setattr(save,'_copy_user_session_upload_to_user',copy)
        edits=AsyncMock()
        monkeypatch.setattr(save,'_safe_status_edit_message',edits)
        monkeypatch.setattr(save.asyncio,'sleep',AsyncMock())
        if outcome=='cleanup_flood':
            bot.delete_messages.side_effect=FloodWait(90)
        elif outcome=='cleanup_error':
            bot.delete_messages.side_effect=RuntimeError('synthetic cleanup failure')
        result=await save.download_and_send_media.__wrapped__(bot,acc,request,msg,'Video' if video else 'Document',str(tmp_path),pipeline)
        assert result is (outcome not in ('upload_failure','copy_failure'))
        assert sent.await_count==2  # A cleanup failure must never replay uploaded parts.
        if result:
            bot.delete_messages.assert_awaited_once_with(123,[33])
            assert copy.await_count==2
            assert not any('Файл разделён' in call.args[3] for call in edits.await_args_list)
        else:
            bot.delete_messages.assert_not_awaited()  # Keep the failure context visible.
            if outcome=='upload_failure':
                assert 'Не удалось разделить' in edits.await_args.args[3]
        assert not source.exists()
        assert all(not p.exists() for p in parts)
    asyncio.run(run())
