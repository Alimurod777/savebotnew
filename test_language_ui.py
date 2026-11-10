from __future__ import annotations

import ast
import asyncio
import inspect
from contextlib import asynccontextmanager
from pathlib import Path
from string import Formatter
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from wzgram.enums import MessageEntityType
from wzgram.types import MessageEntity

from TechVJ import lang, save
from core.caption_splitter import split_caption
from core.utf16_utils import utf16_len


@pytest.fixture
def language_storage(monkeypatch, tmp_path):
    monkeypatch.setattr(lang, 'LANG_DIR', str(tmp_path))
    return tmp_path


def test_translation_catalogues_are_complete_and_format_compatible():
    fields = lambda value: {key for _, key, _, _ in Formatter().parse(value) if key is not None}
    for code in ('uz', 'ru'):
        assert lang.STRINGS[code].keys() == lang.STRINGS['en'].keys()
        for key, english in lang.STRINGS['en'].items():
            translated = lang.STRINGS[code][key]
            assert translated.strip(), (code, key)
            assert fields(english) == fields(translated), (code, key)
    # Duplicate dictionary keys would silently hide a translation.
    tree = ast.parse(Path(lang.__file__).read_text())
    strings = next(n.value for n in tree.body if isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name) and n.target.id == 'STRINGS')
    for catalogue in strings.values:
        keys = [node.value for node in catalogue.keys]
        assert len(keys) == len(set(keys))


@pytest.mark.parametrize('code', ['en', 'uz', 'ru'])
def test_selection_persists_and_reopened_menu_uses_selected_language(language_storage, code):
    async def run():
        message = NS(edit_text=AsyncMock(), reply=AsyncMock(), from_user=NS(id=123))
        query = NS(from_user=message.from_user, data='lang_' + code, message=message, answer=AsyncMock())
        await lang.lang_callback(None, query)
        assert (language_storage / '123.txt').read_text() == code
        message.edit_text.assert_awaited_once_with(lang.STRINGS[code]['lang_changed'])
        await lang.lang_command(None, message)
        assert message.reply.await_args.args[0] == lang.STRINGS[code]['lang_select']
        keyboard = message.reply.await_args.kwargs['reply_markup'].inline_keyboard
        assert {button.callback_data for row in keyboard for button in row} == {'lang_en', 'lang_uz', 'lang_ru'}
    asyncio.run(run())


def test_failed_language_save_does_not_claim_success(language_storage, monkeypatch):
    async def run():
        assert lang.set_user_lang(123, 'ru')
        monkeypatch.setattr(lang, 'set_user_lang', lambda *args: False)
        query = NS(from_user=NS(id=123), data='lang_en', message=NS(edit_text=AsyncMock()), answer=AsyncMock())
        await lang.lang_callback(None, query)
        query.message.edit_text.assert_not_awaited()
        query.answer.assert_awaited_once_with(lang.STRINGS['ru']['language_save_failed'], show_alert=True)
        assert lang.get_user_lang(123) == 'ru'
    asyncio.run(run())


@pytest.mark.parametrize('code,marker', [('en', 'Continued'), ('uz', 'Davomi'), ('ru', 'Продолжение')])
def test_caption_header_preserves_original_russian_content_and_entity_offsets(language_storage, code, marker):
    lang.set_user_lang(123, code)
    body = 'Ставьте напоминания на эти даты в свой календарь 📆'
    text = 'x' * 1024 + body
    entity = MessageEntity(type=MessageEntityType.BOLD, offset=1024, length=utf16_len('Ставьте'))
    primary, overflow = split_caption(text, [entity], continuation_prefix=lang.get_string(123, 'continuation_prefix'))
    header = f'📌 {marker} 2/2\n\n'
    assert primary.text == 'x' * 1024
    assert overflow[0].text == header + body
    assert overflow[0].entities[0].offset == utf16_len(header)
    assert overflow[0].entities[0].length == entity.length
    assert entity.offset == 1024  # Original entity is never changed.


@pytest.mark.parametrize('premium', [False, True])
def test_localized_overflow_stays_within_telegram_utf16_limit(premium):
    primary, overflow = split_caption('😀' * 10000, [], is_premium=premium, continuation_prefix='📌 Продолжение')
    assert primary.utf16_length <= (2048 if premium else 1024)
    assert len(overflow) > 1
    for chunk in overflow:
        assert chunk.utf16_length <= 4096
        assert chunk.text.startswith('📌 Продолжение ')


@pytest.mark.parametrize('code,source,complete', [('en', 'Source', 'Complete'), ('uz', 'Manba', 'Tugadi'), ('ru', 'Источник', 'Готово')])
def test_private_range_result_and_source_anchor_follow_selected_language(monkeypatch, language_storage, code, source, complete):
    async def run():
        lang.set_user_lang(123, code)
        pipeline = NS(get_temp_dir=AsyncMock(return_value='/tmp/mock-language'), check_cancelled=AsyncMock(return_value=False))
        acc = NS(get_chat=AsyncMock(return_value=NS(title="MB2 Master's")), get_messages=AsyncMock(return_value=None))

        @asynccontextmanager
        async def open_pipeline(*args, **kwargs):
            yield pipeline

        @asynccontextmanager
        async def open_session(*args, **kwargs):
            yield acc

        async def send(chat_id, text, **kwargs):
            return NS(id=40, chat=NS(id=chat_id), text=text)

        bot = NS(send_message=AsyncMock(side_effect=send), edit_message_text=AsyncMock(), pin_chat_message=AsyncMock(), unpin_chat_message=AsyncMock())
        monkeypatch.setattr(save, 'StopSafePipeline', open_pipeline)
        monkeypatch.setattr(save, '_open_download_session', open_session)
        monkeypatch.setattr(save.async_db, 'find_user', AsyncMock(return_value={'logged_in': True, 'session': 'synthetic'}))
        monkeypatch.setattr(save, '_resolve_peer_safe', AsyncMock())
        monkeypatch.setattr(save, 'ping_activity', AsyncMock())
        monkeypatch.setattr(save, 'detect_album_boundary', AsyncMock(return_value=None))
        monkeypatch.setattr(save, '_THROTTLE_AVAILABLE', False)
        monkeypatch.setattr(save.asyncio, 'sleep', AsyncMock())
        monkeypatch.setattr(save, 'process_single_post', AsyncMock(return_value=True))
        await inspect.unwrap(save.process_private_posts)(bot, NS(id=1, chat=NS(id=123)), NS(channel_id=-100999, post_ids=list(range(1, 7))), context=NS(user_id=123))
        final = bot.edit_message_text.await_args.args[2]
        assert complete in final and '6/6' in final
        assert any(f'📡 {source}: MB2' in c.args[1] for c in bot.send_message.await_args_list)
        if code == 'ru':
            assert final == '✅ Готово\nОтправлено: 6/6\n📁 Альбомов: 0'
            assert 'Manba' not in final and 'Tugadi' not in final
    asyncio.run(run())


@pytest.mark.parametrize('code', ['en', 'uz', 'ru'])
def test_basic_commands_use_same_locale(language_storage, monkeypatch, code):
    async def run():
        lang.set_user_lang(123, code)
        monkeypatch.setattr(save.async_db, 'find_user', AsyncMock(return_value=None))
        monkeypatch.setattr(save, '_GOVERNANCE_AVAILABLE', False)
        monkeypatch.setattr(save.user_queue, 'get_status', AsyncMock(return_value={'is_processing': False, 'queue_size': 0}))
        bot = NS(send_message=AsyncMock())
        message = NS(from_user=NS(id=123, first_name='Example'), chat=NS(id=123), reply=AsyncMock())
        await inspect.unwrap(save.send_start)(bot, message)
        assert lang.STRINGS[code]['logged_out'] in bot.send_message.await_args.args[1]
        await inspect.unwrap(save.send_help)(bot, message)
        assert bot.send_message.await_args.args[1] == lang.STRINGS[code]['help_detail']
        assert 'Русский' in bot.send_message.await_args.args[1]
        await inspect.unwrap(save.send_status)(bot, message)
        assert lang.STRINGS[code]['session_absent'] in message.reply.await_args.args[0]
    asyncio.run(run())


def test_queue_progress_and_preferences_use_russian(language_storage, monkeypatch):
    from TechVJ import premium_commands
    from TechVJ.progress_controller import ProgressController
    from TechVJ.user_queue import format_queue_status
    lang.set_user_lang(123, 'ru')
    assert format_queue_status({'is_processing': False, 'queue_size': 0}, 123) == '📭 Очередь пуста'
    controller = ProgressController()
    text = controller._format_progress_text('download', 50, 100, 200, 10, 10, user_id=123)
    assert 'Скачивание' in text and 'Осталось' in text

    async def run():
        monkeypatch.setattr(premium_commands, 'get_user_upload_setting', lambda _: 'auto')
        message = NS(from_user=NS(id=123), text='/uploadsetting', reply=AsyncMock())
        await premium_commands.upload_setting_command(None, message)
        assert 'Настройка отправки' in message.reply.await_args.args[0]
    asyncio.run(run())


def test_russian_transfer_status_is_filtered_without_filtering_source_captions():
    from core.chat_log import _is_progress_text
    assert _is_progress_text(NS(text='⬇️ Скачивание...\n50%'))
    assert _is_progress_text(NS(text='⏳ Скачивание постов: 6...'))
    assert not _is_progress_text(NS(text='✅ Готово\nОтправлено: 6/6'))
    assert not _is_progress_text(NS(text=None, caption='⬇️ Скачивание...'))


@pytest.mark.parametrize('code', ['en', 'uz', 'ru'])
def test_login_prompt_and_logout_follow_selection_without_connecting_to_telegram(language_storage, monkeypatch, code):
    from TechVJ import generate

    async def run():
        lang.set_user_lang(123, code)
        monkeypatch.setattr(generate.async_db, 'is_banned', AsyncMock(return_value=False))
        monkeypatch.setattr(generate.async_db, 'insert_user', AsyncMock())
        monkeypatch.setattr(generate.async_db, 'find_user', AsyncMock(return_value=None))
        monkeypatch.setattr(generate.login_limiter, 'can_login', AsyncMock(return_value=(True, 0, '')))
        phone_reply = NS(text='/cancel', reply=AsyncMock())
        prompt = AsyncMock(return_value=phone_reply)
        monkeypatch.setattr(generate, 'ask_message', prompt)
        message = NS(from_user=NS(id=123), chat=NS(id=123), reply=AsyncMock())
        await generate.main(NS(), message)
        assert prompt.await_args.kwargs['text'].startswith(lang.STRINGS[code]['send_phone'])
        phone_reply.reply.assert_awaited_once_with(lang.STRINGS[code]['process_cancelled'], parse_mode=generate.ParseMode.DISABLED)
        monkeypatch.setattr(generate, 'load_session_from_file', lambda _: None)
        await generate.logout(NS(), message)
        assert message.reply.await_args.args[0] == lang.STRINGS[code]['not_logged_in']
    asyncio.run(run())


def test_qr_caption_and_password_prompt_use_russian(language_storage, monkeypatch):
    from TechVJ import qr_login

    async def run():
        lang.set_user_lang(123, 'ru')
        monkeypatch.setattr(qr_login, 'generate_qr_login_token', AsyncMock(return_value=('synthetic-token', None)))
        monkeypatch.setattr(qr_login, 'create_qr_image', lambda *args: '/tmp/mock-qr.png')
        monkeypatch.setattr(qr_login, 'check_qr_login_status', AsyncMock(return_value=('success', None)))
        monkeypatch.setattr(qr_login, 'cleanup_qr_files', lambda _: None)
        bot = NS(send_photo=AsyncMock(return_value=NS(delete=AsyncMock())))
        result = await qr_login.wait_for_qr_scan(NS(), 123, bot, 123)
        assert result == (True, False, None)
        assert bot.send_photo.await_args.kwargs['caption'].startswith('Отсканируйте QR-код в Telegram:')
        assert '/cancel' in bot.send_photo.await_args.kwargs['caption']
    asyncio.run(run())
