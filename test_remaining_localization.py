from __future__ import annotations

import asyncio
from datetime import datetime
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from wzgram.enums import PollType, MessageEntityType
from wzgram.types import Message, MessageEntity, InlineKeyboardMarkup, InlineKeyboardButton

from TechVJ import lang, save, group_command, info_command, post_command
from core.downloader.progress_v2 import ProgressPublisher, ProgressState, TransferStatus


@pytest.fixture
def locale(monkeypatch, tmp_path):
    monkeypatch.setattr(lang, 'LANG_DIR', str(tmp_path))
    lang.set_user_lang(123, 'ru')
    lang.set_user_lang(124, 'en')


@pytest.mark.parametrize('code,heading', [('en','Group Analysis'),('uz','Guruh tahlili'),('ru','Анализ группы')])
def test_group_report_translates_labels_but_not_titles_or_topic_links(locale, code, heading):
    lang.set_user_lang(123, code)
    title = 'Original Group Analysis — Title 😀'
    topic_title = 'First / Last / CLOSED 😀'
    when = datetime(2026, 11, 10, 12, 30)
    topic = group_command.TopicInfo(7, topic_title, message_count=2,
                                   first_message=NS(message_id=8, date=when),
                                   last_message=NS(message_id=9, date=when), is_closed=True)
    analysis = group_command.GroupAnalysis(-100123456, '123456', title,
                                          is_forum=True, topics=[topic], total_topics=3, member_count=10)
    text = group_command.format_group_analysis(analysis, user_id=123)
    assert heading in text and title in text and topic_title in text
    assert 'https://t.me/c/123456/7/8' in text
    assert 'https://t.me/c/123456/7/9' in text
    assert '2026-11-10 12:30' in text
    if code=='ru':
        assert 'Всего тем' in text and 'Участники' in text and '[ЗАКРЫТО]' in text
        assert 'Forum Topics' not in text
    assert analysis.title == title and topic.title == topic_title


@pytest.mark.parametrize('code,heading', [('en','Post Analysis'),('uz','Post tahlili'),('ru','Анализ поста')])
def test_post_report_locale_is_request_specific_and_preserves_metadata(locale, code, heading):
    lang.set_user_lang(123, code)
    analysis = post_command.PostAnalysis(-100123456, 8, chat_title='Original Post Analysis 😀',
        post_type='album', is_album=True, album_count=2, album_types=['photo','photo'],
        text='Original Caption 😀', text_utf16_length=5000, exceeds_limit=True,
        split_count_estimate=2, media_size_bytes=100, media_size_formatted='100 B',
        media_filename='Original Filename — Title.mp4', media_duration=62,
        entity_analysis=post_command.EntityAnalysis(total_count=2, hyperlinks=1, bold=1))
    text = post_command.format_post_analysis(analysis, user_id=123)
    assert heading in text and analysis.chat_title in text and analysis.media_filename in text
    assert '5000' in text and '1:02' in text
    if code=='ru':
        assert 'Альбом: Фото' in text and 'Жирный' in text and 'нужно разделение' in text
        assert 'Caption Length' not in text and 'Filename:' not in text
    english = post_command.format_post_analysis(analysis, user_id=124)
    assert 'Post Analysis' in english and 'Filename' in english
    assert analysis.text == 'Original Caption 😀'


@pytest.mark.parametrize('code,heading', [('en','Channel/Group Info'),('uz','Kanal/guruh ma’lumoti'),('ru','Информация о канале/группе')])
def test_info_report_translates_flags_and_chat_type(locale, code, heading):
    lang.set_user_lang(123, code)
    description = 'Original Description — Verified Enabled 😀'
    info = dict(title='Original Title', id=-100123456, type='SUPERGROUP', member_count=20,
                last_message_id=25, comments_enabled=True, topics_enabled=True,
                linked_chat_id=-100654321, linked_chat_title='Original Linked Title',
                description=description, creation_date=datetime(2026, 11, 10), is_verified=True)
    text = info_command.format_info_response(info, 'private', user_id=123)
    assert heading in text and description in text and 'Original Linked Title' in text
    if code=='ru':
        assert 'Супергруппа' in text and '✅ Включено' in text and 'Подтверждено' in text
    assert info['description']==description


@pytest.mark.parametrize('parser,argument', [(info_command.parse_info_link,'broken'),
        (post_command.parse_post_link,'broken'),(group_command.parse_group_input,'!')])
def test_input_errors_follow_current_language(locale, parser, argument):
    error = parser(argument, user_id=123)[-1]
    assert 'Неверн' in error or 'Ссылка должна' in error
    assert 'Invalid' not in error


@pytest.mark.parametrize('module,handler_name,command,expected', [
    (info_command,'info_command_handler','/info','Использование: /info'),
    (post_command,'post_command_handler','/post','Использование: /post'),
    (group_command,'group_command_handler','/group','Использование: /group'),
])
def test_command_usage_is_localized(locale, module, handler_name, command, expected):
    async def run():
        msg=NS(text=command, chat=NS(id=123), from_user=NS(id=123), reply=AsyncMock())
        await getattr(module, handler_name)(NS(), msg)
        assert expected in msg.reply.await_args.args[0]
    asyncio.run(run())


def test_classified_report_errors_are_translated_without_changing_details(locale):
    error = post_command.PostError('channel_private', 'This channel is private.', 'CHANNEL_PRIVATE')
    analysis = post_command.PostAnalysis(-100123456, 5, error=error)
    text = post_command.format_post_analysis(analysis, user_id=123)
    assert 'Этот чат закрыт' in text and 'Подробности' in text and 'CHANNEL_PRIVATE' in text
    assert error.user_message == 'This channel is private.'
    assert lang.get_report_error(123, NS(error_type='not_found')) == lang.STRINGS['ru']['report_error_msg_invalid']


@pytest.mark.parametrize('transfer', ['download', 'upload'])
@pytest.mark.parametrize('status,key', [(TransferStatus.COMPLETED,'download_complete'),
    (TransferStatus.FAILED,'download_failed'),(TransferStatus.CANCELLED,'operation_cancelled')])
def test_active_download_engine_final_ui_uses_locale(locale, status, key, transfer):
    async def run():
        state=ProgressState('test',transfer,status=status)
        bot=NS(edit_message_text=AsyncMock())
        publisher=ProgressPublisher(bot,123,7,state)
        await publisher._update_ui_final()
        text=bot.edit_message_text.await_args.kwargs['text']
        expected = key.replace('download_', 'upload_') if transfer == 'upload' else key
        assert text.startswith(lang.STRINGS['ru'][expected])
        if status==TransferStatus.FAILED:
            assert lang.STRINGS['ru']['unknown_error'] in text
    asyncio.run(run())


def test_active_download_engine_progress_uses_locale(locale):
    publisher=ProgressPublisher(NS(),123,7,ProgressState('test','download'))
    text=publisher._format_progress(dict(percent=50,current_bytes=100,total_bytes=200,
        transfer_type='download',speed_samples=[(0,0),(1,100)]))
    assert 'Скачивание' in text and 'Скорость' in text and 'Осталось' in text
    assert 'Downloading' not in text and 'Speed:' not in text


def test_poll_labels_are_russian_and_question_options_explanation_unchanged(locale, monkeypatch):
    async def run():
        poll=NS(type=PollType.QUIZ, correct_option_id=0, question='Original Question 😀',
                options=[NS(text='Original Correct answer'),NS(text='Original Wrong')], explanation='Original Explanation 😀')
        send=AsyncMock()
        monkeypatch.setattr(save,'safe_send_message',send)
        assert await save.handle_poll(NS(),NS(id=7,chat=NS(id=123)),poll,auto_vote=False)
        text=send.await_args.kwargs['text']
        for item in (poll.question,poll.options[0].text,poll.options[1].text,poll.explanation):
            assert item in text
        assert 'Вопрос' in text and 'Варианты' in text and 'Правильный ответ' in text
        assert 'Savol' not in text and 'Sizning ovozingiz' not in text
    asyncio.run(run())


def test_quizbot_heading_follows_locale_without_shifting_source_entities(locale):
    async def run():
        original='Original 😀 QuizBot body'
        entity=MessageEntity(type=MessageEntityType.BOLD,offset=0,length=8)
        link='https://t.me/QuizBot?start=synthetic'
        source=Message(id=5,text=original,entities=[entity],reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('Start',url=link)]]))
        bot=NS(send_message=AsyncMock(return_value=NS(id=9)))
        assert await save.handle_quizbot_post(bot,NS(id=7,chat=NS(id=123)),source)
        text=bot.send_message.await_args.kwargs['text']
        assert text.startswith(original) and '🤖 Начать тест:' in text and link in text
        assert source.text==original
        assert bot.send_message.await_args.kwargs['entities'][0].offset==0
    asyncio.run(run())
