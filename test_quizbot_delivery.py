from __future__ import annotations

import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from wzgram.enums import MessageEntityType, ParseMode
from wzgram.errors import FloodWait
from wzgram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, MessageEntity

from TechVJ import save
from core.entity_validator import utf16_length
from core.reply_compat import get_reply_message_id


@pytest.mark.parametrize('route', ['topic', 'post', 'queued'])
@pytest.mark.parametrize('long_text', [False, True])
def test_quiz_link_in_text_and_other_buttons_preserved(route, long_text):
    async def run():
        text = '🎲 “UZTURK & IRODAHOCA ! A2 Istanbul 1 - ünit grammatika” testi\n🖊 20 ta savol  ·  ⏱ 30 soniya'
        if long_text:
            text += '\n' + '😀 test ' * 650
        link = 'https://t.me/QuizBot?start=synthetic_quiz'
        keep_url = InlineKeyboardButton('Kanal', url='https://t.me/example')
        keep_inline = InlineKeyboardButton('Ulashish', switch_inline_query='synthetic_quiz')
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton('Boshlash', url=link)],
            [keep_url, InlineKeyboardButton('Start', url=link)],
            [keep_inline],
        ])
        entity = MessageEntity(type=MessageEntityType.BOLD, offset=0, length=2)
        msg = Message(id=50, text=text, entities=[entity], reply_markup=markup)
        bot = NS(send_message=AsyncMock(return_value=NS(id=99)))
        acc = NS(send_message=AsyncMock())
        request = NS(id=10, chat=NS(id=123))
        if route == 'topic':
            result = await save.process_single_topic_message(
                bot, acc, 123, msg, '/tmp/synthetic', NS(), request_message=request)
        elif route == 'post':
            result = await save.handle_quizbot_post(bot, request, msg)
        else:
            result = await save._send_post_direct(bot, acc, 123, msg, 'Text', None)
        assert result is True
        calls = bot.send_message.await_args_list
        delivered = ''.join(c.kwargs['text'] for c in calls)
        assert ''.join(delivered.split()).startswith(''.join(text.split()))
        assert 'Testni boshlash:' in delivered and link in delivered
        for call in calls:
            assert call.kwargs['chat_id'] == 123
            assert utf16_length(call.kwargs['text']) <= 4096
            assert call.kwargs['parse_mode'] == ParseMode.DISABLED
            assert get_reply_message_id(call.kwargs) == (None if route == 'queued' else 10)
            for ent in call.kwargs.get('entities') or []:
                assert ent.offset + ent.length <= utf16_length(call.kwargs['text'])
        assert calls[0].kwargs['entities'][0].type == MessageEntityType.BOLD
        assert calls[-1].kwargs['reply_markup'].inline_keyboard == [[keep_url], [keep_inline]]
        assert all(c.kwargs['reply_markup'] is None for c in calls[:-1])
        assert msg.text == text and msg.reply_markup is markup
        assert len(markup.inline_keyboard[0]) == 1
        acc.send_message.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize('caption', [False, True])
def test_quiz_caption_and_missing_delivery(caption):
    async def run():
        msg = Message(id=1, text=None if caption else 'Quiz',
                      caption='Quiz' if caption else None,
                      reply_markup=InlineKeyboardMarkup([[
                          InlineKeyboardButton('Start', url='https://t.me/QuizBot?start=test')]]))
        bot = NS(send_message=AsyncMock(return_value=None))
        assert await save.handle_quizbot_post(bot, NS(chat=NS(id=123), id=None), msg) is False
        assert bot.send_message.await_args.kwargs['text'].startswith('Quiz\n\n')
    asyncio.run(run())


def test_topic_quiz_floodwait_propagates():
    async def run():
        msg = Message(id=1, text='Quiz', reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton('Start', url='https://t.me/QuizBot?start=test')]]))
        bot = NS(send_message=AsyncMock(side_effect=FloodWait(5)))
        with pytest.raises(FloodWait):
            await save.process_single_topic_message(bot, NS(), 123, msg, '/tmp/synthetic', NS())
        assert bot.send_message.await_count == 1
    asyncio.run(run())
