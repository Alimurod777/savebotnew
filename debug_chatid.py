import asyncio
import sys
from pyrogram import Client
from pyrogram import raw
from pyrogram.raw import functions, types
from config import API_ID, API_HASH
from database.db import database

async def debug_chat_id(chat_id, message_id, session_string):
    # Create client with the session
    client = Client(
        "debug_client", 
        session_string=session_string, 
        api_hash=API_HASH, 
        api_id=API_ID,
        no_updates=True
    )
    
    try:
        await client.connect()
        print(f"Connected to Telegram with session.")
        
        # First try: Direct chat access
        try:
            print(f"Trying to get chat directly with ID: {chat_id}")
            chat = await client.get_chat(chat_id)
            print(f"SUCCESS: Got chat info directly - Title: {chat.title}, ID: {chat.id}")
        except Exception as e:
            print(f"FAILED: Direct chat access - {str(e)}")
        
        # Second try: Try with direct -100 format (sometimes Pyrogram requires this specific format)
        if isinstance(chat_id, int) and not str(chat_id).startswith("-100"):
            try:
                new_chat_id = int(f"-100{str(chat_id).replace('-100', '')}")
                print(f"Trying to get chat with reformatted ID: {new_chat_id}")
                chat = await client.get_chat(new_chat_id)
                print(f"SUCCESS: Got chat with reformatted ID - Title: {chat.title}, ID: {chat.id}")
            except Exception as e:
                print(f"FAILED: Reformatted chat ID - {str(e)}")
        
        # Third try: Try to get the message first, then the chat
        if message_id:
            try:
                print(f"Trying to get message first: chat_id={chat_id}, message_id={message_id}")
                msg = await client.get_messages(chat_id, message_id)
                if msg and hasattr(msg, 'chat'):
                    print(f"SUCCESS: Got message and chat info - Title: {msg.chat.title}, ID: {msg.chat.id}")
                else:
                    print(f"FAILED: Message found but no chat info")
            except Exception as e:
                print(f"FAILED: Message access - {str(e)}")
        
        # Fourth try: Check if we need to join this chat first
        try:
            print(f"Trying to get chat info via get_dialogs() - looking for {chat_id}")
            matching_chat = None
            async for dialog in client.get_dialogs():
                if str(dialog.chat.id) == str(chat_id) or (isinstance(chat_id, str) and dialog.chat.username and dialog.chat.username.lower() == chat_id.lower()):
                    matching_chat = dialog.chat
                    break
            
            if matching_chat:
                print(f"SUCCESS: Found chat in dialogs - Title: {matching_chat.title}, ID: {matching_chat.id}")
            else:
                print(f"FAILED: Chat not found in dialogs")
        except Exception as e:
            print(f"FAILED: Dialog search - {str(e)}")
        
        # Fifth try: Use raw API if all else fails
        try:
            print("Trying raw API access...")
            if isinstance(chat_id, int):
                raw_chat_id = chat_id
                if not str(chat_id).startswith("-100"):
                    raw_chat_id = int(f"-100{str(chat_id).replace('-100', '')}")
                
                result = await client.invoke(
                    functions.channels.GetChannels(
                        id=[types.InputChannel(
                            channel_id=int(str(raw_chat_id).replace("-100", "")),
                            access_hash=0
                        )]
                    )
                )
                print(f"SUCCESS with raw API: {result}")
            else:
                print("SKIPPED: Raw API access only works with numeric channel IDs")
        except Exception as e:
            print(f"FAILED: Raw API access - {str(e)}")
        
    except Exception as e:
        print(f"Error in debug script: {e}")
    finally:
        await client.disconnect()
        print("Disconnected from Telegram")

async def main():
    if len(sys.argv) < 3:
        print("Usage: python debug_chatid.py <chat_id> <message_id>")
        return
    
    chat_id_input = sys.argv[1]
    message_id = int(sys.argv[2])
    
    # Convert chat_id to correct format
    chat_id = chat_id_input
    if chat_id_input.isdigit():
        chat_id = int(chat_id_input)
    
    # Get a session string from database
    user_data = database.find_one()
    if not user_data or not user_data.get('session'):
        print("No session found in database. Please make sure a user is logged in.")
        return
    
    session_string = user_data.get('session')
    await debug_chat_id(chat_id, message_id, session_string)

if __name__ == "__main__":
    asyncio.run(main()) 