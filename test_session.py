#!/usr/bin/env python3
"""
Quick test of the configured session (default: +15167864134)
"""
import asyncio
import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session_15167864134")
PHONE = os.environ.get("ACCOUNT_PHONE", "+15167864134")

_raw_bot_username = os.environ.get("VK_BOT_USERNAME", "sherlock_bot_ne_bot")
if _raw_bot_username and not _raw_bot_username.startswith("@"):
    _raw_bot_username = f"@{_raw_bot_username}"
VK_BOT_USERNAME = _raw_bot_username

async def test_session():
    """Test the specific session"""
    proxy = {
        'proxy_type': 'socks5',
        'addr': '194.31.73.124',
        'port': 60741,
        'username': 'QzYtokLcGL',
        'password': '4MR8FmpoKN',
        'rdns': True
    }
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH, proxy=proxy)
    
    try:
        print(f"🔗 Connecting to session for {PHONE}...")
        await client.start(phone=PHONE)
        
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"✅ Session works! User: {me.first_name} {me.last_name or ''}")
            print(f"📱 Phone: {me.phone}")
            print(f"🆔 ID: {me.id}")
            
            # Test bot access
            try:
                bot = await client.get_entity(VK_BOT_USERNAME)
                print(f"✅ Bot accessible: {bot.first_name}")
                
                # Send test message
                msg = await client.send_message(bot, "/start")
                print(f"✅ Test message sent (ID: {msg.id})")
                
                return True
            except Exception as e:
                print(f"❌ Bot error: {e}")
                return False
        else:
            print("❌ Session not authorized")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        await client.disconnect()

if __name__ == "__main__":
    success = asyncio.run(test_session())
    print(f"\n{'✅ Success' if success else '❌ Failed'}")
