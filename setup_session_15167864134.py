#!/usr/bin/env python3
"""
Setup script for the current Telegram session (default: 15167864134)
Run this to authenticate your session
"""
import asyncio
import os
import sys
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneNumberInvalidError
from dotenv import load_dotenv

load_dotenv()

# Configuration for your specific session
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session_15167864134")
PHONE = os.environ.get("ACCOUNT_PHONE", "+15167864134")

_primary_bot = os.environ.get("VK_BOT_USERNAME", "sherlock_bot_ne_bot")
if _primary_bot and not _primary_bot.startswith("@"):
    _primary_bot = f"@{_primary_bot}"

async def setup_session():
    """Setup and authenticate the session"""
    print("=" * 60)
    print(f"🔐 TELEGRAM SESSION SETUP FOR {PHONE}")
    print("=" * 60)
    print(f"📱 Phone: {PHONE}")
    print(f"💾 Session file: {SESSION_NAME}.session")
    print("=" * 60)
    
    # Proxy settings
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
        print("🔗 Connecting to Telegram...")
        await client.connect()
        
        if await client.is_user_authorized():
            print("✅ Session already authorized!")
            me = await client.get_me()
            print(f"👤 User: {me.first_name} {me.last_name or ''}")
            print(f"📱 Phone: {me.phone}")
            
            # Test bot access
            print(f"\n🤖 Testing bot access ({_primary_bot})...")
            try:
                bot = await client.get_entity(_primary_bot)
                print(f"✅ Bot found: {bot.first_name}")
                
                # Send test message
                msg = await client.send_message(bot, "/start")
                print(f"✅ Test message sent (ID: {msg.id})")
                
                print("\n🎉 SESSION IS READY!")
                return True
            except Exception as e:
                print(f"❌ Bot access error: {e}")
                return False
        else:
            print("❌ Session not authorized. Manual authentication required.")
            print("\nTo authenticate your session:")
            print("1. Run: python auth_session_15167864134.py")
            print("2. Enter the verification code from Telegram")
            print("3. Run this script again to verify")
            return False
            
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False
    finally:
        await client.disconnect()

if __name__ == "__main__":
    success = asyncio.run(setup_session())
    
    if success:
        print("\n" + "=" * 60)
        print("✅ SESSION READY! You can now run your bot:")
        print("   python run.py")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("❌ Session setup failed.")
        print("Please run: python auth_session_15167864134.py")
        print("=" * 60)
        sys.exit(1)
