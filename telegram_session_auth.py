#!/usr/bin/env python3
"""
Telegram Session Authentication for specific user session
Using session ID configured via SESSION_NAME/ACCOUNT_PHONE (.env default: 15167864134)
"""
import asyncio
import os
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneNumberInvalidError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = os.environ.get("SESSION_NAME", "user_session_15167864134")
ACCOUNT_PHONE = os.environ.get("ACCOUNT_PHONE", "+15167864134")

_raw_bot_username = os.environ.get("VK_BOT_USERNAME", "sherlock_bot_ne_bot")
if _raw_bot_username and not _raw_bot_username.startswith("@"):
    _raw_bot_username = f"@{_raw_bot_username}"
VK_BOT_USERNAME = _raw_bot_username

class TelegramSessionAuth:
    """Class to handle Telegram authentication for specific session"""
    
    def __init__(self, phone_number: str = None):
        self.phone_number = phone_number or ACCOUNT_PHONE
        self.api_id = API_ID
        self.api_hash = API_HASH
        self.session_name = SESSION_NAME
        self.client = None
        
        # Proxy settings (if needed)
        self.proxy = {
            'proxy_type': 'socks5',
            'addr': '194.31.73.124',
            'port': 60741,
            'username': 'QzYtokLcGL',
            'password': '4MR8FmpoKN',
            'rdns': True
        }
    
    async def create_client(self, use_proxy: bool = True):
        """Create Telegram client with or without proxy"""
        proxy_config = self.proxy if use_proxy else None
        self.client = TelegramClient(
            self.session_name, 
            self.api_id, 
            self.api_hash, 
            proxy=proxy_config
        )
        return self.client
    
    async def connect_session(self):
        """Connect to existing session"""
        if not self.client:
            await self.create_client()
        
        print(f"🔗 Connecting to Telegram session for {self.phone_number}...")
        try:
            await self.client.connect()
            
            if await self.client.is_user_authorized():
                me = await self.client.get_me()
                print(f"✅ Session active: {me.first_name} {me.last_name or ''} ({me.phone})")
                return True
            else:
                print("❌ Session not authorized. Need to authenticate first.")
                return False
        except Exception as e:
            print(f"❌ Connection error: {e}")
            # Try to start the client with the phone number
            try:
                await self.client.start(phone=self.phone_number)
                if await self.client.is_user_authorized():
                    me = await self.client.get_me()
                    print(f"✅ Session restarted: {me.first_name} {me.last_name or ''} ({me.phone})")
                    return True
            except Exception as e2:
                print(f"❌ Failed to restart session: {e2}")
            return False
    
    async def authenticate_session(self):
        """Full authentication process for new session"""
        if not self.client:
            await self.create_client()
        
        print("=" * 60)
        print(f"🔐 TELEGRAM SESSION AUTHENTICATION")
        print(f"📱 Phone: {self.phone_number}")
        print("=" * 60)
        
        await self.client.connect()
        
        if await self.client.is_user_authorized():
            print("✅ Session already authorized!")
            return await self.verify_session()
        
        try:
            print(f"📞 Sending auth code to {self.phone_number}...")
            await self.client.send_code_request(self.phone_number)
            
            print("\n⚠️  IMPORTANT: Check Telegram on your phone!")
            print("📱 You will receive a message with code from Telegram")
            print("💬 Code format: 12345 (5 digits)")
            print("-" * 40)
            
            code = input("✏️  Enter code from Telegram: ")
            
            try:
                await self.client.sign_in(self.phone_number, code)
                print("✅ Authentication successful!")
                
            except SessionPasswordNeededError:
                print("\n🔒 Two-factor authentication required")
                password = input("🔑 Enter password: ")
                await self.client.sign_in(password=password)
                print("✅ 2FA authentication successful!")
                
        except PhoneNumberInvalidError:
            print(f"❌ Invalid phone number: {self.phone_number}")
            return False
        except Exception as e:
            print(f"❌ Authentication error: {e}")
            return False
        
        return await self.verify_session()
    
    async def verify_session(self):
        """Verify session and test bot access"""
        try:
            # Check connection
            me = await self.client.get_me()
            print(f"\n✅ Connected as: {me.first_name} {me.last_name or ''}")
            print(f"📱 Phone: {me.phone}")
            print(f"🆔 ID: {me.id}")
            
            # Test bot access
            print(f"\n🤖 Testing access to bot {VK_BOT_USERNAME}...")
            bot = await self.client.get_entity(VK_BOT_USERNAME)
            print(f"✅ Bot found: {bot.first_name}")
            
            # Send test message
            print("\n📤 Sending test message to bot...")
            msg = await self.client.send_message(bot, "/start")
            print(f"✅ Message sent (ID: {msg.id})")
            
            print("\n🎉 SESSION READY TO USE!")
            return True
            
        except Exception as e:
            print(f"❌ Session verification error: {e}")
            return False
    
    async def disconnect(self):
        """Disconnect from Telegram"""
        if self.client:
            await self.client.disconnect()
            print("👋 Disconnected from Telegram")
    
    async def get_session_info(self):
        """Get current session information"""
        if not self.client:
            await self.create_client()
        
        await self.client.connect()
        
        if await self.client.is_user_authorized():
            me = await self.client.get_me()
            return {
                "id": me.id,
                "phone": me.phone,
                "first_name": me.first_name,
                "last_name": me.last_name,
                "username": me.username,
                "is_authorized": True
            }
        else:
            return {"is_authorized": False}

async def quick_auth_check():
    """Quick check if session is working"""
    auth = TelegramSessionAuth()
    
    try:
        await auth.create_client()
        is_connected = await auth.connect_session()
        
        if is_connected:
            info = await auth.get_session_info()
            print(f"✅ Session working: {info['first_name']} {info['last_name']} ({info['phone']})")
        else:
            print("❌ Session not working. Run full authentication.")
        
        await auth.disconnect()
        return is_connected
        
    except Exception as e:
        print(f"❌ Error checking session: {e}")
        await auth.disconnect()
        return False

async def full_authentication():
    """Full authentication process"""
    auth = TelegramSessionAuth()
    
    try:
        success = await auth.authenticate_session()
        await auth.disconnect()
        return success
        
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        await auth.disconnect()
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "check":
        # Quick check mode
        success = asyncio.run(quick_auth_check())
    else:
        # Full authentication mode
        success = asyncio.run(full_authentication())
    
    if success:
        print("\n✅ Ready to use! Run your bot with:")
        print("   python run.py")
    else:
        print("\n❌ Authentication failed. Try again.")
        sys.exit(1)
