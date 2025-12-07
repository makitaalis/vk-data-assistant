#!/usr/bin/env python3
"""
Interactive Telegram Authentication for +77086221203
"""
import asyncio
import os
import sys
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneNumberInvalidError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = "user_session_77086221203"
PHONE_NUMBER = "+77086221203"

async def interactive_auth():
    """Interactive authentication process"""
    print("=" * 60)
    print(f"🔐 TELEGRAM INTERACTIVE AUTHENTICATION")
    print(f"📱 Phone: {PHONE_NUMBER}")
    print("=" * 60)

    # Create client without proxy first
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    try:
        await client.connect()

        if await client.is_user_authorized():
            print("✅ Session already authorized!")
            me = await client.get_me()
            print(f"✅ Connected as: {me.first_name} {me.last_name or ''} ({me.phone})")
            return True

        print(f"📞 Sending auth code to {PHONE_NUMBER}...")
        await client.send_code_request(PHONE_NUMBER)

        print("\n⚠️  IMPORTANT: Check Telegram on your phone!")
        print("📱 You will receive a message with code from Telegram")
        print("💬 Code format: 12345 (5 digits)")
        print("-" * 40)
        print("🔄 Waiting for code input...")

        # Get code from stdin
        try:
            code = input("✏️  Enter code from Telegram: ")
            print(f"📝 Received code: {code}")

            # Sign in with code
            await client.sign_in(PHONE_NUMBER, code)
            print("✅ Authentication successful!")

        except SessionPasswordNeededError:
            print("\n🔒 Two-factor authentication required")
            password = input("🔑 Enter 2FA password: ")
            await client.sign_in(password=password)
            print("✅ 2FA authentication successful!")

        except Exception as e:
            print(f"❌ Sign in error: {e}")
            return False

        # Verify authentication
        me = await client.get_me()
        print(f"\n✅ Successfully authenticated as:")
        print(f"   Name: {me.first_name} {me.last_name or ''}")
        print(f"   Phone: {me.phone}")
        print(f"   ID: {me.id}")

        print("\n🎉 SESSION READY TO USE!")
        return True

    except PhoneNumberInvalidError:
        print(f"❌ Invalid phone number: {PHONE_NUMBER}")
        return False
    except Exception as e:
        print(f"❌ Authentication error: {e}")
        return False
    finally:
        await client.disconnect()
        print("👋 Disconnected from Telegram")

if __name__ == "__main__":
    try:
        success = asyncio.run(interactive_auth())
        if success:
            print(f"\n✅ Authentication completed successfully!")
            print(f"📁 Session file: {SESSION_NAME}.session")
        else:
            print("\n❌ Authentication failed.")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️ Authentication cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)