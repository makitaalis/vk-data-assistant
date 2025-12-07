#!/usr/bin/env python3
"""
Live Chat Authentication for +77086221203
Usage: Script waits for code to be written to chat_code.txt in real-time
"""
import asyncio
import os
import time
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneNumberInvalidError
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_NAME = "user_session_77086221203"
PHONE_NUMBER = "+77086221203"
CHAT_CODE_FILE = "/home/vkbot/vk-data-assistant/chat_code.txt"

# Create empty chat code file if it doesn't exist
if not os.path.exists(CHAT_CODE_FILE):
    with open(CHAT_CODE_FILE, 'w') as f:
        f.write("")

client = None

async def monitor_chat_for_code():
    """Monitor chat_code.txt for new code input"""
    global client

    print("=" * 60)
    print(f"🔐 LIVE TELEGRAM AUTHENTICATION")
    print(f"📱 Phone: {PHONE_NUMBER}")
    print("=" * 60)

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

        print("\n🎯 READY FOR CODE INPUT!")
        print(f"💬 Waiting for you to write the code in chat...")
        print(f"📁 Monitoring file: {CHAT_CODE_FILE}")
        print("⚡ When you send code, authentication will start automatically!")
        print("-" * 40)

        # Monitor file for changes
        last_content = ""
        wait_time = 0

        while True:
            try:
                with open(CHAT_CODE_FILE, 'r') as f:
                    current_content = f.read().strip()

                # Check if content changed and is not empty
                if current_content != last_content and current_content:
                    code = current_content
                    print(f"\n📝 Got code from chat: {code}")
                    print("🔄 Authenticating...")

                    try:
                        await client.sign_in(PHONE_NUMBER, code)
                        print("✅ Authentication successful!")

                        # Verify
                        me = await client.get_me()
                        print(f"\n🎉 SUCCESS! Authenticated as:")
                        print(f"   Name: {me.first_name} {me.last_name or ''}")
                        print(f"   Phone: {me.phone}")
                        print(f"   ID: {me.id}")

                        # Clear the file
                        with open(CHAT_CODE_FILE, 'w') as f:
                            f.write("")

                        return True

                    except SessionPasswordNeededError:
                        print("\n🔒 Two-factor authentication required")
                        print("🔑 Using stored 2FA password...")
                        password = "5vmoqi3tgjf"
                        await client.sign_in(password=password)
                        print("✅ 2FA authentication successful!")

                        # Verify
                        me = await client.get_me()
                        print(f"\n🎉 SUCCESS! Authenticated as:")
                        print(f"   Name: {me.first_name} {me.last_name or ''}")
                        print(f"   Phone: {me.phone}")
                        print(f"   ID: {me.id}")

                        # Clear the file
                        with open(CHAT_CODE_FILE, 'w') as f:
                            f.write("")

                        return True

                    except Exception as auth_error:
                        print(f"❌ Authentication failed: {auth_error}")
                        print("💡 Make sure the code is correct and try again")
                        # Clear the file so user can try again
                        with open(CHAT_CODE_FILE, 'w') as f:
                            f.write("")
                        last_content = ""
                        continue

                last_content = current_content
                wait_time += 1

                # Show waiting status every 10 seconds
                if wait_time % 10 == 0:
                    print(f"⏳ Still waiting for code... ({wait_time}s)")

                await asyncio.sleep(1)

                if wait_time > 600:  # 10 minutes timeout
                    print("\n❌ Timeout waiting for code")
                    return False

            except FileNotFoundError:
                await asyncio.sleep(1)
                continue

    except Exception as e:
        print(f"\n❌ Authentication error: {e}")
        return False
    finally:
        if client:
            await client.disconnect()
            print("👋 Disconnected from Telegram")

if __name__ == "__main__":
    try:
        success = asyncio.run(monitor_chat_for_code())
        print(f"\n{'✅ AUTHENTICATION COMPLETED!' if success else '❌ AUTHENTICATION FAILED'}")
    except KeyboardInterrupt:
        print("\n⚠️ Authentication cancelled")
    except Exception as e:
        print(f"\n❌ Error: {e}")