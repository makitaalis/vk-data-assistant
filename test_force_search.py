#!/usr/bin/env python3
"""Test force search functionality"""

import asyncio
import sys
sys.path.append('/home/vkbot/vk-data-assistant')

from services.vk_service import VKService

async def test_force_search():
    """Test force search for a single VK link"""
    vk_service = VKService(
        api_id=13801751,
        api_hash="ba0fdc4c9c75c16ab3013af244f594e9",
        session_name="user_session",
        phone="+380930157086"
    )
    
    # Test link
    link = "https://vk.com/id1"  # Pavel Durov
    
    print(f"🔍 Testing force search for: {link}")
    print("=" * 50)
    
    try:
        # Initialize VK service
        await vk_service.initialize()
        print("✅ VK Service initialized")
        
        # Perform search
        print(f"\n🔎 Searching {link}...")
        result = await vk_service.search_vk_link(link)
        
        if result:
            print(f"\n✅ Search completed!")
            print(f"📱 Phones found: {result.get('phones', [])}")
            print(f"👤 Full name: {result.get('full_name', 'Not found')}")
            print(f"📅 Birth date: {result.get('birth_date', 'Not found')}")
        else:
            print("❌ No results found")
            
    except Exception as e:
        print(f"❌ Error during search: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Close VK service
        await vk_service.close()
        print("\n✅ Test completed")

if __name__ == "__main__":
    asyncio.run(test_force_search())
