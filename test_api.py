# test_api.py - Quick test of DeepSeek API connection
import asyncio
import aiohttp
import os
from dotenv import load_dotenv

load_dotenv()

async def test_deepseek():
    """Test DeepSeek API connection"""
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    
    if not api_key:
        print("ERROR: DEEPSEEK_API_KEY not found in .env file")
        return
    
    print(f"API Key found: {api_key[:10]}...")
    
    url = "https://api.deepseek.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Say 'API test successful' and nothing else."}
        ],
        "temperature": 0.1,
    }
    
    print("Testing DeepSeek API connection...")
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                url,
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                print(f"Response status: {response.status}")
                
                if response.status == 200:
                    data = await response.json()
                    result = data["choices"][0]["message"]["content"]
                    print(f"API Response: {result}")
                    print("✓ DeepSeek API connection successful!")
                else:
                    text = await response.text()
                    print(f"ERROR: {text}")
                    
        except Exception as e:
            print(f"Connection error: {e}")

if __name__ == "__main__":
    asyncio.run(test_deepseek())