"""
ai selfbot
to get your token: https://youtu.be/OUd37MWgBfs
if your smart enough you can add a prompt
originally made by dex4tw
"""
import os
import sys
import requests
import json
import asyncio
import websockets
import toml
import queue
import threading
import google.generativeai as genai
import aiohttp
from google.generativeai.types import HarmCategory, HarmBlockThreshold

from stayawake import host

os.system('cls' if os.name == 'nt' else 'clear')
print("Starting..")

class GeminiBot:
    def __init__(self):
        host()

        self.config = toml.load(open(os.path.join(os.getcwd(), "bin", "config.toml")))
        self.auth_headers = {"Authorization": self.config["Token"]}
        self.user_id = self.config["UserID"]
        self.allowed_channels = self.config["Channels"]
        self.trigger = self.config.get("Trigger", "@")
        
        genai.configure(api_key=self.config.get("GeminiAPIKey", "api key here"))
        self.model = genai.GenerativeModel('gemini-1.5-flash-latest')
        
        self.chat_sessions = queue.Queue()
        self.max_concurrent_sessions = 5  
        self.session_lock = threading.Lock()
        self.initialize_chat_sessions()
        
        self.request_queue = asyncio.Queue()
        self.max_workers = 3  

    def initialize_chat_sessions(self):
        for _ in range(self.max_concurrent_sessions):
            self.chat_sessions.put(self.model.start_chat(history=[]))

    async def get_chat_session(self):
        while True:
            try:
                return self.chat_sessions.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.1)

    async def release_chat_session(self, session):
        self.chat_sessions.put(session)

    async def send_message(self, channel_id, content, reply_to=None):
        data = {
            "content": content,
            "tts": False
        }
        if reply_to:
            data["message_reference"] = {
                "message_id": reply_to,
                "fail_if_not_exists": False
            }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"https://discord.com/api/v9/channels/{channel_id}/messages",
                        json=data,
                        headers=self.auth_headers
                    ) as resp:
                        if resp.status == 429:
                            retry_after = float(resp.headers.get('Retry-After', 1))
                            await asyncio.sleep(retry_after)
                            continue
                        return await resp.json()
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"Failed to send message after {max_retries} attempts: {e}")
                await asyncio.sleep(1)

    def typing_start(self, channel_id):
        try:
            requests.post(
                f"https://discord.com/api/v9/channels/{channel_id}/typing",
                headers=self.auth_headers,
                timeout=2
            )
        except requests.RequestException:
            pass

    async def process_message(self, message):
        author = message["author"]
        content = message["content"]
        message_id = message["id"]
        channel_id = message["channel_id"]
        mentions = message.get("mentions", [])

        if author["id"] == self.user_id:
            return

        is_triggered = any(user['id'] == self.user_id for user in mentions) or (
            self.trigger != "@" and content.startswith(self.trigger)
        )
        
        if not is_triggered:
            return

        content = content.replace(f"<@{self.user_id}>", "").strip()
        if self.trigger != "@" and content.startswith(self.trigger):
            content = content[len(self.trigger):].strip()

        if self.allowed_channels and channel_id not in self.allowed_channels:
            return

        if content.lower() == "reset":
            with self.session_lock:
                self.initialize_chat_sessions()
            await self.send_message(channel_id, "All chat sessions have been reset!")
            return

        try:
            print(f"Processing message from {author['username']}: {content}")
            self.typing_start(channel_id)
            
            chat_session = await self.get_chat_session()
            
            try:
                response = await asyncio.to_thread(
                    chat_session.send_message,
                    content,
                    stream=False,  
                    safety_settings={ # safety settings if u can understand then u can change it
                        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
                        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
                        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
                        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
                    }
                )
                
                response_text = response.text[:2000] if len(response.text) > 2000 else response.text
                await self.send_message(channel_id, response_text, message_id)
            finally:
                await self.release_chat_session(chat_session)
                
        except Exception as e:
            print(f"Error processing message: {e}")
            await self.send_message(
                channel_id, 
                "I encountered an error processing your request. Please try again later.", 
                message_id
            )

    async def message_worker(self):
        while True:
            message = await self.request_queue.get()
            try:
                await self.process_message(message)
            except Exception as e:
                print(f"Worker error: {e}")
            finally:
                self.request_queue.task_done()

    async def run(self):
        workers = [asyncio.create_task(self.message_worker()) 
                 for _ in range(self.max_workers)]
        
        async def send_heartbeat(ws, interval):
            while True:
                try:
                    await ws.send(json.dumps({"op": 1, "d": None}))
                    await asyncio.sleep(interval / 1000)
                except Exception:
                    break

        reconnect_delay = 1
        max_reconnect_delay = 60
        
        while True:
            try:
                async with websockets.connect(
                    "wss://gateway.discord.gg/?v=9&encoding=json",
                    max_size=2**20  # 1mb max message size
                ) as ws:
                    print("Connected to Discord Gateway")
                    hello = json.loads(await ws.recv())
                    heartbeat_interval = hello['d']['heartbeat_interval'] / 1000
                    heartbeat_task = asyncio.create_task(
                        send_heartbeat(ws, heartbeat_interval)
                    )
                    
                    await ws.send(json.dumps({
                        'op': 2,
                        'd': {
                            'token': self.config["Token"],
                            'intents': 1 << 15,  
                            'properties': {
                                '$os': sys.platform,
                                '$browser': 'Firefox', # change this to whatever you want
                                '$device': 'CrustyIpad' # change this to whatever youw ant
                            }
                        }
                    }))
                    
                    reconnect_delay = 1  
                    
                    while True:
                        try:
                            message = await ws.recv()
                            data = json.loads(message)
                            
                            if data.get('op') == 10:  # Hello
                                continue
                                
                            event = data.get('t')
                            if event == 'READY':
                                print('Bot is fully connected and ready!')
                            elif event == "MESSAGE_CREATE":
                                await self.request_queue.put(data.get('d'))
                                
                        except websockets.ConnectionClosed:
                            print("Connection closed, reconnecting...")
                            break
                        except Exception as e:
                            print(f"Gateway error: {e}")
                            break
                            
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except:
                        pass
                        
            except Exception as e:
                print(f"Connection failed: {e}")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                continue

if __name__ == "__main__":
    bot = GeminiBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\nshutting down")
        sys.exit(0)
