# /home/test/project/proxy/proxy_app/main.py
import asyncio
import websockets
import json
import aiohttp
import os
from logger import setup_logger

# Initialize the logger
logger = setup_logger()

# Read environment variables
DB_HANDLER_BASE_URL = os.getenv("DB_HANDLER_URL")
ACCOUNT_HANDLER_URL = f"{DB_HANDLER_BASE_URL}/update_account_info"
SYMBOLS_SYNC_URL = f"{DB_HANDLER_BASE_URL}/sync_symbols"
GET_SYMBOLS_URL = f"{DB_HANDLER_BASE_URL}/get_symbols" # New endpoint

async def handler(websocket, http_session):
    logger.info(f"Agent connected from {websocket.remote_address}")
    try:
        async for message_str in websocket:
            try:
                message_data = json.loads(message_str)
                msg_type = message_data.get("type")

                if msg_type == "account_info":
                    logger.info("Forwarding account_info to DB handler...")
                    async with http_session.post(ACCOUNT_HANDLER_URL, json=message_data.get("data")) as resp:
                        if resp.status != 200:
                            logger.error(f"DB handler returned error {resp.status} for account_info")

                elif msg_type == "symbols_info_sync":
                    login = message_data.get('login')
                    logger.info(f"Forwarding symbol batch for login {login}")
                    async with http_session.post(SYMBOLS_SYNC_URL, json=message_data) as resp:
                        if resp.status != 200:
                             logger.error(f"DB handler returned error {resp.status} for symbols_info_sync")

                elif msg_type == "get_db_symbols":
                    login = message_data.get('login')
                    if not login:
                        logger.warning("Received get_db_symbols request without login number.")
                        continue
                    
                    request_url = f"{GET_SYMBOLS_URL}/{login}"
                    logger.info(f"Requesting symbols for login {login} from {request_url}")
                    
                    async with http_session.get(request_url) as resp:
                        if resp.status == 200:
                            symbols_data = await resp.json()
                            logger.info(f"Successfully retrieved {len(symbols_data)} symbols for login {login}")
                            # ?? FIX: The symbols_data variable is now correctly included
                            await websocket.send(json.dumps({"type": "db_symbols_list", "data": symbols_data}))
                        else:
                            logger.error(f"DB handler returned error {resp.status} for get_db_symbols for login {login}")
                            # Optionally send an error back to the client
                            await websocket.send(json.dumps({"type": "db_symbols_list", "error": f"Failed to retrieve symbols, status: {resp.status}"}))

            except json.JSONDecodeError:
                logger.error(f"Could not decode JSON from message: {message_str}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Agent disconnected from {websocket.remote_address}")

async def main():
    if not DB_HANDLER_BASE_URL:
        logger.critical("FATAL: DB_HANDLER_URL is not set. Application cannot start.")
        return

    # Create a single, reusable aiohttp session for the lifetime of the application
    async with aiohttp.ClientSession() as http_session:
        # Pass the session to the handler
        websocket_handler = lambda ws: handler(ws, http_session)
        async with websockets.serve(websocket_handler, "0.0.0.0", 9000):
            logger.info("?? Proxy Server started successfully on port 9000")
            await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())