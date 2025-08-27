# ==================================================================
# File: Mani_FAI_Server/proxy/proxy_app/main.py
# Description: کد کامل و نهایی سرور پراکسی.
# تغییر اصلی: افزودن منطق برای مدیریت پیام جدید sync_rates_data.
# ==================================================================
import asyncio
import websockets
import json
import aiohttp
import os
from logger import setup_logger

logger = setup_logger()
DB_HANDLER_BASE_URL = os.getenv("DB_HANDLER_URL")
ACCOUNT_HANDLER_URL = f"{DB_HANDLER_BASE_URL}/update_account_info"
SYMBOLS_SYNC_URL = f"{DB_HANDLER_BASE_URL}/sync_symbols"
GET_SYMBOLS_URL = f"{DB_HANDLER_BASE_URL}/get_symbols"
# مسیر جدید برای ارسال داده‌های کندل
RATES_DATA_SYNC_URL = f"{DB_HANDLER_BASE_URL}/sync_rates_data"

async def handler(websocket, http_session):
    logger.info(f"Agent connected from {websocket.remote_address}")
    try:
        async for message_str in websocket:
            try:
                logger.debug(f"Received raw message: {message_str[:200]}...")
                message_data = json.loads(message_str)
                
                if not isinstance(message_data, dict):
                    logger.error(f"Parsed message is not a dictionary. Type: {type(message_data)}")
                    continue

                msg_type = message_data.get("type")

                if msg_type == "account_info":
                    # ... (بدون تغییر)
                    logger.info("Forwarding account_info to DB handler...")
                    account_data = message_data.get("data")
                    if account_data:
                        async with http_session.post(ACCOUNT_HANDLER_URL, json=account_data) as resp:
                            if resp.status != 200: logger.error(f"DB handler returned error {resp.status} for account_info")
                    else:
                        logger.warning("account_info message received without 'data' field.")

                elif msg_type == "symbols_info_sync":
                    # ... (بدون تغییر)
                    login = message_data.get('login')
                    symbols = message_data.get('symbols')
                    if not login or symbols is None:
                        logger.error("Received symbols_info_sync with missing 'login' or 'symbols'.")
                        continue
                    logger.info(f"Forwarding symbol batch for login {login} with {len(symbols)} symbols.")
                    payload_to_forward = {"login": login, "symbols": symbols}
                    async with http_session.post(SYMBOLS_SYNC_URL, json=payload_to_forward) as resp:
                        if resp.status != 200: logger.error(f"DB handler returned error {resp.status} for symbols_info_sync")

                elif msg_type == "sync_rates_data":
                    # *** بخش جدید برای مدیریت پیام داده‌های کندل ***
                    login = message_data.get('login')
                    symbol = message_data.get('symbol')
                    rates = message_data.get('data')
                    
                    if not all([login, symbol, rates is not None]):
                        logger.error("Received sync_rates_data with missing fields.")
                        continue
                        
                    logger.info(f"Forwarding {len(rates)} rates for '{symbol}' for login {login} to DB handler.")
                    # بسته را مستقیماً به db-handler ارسال می‌کنیم (چون ساختار آن صحیح است)
                    async with http_session.post(RATES_DATA_SYNC_URL, json=message_data) as resp:
                        if resp.status != 200:
                            logger.error(f"DB handler returned error {resp.status} for sync_rates_data on symbol '{symbol}'")

                elif msg_type == "get_db_symbols":
                    # ... (بدون تغییر)
                    login = message_data.get('login')
                    if not login:
                        logger.warning("Received get_db_symbols without login.")
                        continue
                    request_url = f"{GET_SYMBOLS_URL}/{login}"
                    logger.info(f"Requesting symbols for login {login} from {request_url}")
                    async with http_session.get(request_url) as resp:
                        if resp.status == 200:
                            symbols_data = await resp.json()
                            logger.info(f"Successfully retrieved {len(symbols_data)} symbols for login {login}")
                            await websocket.send(json.dumps({"type": "db_symbols_list", "data": symbols_data}))
                        else:
                            logger.error(f"DB handler returned error {resp.status} for get_db_symbols for login {login}")
                            await websocket.send(json.dumps({"type": "db_symbols_list", "error": f"Failed, status: {resp.status}"}))

            except json.JSONDecodeError:
                logger.error(f"Could not decode JSON from message: {message_str}")
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Agent disconnected from {websocket.remote_address}")

async def main():
    if not DB_HANDLER_BASE_URL:
        logger.critical("FATAL: DB_HANDLER_URL is not set.")
        return
    async with aiohttp.ClientSession() as http_session:
        websocket_handler = lambda ws: handler(ws, http_session)
        async with websockets.serve(websocket_handler, "0.0.0.0", 9000, max_size=2**24):
            logger.info("Proxy Server started successfully on port 9000")
            await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
