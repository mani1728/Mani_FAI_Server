# ==================================================================
# File: Mani_FAI_Server/proxy/proxy_app/main.py (Kafka Version)
# Description: نسخه بازنویسی شده پراکسی برای کار با کافکا.
# ==================================================================
import asyncio
import websockets
import json
import aiohttp
import os
from aiokafka import AIOKafkaProducer  # کتابخانه کافکا برای asyncio
from logger import setup_logger

# Initialize the logger
logger = setup_logger()

# --- Kafka Settings ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# --- DB Handler HTTP Settings (برای درخواست‌های Request/Response) ---
DB_HANDLER_BASE_URL = os.getenv("DB_HANDLER_URL")
GET_SYMBOLS_URL = f"{DB_HANDLER_BASE_URL}/get_symbols"

# --- Kafka Topics ---
# بهتر است نام تاپیک‌ها را نیز از متغیرهای محیطی بخوانید
TOPIC_ACCOUNT_INFO = "account_info"
TOPIC_SYMBOLS_SYNC = "symbols_info_sync"
TOPIC_RATES_SYNC = "sync_rates_data"


async def get_kafka_producer():
    """یک تولیدکننده کافکا ایجاد و آن را برای استفاده آماده می‌کند."""
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    logger.info(f"Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    return producer


async def handler(websocket, http_session, kafka_producer):
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
                
                # تبدیل پیام به فرمت বাইт برای ارسال به کافکا
                message_bytes = message_str.encode('utf-8')

                if msg_type == "account_info":
                    logger.info("Producing account_info message to Kafka...")
                    await kafka_producer.send_and_wait(TOPIC_ACCOUNT_INFO, message_bytes)

                elif msg_type == "symbols_info_sync":
                    login = message_data.get('login')
                    symbol_count = len(message_data.get('symbols', []))
                    logger.info(f"Producing symbol batch for login {login} with {symbol_count} symbols to Kafka...")
                    await kafka_producer.send_and_wait(TOPIC_SYMBOLS_SYNC, message_bytes)

                elif msg_type == "sync_rates_data":
                    login = message_data.get('login')
                    symbol = message_data.get('symbol')
                    rates_count = len(message_data.get('data', []))
                    logger.info(f"Producing {rates_count} rates for '{symbol}' for login {login} to Kafka...")
                    await kafka_producer.send_and_wait(TOPIC_RATES_SYNC, message_bytes)

                elif msg_type == "get_db_symbols":
                    # این بخش همچنان از HTTP استفاده می‌کند
                    login = message_data.get('login')
                    if not login:
                        logger.warning("Received get_db_symbols request without login number.")
                        continue
                    
                    request_url = f"{GET_SYMBOLS_URL}/{login}"
                    logger.info(f"Requesting symbols via HTTP for login {login} from {request_url}")
                    
                    async with http_session.get(request_url) as resp:
                        if resp.status == 200:
                            symbols_data = await resp.json()
                            logger.info(f"Successfully retrieved {len(symbols_data)} symbols for login {login}")
                            await websocket.send(json.dumps({"type": "db_symbols_list", "data": symbols_data}))
                        else:
                            logger.error(f"DB handler returned HTTP error {resp.status} for get_db_symbols for login {login}")
                            await websocket.send(json.dumps({"type": "db_symbols_list", "error": f"Failed, status: {resp.status}"}))

            except json.JSONDecodeError:
                logger.error(f"Could not decode JSON from message: {message_str}")
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Agent disconnected from {websocket.remote_address}")


async def main():
    if not DB_HANDLER_BASE_URL or not KAFKA_BOOTSTRAP_SERVERS:
        logger.critical("FATAL: DB_HANDLER_URL or KAFKA_BOOTSTRAP_SERVERS is not set.")
        return

    kafka_producer = await get_kafka_producer()
    
    try:
        async with aiohttp.ClientSession() as http_session:
            # پاس دادن kafka_producer و http_session به هر کانکشن جدید
            bound_handler = lambda ws: handler(ws, http_session, kafka_producer)
            async with websockets.serve(bound_handler, "0.0.0.0", 9000, max_size=2**24):
                logger.info("Kafka-based Proxy Server started successfully on port 9000")
                await asyncio.Future()
    finally:
        logger.info("Stopping Kafka producer...")
        await kafka_producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server is shutting down.")