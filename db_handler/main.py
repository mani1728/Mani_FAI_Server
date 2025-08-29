# ==================================================================
# File: Mani_FAI_Server/db_handler/main.py (نسخه کامل و نهایی)
# Description: این سرویس هم به عنوان مصرف‌کننده کافکا و هم وب سرور عمل می‌کند.
# ==================================================================
import os
import re
import json
import asyncio
import logging
from flask import Flask, jsonify, request, Response
from pymongo import MongoClient, UpdateOne
from bson.json_util import dumps
from aiokafka import AIOKafkaConsumer
from threading import Thread

# --- تنظیمات کلی ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- تنظیمات کافکا ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_ACCOUNT_INFO = "account_info"
TOPIC_SYMBOLS_SYNC = "symbols_info_sync"
TOPIC_RATES_SYNC = "sync_rates_data"

# --- تنظیمات MongoDB ---
MONGO_URI = os.environ.get("MONGO_URI")
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30000)
logging.info("Successfully connected to MongoDB.")

# --- بخش مصرف‌کننده کافکا (Kafka Consumer) ---
async def consume_kafka_messages():
    consumer = AIOKafkaConsumer(
        TOPIC_ACCOUNT_INFO, TOPIC_SYMBOLS_SYNC, TOPIC_RATES_SYNC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="db_handler_group",
        auto_offset_reset='earliest'
    )
    await consumer.start()
    logging.info("Kafka Consumer started and subscribed to topics.")
    try:
        async for msg in consumer:
            try:
                message_data = json.loads(msg.value.decode('utf-8'))
                login_id = message_data.get("login")

                if not login_id:
                    logging.warning(f"Message received on topic {msg.topic} without login_id.")
                    continue

                db_name = f"db_{login_id}"
                db = mongo_client[db_name]

                if msg.topic == TOPIC_ACCOUNT_INFO:
                    account_data = message_data.get("data")
                    db_central = mongo_client["my-app"]
                    db_central["account_info"].update_one({"_id": login_id}, {"$set": account_data}, upsert=True)
                    logging.info(f"Processed account_info for login {login_id}")

                elif msg.topic == TOPIC_SYMBOLS_SYNC:
                    symbols_to_sync = message_data.get('symbols', [])
                    collection = db["symbols"]
                    operations = [
                        UpdateOne({"_id": s.get('name')}, {"$set": s}, upsert=True)
                        for s in symbols_to_sync if s.get('name')
                    ]
                    if operations:
                        collection.bulk_write(operations)
                    logging.info(f"Processed batch of {len(symbols_to_sync)} symbols for login {login_id}")

                elif msg.topic == TOPIC_RATES_SYNC:
                    symbol_name = message_data.get('symbol')
                    rates_data = message_data.get('data', [])
                    safe_collection_name = symbol_name.replace('.', '_').replace('$', '')
                    collection = db[safe_collection_name]
                    operations = [
                        UpdateOne({"_id": r.get("time")}, {"$set": r}, upsert=True)
                        for r in rates_data if r.get("time")
                    ]
                    if operations:
                        collection.bulk_write(operations)
                    logging.info(f"Processed batch of {len(rates_data)} rates for '{symbol_name}' for login {login_id}")

            except Exception as e:
                logging.error(f"Error processing message from topic {msg.topic}: {e}", exc_info=True)
    finally:
        await consumer.stop()

# --- بخش وب سرور فلسک (برای درخواست‌های Request/Response) ---
app = Flask(__name__)

@app.route('/get_symbols/<int:login_id>', methods=['GET'])
def get_symbols(login_id):
    try:
        db_name = f"db_{login_id}"
        db = mongo_client[db_name]
        symbols_collection = db["symbols"]
        symbols_list = list(symbols_collection.find({}, {"name": 1, "_id": 0}))
        json_response = dumps(symbols_list)
        return Response(json_response, mimetype='application/json'), 200
    except Exception as e:
        logging.error(f"Error in get_symbols for {login_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/search_symbols/<int:login_id>', methods=['GET'])
def search_symbols(login_id):
    search_query = request.args.get('q', '').strip()
    logging.info(f"Received search request for login '{login_id}' with query: '{search_query}'")
    if not search_query:
        return Response(json.dumps([]), mimetype='application/json'), 200
    try:
        db_name = f"db_{login_id}"
        db = mongo_client[db_name]
        symbols_collection = db["symbols"]
        regex_query = re.compile(search_query, re.IGNORECASE)
        matched_symbols = list(symbols_collection.find(
            {"name": {"$regex": regex_query}},
            {"name": 1, "_id": 0}
        ))
        logging.info(f"Found {len(matched_symbols)} symbols matching '{search_query}' for login '{login_id}'")
        json_response = dumps(matched_symbols)
        return Response(json_response, mimetype='application/json'), 200
    except Exception as e:
        logging.error(f"Error in search_symbols for login '{login_id}': {e}")
        return jsonify({"error": "Internal server error"}), 500

def run_flask_app():
    # اجرای فلسک روی یک پورت متفاوت برای جلوگیری از تداخل
    app.run(host='0.0.0.0', port=8080)

# --- اجرای همزمان هر دو بخش ---
if __name__ == '__main__':
    if not MONGO_URI or not KAFKA_BOOTSTRAP_SERVERS:
        logging.critical("FATAL: MONGO_URI or KAFKA_BOOTSTRAP_SERVERS is not set.")
    else:
        # اجرای فلسک در یک ترد جداگانه
        flask_thread = Thread(target=run_flask_app, daemon=True)
        flask_thread.start()
        logging.info("Flask server started in a background thread on port 8080.")
        
        # اجرای مصرف‌کننده کافکا در ترد اصلی
        try:
            asyncio.run(consume_kafka_messages())
        except KeyboardInterrupt:
            logging.info("Shutting down Kafka consumer...")