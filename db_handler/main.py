# ==================================================================
# File: Mani_FAI_Server/db_handler/main.py
# Description: کد کامل و نهایی سرور دی‌بی‌هندلر.
# تغییر اصلی: افزودن مسیر جدید /sync_rates_data برای ذخیره داده‌های کندل.
# ==================================================================
import os
import re
import json
from flask import Flask, jsonify, request, Response
from pymongo import MongoClient, UpdateOne
from bson.json_util import dumps
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    logging.critical("FATAL: MONGO_URI environment variable not set!")
    raise ValueError("MONGO_URI is not set in the environment")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30000)
logging.info("Successfully connected to MongoDB.")


@app.route('/sync_rates_data', methods=['POST'])
def sync_rates_data():
    """
    داده‌های کندل (rates) یک نماد خاص را دریافت کرده و در کالکشن مربوط به آن نماد ذخیره/آپدیت می‌کند.
    """
    data = request.get_json()
    login_id = data.get('login')
    symbol_name = data.get('symbol')
    rates_data = data.get('data')

    if not all([login_id, symbol_name, rates_data is not None]):
        return jsonify({"error": "Missing login, symbol, or data field"}), 400

    logging.info(f"Received request to sync {len(rates_data)} rates for symbol '{symbol_name}' for login {login_id}")
    
    try:
        db_name = f"db_{login_id}"
        db = client[db_name]
        # نام کالکشن برابر با نام نماد خواهد بود (مثلاً 'EURUSD_ecn')
        # کاراکترهای نامعتبر در نام کالکشن را با آندرلاین جایگزین می‌کنیم
        safe_collection_name = symbol_name.replace('.', '_').replace('$', '')
        collection = db[safe_collection_name]

        # استفاده از bulk_write برای افزایش چشمگیر سرعت درج/آپدیت داده‌ها
        operations = []
        for rate in rates_data:
            # از زمان (time) به عنوان شناسه یکتا (_id) برای هر کندل استفاده می‌کنیم
            operation = UpdateOne(
                {"_id": rate.get("time")},
                {"$set": rate},
                upsert=True
            )
            operations.append(operation)
        
        if operations:
            result = collection.bulk_write(operations)
            logging.info(f"Bulk write result for '{symbol_name}': "
                         f"Inserted: {result.inserted_count}, "
                         f"Modified: {result.modified_count}, "
                         f"Upserted: {result.upserted_count}")

        logging.info(f"Successfully synced rates for '{symbol_name}' for login {login_id}")
        return jsonify({"status": "success"}), 200

    except Exception as e:
        logging.error(f"Error in sync_rates_data for {login_id}: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500


# ... (بقیه مسیرهای شما بدون تغییر باقی می‌مانند)
@app.route('/get_symbols/<int:login_id>', methods=['GET'])
def get_symbols(login_id):
    logging.info(f"Received request to get symbols for login_id: {login_id}")
    try:
        db_name = f"db_{login_id}"
        db = client[db_name]
        symbols_collection = db["symbols"]
        symbols_list = list(symbols_collection.find({}, {"name": 1, "_id": 0}))
        if not symbols_list:
            logging.warning(f"No symbols found for login_id: {login_id}")
            return Response(json.dumps([]), mimetype='application/json'), 200
        logging.info(f"Successfully retrieved {len(symbols_list)} symbols for login_id: {login_id}")
        json_response = dumps(symbols_list)
        return Response(json_response, mimetype='application/json'), 200
    except Exception as e:
        logging.error(f"Error in get_symbols for {login_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/update_account_info', methods=['POST'])
def update_account_info():
    data = request.get_json()
    login_id = data.get('login')
    if not login_id: return jsonify({"error": "login field is required"}), 400
    logging.info(f"Received request to update account info for login: {login_id}")
    try:
        db = client["my-app"]
        account_collection = db["account_info"]
        account_collection.update_one({"_id": login_id}, {"$set": data}, upsert=True)
        return jsonify({"status": "success", "message": "Account info updated"}), 200
    except Exception as e:
        logging.error(f"Error updating account info for {login_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/sync_symbols', methods=['POST'])
def sync_symbols():
    data = request.get_json()
    if not data or 'login' not in data or 'symbols' not in data:
        return jsonify({"error": "Invalid payload."}), 400
    login_id = data.get('login')
    symbols_to_sync = data.get('symbols')
    logging.info(f"Received request to sync {len(symbols_to_sync)} symbols for login: {login_id}")
    try:
        db_name = f"db_{login_id}"
        db = client[db_name]
        symbols_collection = db["symbols"]
        for symbol_data in symbols_to_sync:
            symbol_name = symbol_data.get('name') or symbol_data.get('_id')
            if not symbol_name: continue
            symbols_collection.update_one({"_id": symbol_name}, {"$set": symbol_data}, upsert=True)
        logging.info(f"Successfully synced symbols for login: {login_id}")
        return jsonify({"status": "success", "message": "Symbols synced successfully"}), 200
    except Exception as e:
        logging.error(f"Error syncing symbols for {login_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)