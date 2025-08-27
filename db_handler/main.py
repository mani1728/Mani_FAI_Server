# ==================================================================
# File: db_handler/main.py
# Description: کد کامل و اصلاح شده.
# تغییر اصلی: استفاده از Response برای تضمین Content-Type صحیح.
# ==================================================================
import os
import re
# 1. Response را از فلسک import می‌کنیم
from flask import Flask, jsonify, request, Response
from pymongo import MongoClient
from bson.json_util import dumps
import logging

# تنظیمات اولیه لاگ
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ساخت نمونه اپلیکیشن فلسک
app = Flask(__name__)

# --- اتصال به MongoDB ---
MONGO_URI = os.environ.get("MONGO_URI")
if not MONO_URI:
    logging.critical("FATAL: MONGO_URI environment variable not set!")
    raise ValueError("MONGO_URI is not set in the environment")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30000)
logging.info("Successfully connected to MongoDB.")


@app.route('/get_symbols/<int:login_id>', methods=['GET'])
def get_symbols(login_id):
    """
    لیست نام تمام نمادها را از دیتابیس مخصوص کاربر برمی‌گرداند.
    """
    logging.info(f"Received request to get symbols for login_id: {login_id}")
    try:
        db_name = f"db_{login_id}"
        db = client[db_name]
        symbols_collection = db["symbols"]
        
        symbols_list = list(symbols_collection.find({}, {"name": 1, "_id": 0}))
        
        if not symbols_list:
            logging.warning(f"No symbols found for login_id: {login_id}")
            # حتی برای لیست خالی هم پاسخ JSON صحیح برمی‌گردانیم
            return Response(json.dumps([]), mimetype='application/json'), 200
            
        logging.info(f"Successfully retrieved {len(symbols_list)} symbols for login_id: {login_id}")
        
        # 2. FIX: پاسخ را در یک Response object با mimetype صحیح قرار می‌دهیم
        json_response = dumps(symbols_list)
        return Response(json_response, mimetype='application/json'), 200

    except Exception as e:
        logging.error(f"Error in get_symbols for {login_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/search_symbols/<int:login_id>', methods=['GET'])
def search_symbols(search_query):
    """
    نمادها را در دیتابیس کاربر به صورت case-insensitive جستجو می‌کند.
    """
    search_query = request.args.get('q', '').strip()
    logging.info(f"Received search request for login '{login_id}' with query: '{search_query}'")

    if not search_query:
        return Response(json.dumps([]), mimetype='application/json'), 200

    try:
        db_name = f"db_{login_id}"
        db = client[db_name]
        symbols_collection = db["symbols"]

        regex_query = re.compile(search_query, re.IGNORECASE)
        
        matched_symbols = list(symbols_collection.find(
            {"name": {"$regex": regex_query}},
            {"name": 1, "_id": 0}
        ))
        
        logging.info(f"Found {len(matched_symbols)} symbols matching '{search_query}' for login '{login_id}'")
        
        # 3. FIX: این مسیر هم باید mimetype صحیح را برگرداند
        json_response = dumps(matched_symbols)
        return Response(json_response, mimetype='application/json'), 200

    except Exception as e:
        logging.error(f"Error in search_symbols for login '{login_id}': {e}")
        return jsonify({"error": "Internal server error"}), 500


# توابع update_account_info و sync_symbols بدون تغییر باقی می‌مانند
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
