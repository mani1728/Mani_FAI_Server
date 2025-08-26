# main.py

import os
import re
from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson.json_util import dumps
import logging

# تنظیمات اولیه لاگ
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ساخت نمونه اپلیکیشن فلسک
app = Flask(__name__)

# --- اتصال به MongoDB ---
MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    logging.critical("FATAL: MONGO_URI environment variable not set!")
    # در محیط واقعی بهتر است برنامه در اینجا متوقف شود
    raise ValueError("MONGO_URI is not set in the environment")

# افزایش timeout برای جلوگیری از کرش کردن در اتصال اولیه
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30000)
logging.info("Successfully connected to MongoDB.")


@app.route('/update_account_info', methods=['POST'])
def update_account_info():
    """
    اطلاعات حساب را در دیتابیس مرکزی 'my-app' ذخیره یا به‌روزرسانی می‌کند.
    """
    data = request.get_json()
    login_id = data.get('login')
    if not login_id:
        return jsonify({"error": "login field is required"}), 400

    logging.info(f"Received request to update account info for login: {login_id}")
    try:
        db = client["my-app"]
        account_collection = db["account_info"]
        # اگر کاربری با این login_id وجود نداشت، ساخته می‌شود (upsert=True)
        account_collection.update_one({"_id": login_id}, {"$set": data}, upsert=True)
        return jsonify({"status": "success", "message": "Account info updated"}), 200
    except Exception as e:
        logging.error(f"Error updating account info for {login_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/sync_symbols', methods=['POST'])
def sync_symbols():
    """
    برای هر کاربر یک دیتابیس مجزا ساخته (db_<login_id>) و اطلاعات نمادها را در آن ذخیره یا به‌روزرسانی می‌کند.
    """
    data = request.get_json()
    if not data or 'login' not in data or 'symbols' not in data:
        return jsonify({"error": "Invalid payload. 'login' and 'symbols' fields are required."}), 400

    login_id = data.get('login')
    symbols_to_sync = data.get('symbols')
    logging.info(f"Received request to sync {len(symbols_to_sync)} symbols for login: {login_id}")
    try:
        # 1. نام دیتابیس به صورت داینامیک بر اساس login_id ساخته می‌شود
        db_name = f"db_{login_id}"
        db = client[db_name]
        symbols_collection = db["symbols"]
        
        # 2. اگر دیتابیس یا کالکشن وجود نداشته باشد، MongoDB خودکار آن را می‌سازد
        for symbol_data in symbols_to_sync:
            symbol_name = symbol_data.get('name') or symbol_data.get('_id')
            if not symbol_name:
                continue
            # 3. اگر نماد وجود داشت آپدیت، و اگر نداشت ساخته می‌شود (upsert=True)
            symbols_collection.update_one({"_id": symbol_name}, {"$set": symbol_data}, upsert=True)
            
        logging.info(f"Successfully synced symbols for login: {login_id}")
        return jsonify({"status": "success", "message": "Symbols synced successfully"}), 200
    except Exception as e:
        logging.error(f"Error syncing symbols for {login_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500


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
        
        # Projection: فقط فیلد 'name' را برمی‌گرداند برای بهینه‌سازی
        symbols_list = list(symbols_collection.find({}, {"name": 1, "_id": 0}))
        
        if not symbols_list:
            logging.warning(f"No symbols found in database '{db_name}' for login_id: {login_id}")
            return jsonify([]), 200 # برگرداندن لیست خالی بهتر از 404 است
            
        logging.info(f"Successfully retrieved {len(symbols_list)} symbols for login_id: {login_id}")
        return dumps(symbols_list), 200

    except Exception as e:
        logging.error(f"Error in get_symbols for {login_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/search_symbols/<int:login_id>', methods=['GET'])
def search_symbols(login_id):
    """
    نمادها را در دیتابیس کاربر به صورت case-insensitive جستجو می‌کند.
    """
    search_query = request.args.get('q', '').strip()
    logging.info(f"Received search request for login '{login_id}' with query: '{search_query}'")

    if not search_query:
        return jsonify([]), 200

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
        return dumps(matched_symbols), 200

    except Exception as e:
        logging.error(f"Error in search_symbols for login '{login_id}': {e}")
        return jsonify({"error": "Internal server error"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)