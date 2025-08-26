# main.py

import os
import re # کتابخانه Regular Expression برای جستجوی پیشرفته
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
    logging.error("MONGO_URI environment variable not set!")

# ۳۰ ثانیه برای انتخاب سرور و اتصال اولیه صبر می‌کند
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30000)
logging.info("Successfully connected to MongoDB.")


# ### مسیر جدید برای جستجوی نمادها ###
@app.route('/search_symbols/<int:login_id>', methods=['GET'])
def search_symbols(login_id):
    """
    این مسیر یک عبارت جستجو را به عنوان query parameter دریافت کرده
    و در کالکشن symbols کاربر به دنبال نمادهای منطبق می‌گردد.
    مثال درخواست: /search_symbols/633447?q=aud
    """
    # دریافت عبارت جستجو از query string (بعد از علامت ؟ در URL)
    search_query = request.args.get('q', '').strip()
    logging.info(f"Received search request for login '{login_id}' with query: '{search_query}'")

    # اگر عبارت جستجو خالی بود، لیست خالی برگردان
    if not search_query:
        return jsonify([]), 200

    try:
        db_name = f"db_{login_id}"
        db = client[db_name]
        symbols_collection = db["symbols"]

        # ساخت کوئری جستجو با استفاده از Regular Expression برای پشتیبانی از
        # جستجوی case-insensitive و partial match
        # 'i' -> case-insensitive
        regex_query = re.compile(search_query, re.IGNORECASE)
        
        # اجرای کوئری روی فیلد 'name'
        # projection (`{"name": 1, "_id": 0}`) باعث می‌شود فقط فیلد name در خروجی باشد
        matched_symbols = list(symbols_collection.find(
            {"name": {"$regex": regex_query}},
            {"name": 1, "_id": 0}
        ))
        
        logging.info(f"Found {len(matched_symbols)} symbols matching '{search_query}' for login '{login_id}'")
        return dumps(matched_symbols), 200

    except Exception as e:
        logging.error(f"Error in search_symbols for login '{login_id}': {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/get_symbols/<int:login_id>', methods=['GET'])
def get_symbols(login_id):
    """
    این مسیر لیست تمام نمادها را از کالکشن 'symbols' برمی‌گرداند.
    """
    logging.info(f"Received request to get symbols for login_id: {login_id}")
    try:
        db_name = f"db_{login_id}"
        db = client[db_name]
        symbols_collection = db["symbols"]
        symbols_list = list(symbols_collection.find({}, {"_id": 0}))
        
        if not symbols_list:
            logging.warning(f"No symbols found in database '{db_name}' for login_id: {login_id}")
            return jsonify({"error": f"Symbols not found for login_id {login_id}"}), 404
            
        logging.info(f"Successfully retrieved {len(symbols_list)} symbols for login_id: {login_id}")
        return dumps(symbols_list), 200

    except Exception as e:
        logging.error(f"Error in get_symbols for {login_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/update_account_info', methods=['POST'])
def update_account_info():
    """
    این مسیر اطلاعات حساب را در دیتابیس 'my-app' ذخیره می‌کند.
    """
    data = request.get_json()
    login_id = data.get('login')
    if not login_id:
        return jsonify({"error": "login field is required"}), 400

    logging.info(f"Received request to update account info for login: {login_id}")
    try:
        db = client["my-app"]
        account_collection = db["account_info"]
        account_collection.update_one({"_id": login_id}, {"$set": data}, upsert=True)
        return jsonify({"status": "success", "message": "Account info updated"}), 200
    except Exception as e:
        logging.error(f"Error updating account info for {login_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

# تابع sync_symbols شما نیز برای تکمیل منطق باید اینجا باشد
@app.route('/sync_symbols', methods=['POST'])
def sync_symbols():
    data = request.get_json()
    if not data or 'login' not in data or 'symbols' not in data:
        return jsonify({"error": "Invalid payload. 'login' and 'symbols' fields are required."}), 400

    login_id = data.get('login')
    symbols_to_sync = data.get('symbols')
    logging.info(f"Received request to sync {len(symbols_to_sync)} symbols for login: {login_id}")
    try:
        db_name = f"db_{login_id}"
        db = client[db_name]
        symbols_collection = db["symbols"]
        for symbol_data in symbols_to_sync:
            symbol_name = symbol_data.get('name') or symbol_data.get('_id')
            if not symbol_name:
                continue
            symbols_collection.update_one({"_id": symbol_name}, {"$set": symbol_data}, upsert=True)
        logging.info(f"Successfully synced symbols for login: {login_id}")
        return jsonify({"status": "success", "message": "Symbols synced successfully"}), 200
    except Exception as e:
        logging.error(f"Error syncing symbols for {login_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)