# main.py

import os
from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson.json_util import dumps
import logging

# تنظیمات اولیه لاگ
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ساخت نمونه اپلیکیشن فلسک
app = Flask(__name__)

# --- اتصال به MongoDB ---
# بهترین روش، خواندن آدرس اتصال از متغیرهای محیطی (Environment Variables) است.
# این کار امنیت را بالا می‌برد و نیاز به تغییر کد در محیط‌های مختلف را از بین می‌برد.
MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    logging.error("MONGO_URI environment variable not set!")
    # در محیط واقعی بهتر است برنامه در اینجا متوقف شود
    # raise ValueError("MONGO_URI is not set in the environment")

client = MongoClient(MONGO_URI)
logging.info("Successfully connected to MongoDB.")


@app.route('/get_symbols/<int:login_id>', methods=['GET'])
def get_symbols(login_id):
    """
    این مسیر بر اساس login_id به دیتابیس مربوطه متصل شده 
    و لیست تمام نمادها را از کالکشن 'symbols' برمی‌گرداند.
    """
    logging.info(f"Received request to get symbols for login_id: {login_id}")
    try:
        # ساخت نام دیتابیس به صورت داینامیک
        db_name = f"db_{login_id}"
        db = client[db_name]
        
        # دسترسی به کالکشن symbols
        symbols_collection = db["symbols"]
        
        # واکشی تمام اسناد (نمادها) از کالکشن
        # projection (`{"_id": 0}`) برای حذف فیلد پیش‌فرض _id از خروجی است
        symbols_list = list(symbols_collection.find({}, {"_id": 0}))
        
        if not symbols_list:
            logging.warning(f"No symbols found in database '{db_name}' for login_id: {login_id}")
            return jsonify({"error": f"Symbols not found for login_id {login_id}"}), 404
            
        logging.info(f"Successfully retrieved {len(symbols_list)} symbols for login_id: {login_id}")
        
        # استفاده از dumps برای تبدیل صحیح داده‌های MongoDB به JSON
        return dumps(symbols_list), 200

    except Exception as e:
        logging.error(f"Error in get_symbols for {login_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500

# مسیرهای دیگر را نیز می‌توانید به همین شکل با منطق دیتابیس تکمیل کنید
@app.route('/update_account_info', methods=['POST'])
def update_account_info():
    data = request.get_json()
    login_id = data.get('login')
    if not login_id:
        return jsonify({"error": "login field is required"}), 400

    logging.info(f"Received request to update account info for login: {login_id}")
    try:
        # در اینجا به دیتابیس 'my-app' و کالکشن 'account_info' متصل می‌شویم
        db = client["my-app"]
        account_collection = db["account_info"]
        # استفاده از update_one با upsert=True باعث می‌شود اگر رکوردی وجود نداشت، آن را ایجاد کند
        account_collection.update_one({"_id": login_id}, {"$set": data}, upsert=True)
        return jsonify({"status": "success", "message": "Account info updated"}), 200
    except Exception as e:
        logging.error(f"Error updating account info for {login_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)