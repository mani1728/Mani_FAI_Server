# /home/test/project/db_handler/main.py
from flask import Flask, request, jsonify
from pymongo import MongoClient
from datetime import datetime
import os # <-- ????? os ?? ???? ?????? ???????? ????? ????? ???????

app = Flask(__name__)

# ?? ???? ??????? ?? ????? ????? ?? ??? MONGO_URI ?????? ??????
# ??? ??? ????? ???? ?????? ?? ?? ???? ??????? ???? ??? ??????? ??????
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:secret@mongodb:27017/")
client = MongoClient(MONGO_URI)

@app.route('/update_account_info', methods=['POST'])
def update_account_info():
    try:
        data = request.get_json()
        login_id = data['login']
        # ?? ????: ??? ??????? ??? 'my-app' ???
        db = client['my-app']
        collection = db.account_info
        collection.update_one({"_id": login_id}, {"$set": data}, upsert=True)
        print(f"Upserted account info for _id: {login_id}")
        return jsonify({"status": "success"}), 200
    except Exception as e:
        print(f"Error in update_account_info: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/sync_symbols', methods=['POST'])
def sync_symbols():
    try:
        payload = request.get_json()
        login_number = payload['login']
        db_name = f"db_{login_number}"
        db = client[db_name]
        collection = db.symbols
        
        upsert_count = 0
        for symbol_data in payload.get('data', []):
            symbol_name = symbol_data.get('name')
            if symbol_name:
                collection.update_one({"_id": symbol_name}, {"$set": symbol_data}, upsert=True)
                upsert_count += 1
        
        print(f"Upserted {upsert_count} symbols for login {login_number}")
        return jsonify({"status": "success"}), 200
    except Exception as e:
        print(f"Error in sync_symbols: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)