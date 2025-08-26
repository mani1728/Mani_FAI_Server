# /home/test/project/proxy/Dockerfile
FROM python:3.9-slim

# مسیر کاری داخل کانتینر
WORKDIR /app

# کپی کردن تمام فایل‌ها و پوشه‌ها (شامل proxy_app) به داخل کانتینر
COPY . .

# نصب کتابخانه‌های مورد نیاز
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# 🎯 اجرای فایل اصلی از داخل زیرپوشه proxy_app
CMD ["python", "proxy_app/main.py"]