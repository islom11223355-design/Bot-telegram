import logging
import json
import os
from datetime import datetime
import re
import asyncio
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.request import HTTPXRequest
from telegram.error import TimedOut, NetworkError, Conflict
import uuid

# Logging sozlamalari
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variable validation
def validate_env_vars():
    required_vars = ["GOOGLE_SHEETS_CREDS", "SHEET_ID", "BOT_TOKEN", "ADMIN_IDS"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

validate_env_vars()

# Google Sheets sozlamalari
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
try:
    CREDS_JSON = json.loads(os.getenv("GOOGLE_SHEETS_CREDS"))
    CREDS = ServiceAccountCredentials.from_json_keyfile_dict(CREDS_JSON, SCOPE)
    CLIENT = gspread.authorize(CREDS)
    SHEET = CLIENT.open_by_key(os.getenv("SHEET_ID"))
    HARIDORLAR_SHEET = SHEET.worksheet("Haridorlar")
    MAHSULOTLAR_SHEET = SHEET.worksheet("Mahsulotlar")
    BUYURTMALAR_SHEET = SHEET.worksheet("Buyurtmalar")
    GURUHLAR_SHEET = SHEET.worksheet("Guruhlar")
except Exception as e:
    logger.error(f"Google Sheets initialization error: {e}")
    raise

# Bot sozlamalari
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMINS = os.getenv("ADMIN_IDS").split(",")  # Comma-separated admin IDs from env

# Foydalanuvchi holatlari va boshqa sozlamalar
USER_STATE = {}
CART = {}
BONUS_REQUESTS = {}
USER_SELECTED_GROUP = {}
USER_CACHE = {}
PRODUCT_CACHE = {}
GROUP_CACHE = None

def format_currency(amount):
    """Narxni 40 000 so'm ko'rinishida formatlash"""
    try:
        return f"{int(float(amount)):,d} so'm".replace(",", " ")
    except (ValueError, TypeError):
        logger.error(f"Invalid amount for formatting: {amount}")
        return "0 so'm"

def init_sheets():
    """Google Sheets sahifalarini boshlash va sarlavhalarni kiritish"""
    try:
        if not HARIDORLAR_SHEET.row_values(1):
            HARIDORLAR_SHEET.append_row(["ID", "Ism", "Telefon", "Manzil", "Faoliyat turi", "Bonus", "Tahrir So‘rovi", "Tahrir Tasdiqlangan"])
        if not MAHSULOTLAR_SHEET.row_values(1):
            MAHSULOTLAR_SHEET.append_row(["Guruh nomi", "Mahsulot nomi", "Narx", "Bonus foizi", "Miqdori"])
        if not BUYURTMALAR_SHEET.row_values(1):
            BUYURTMALAR_SHEET.append_row(["Haridor ID", "Buyurtmachi ismi", "Telefon", "Manzil", "Sana", "Guruh nomi", "Mahsulotlar", "Umumiy summa", "Bonus summasi", "Confirmed"])
        if not GURUHLAR_SHEET.row_values(1):
            GURUHLAR_SHEET.append_row(["Guruh Nomi"])
    except Exception as e:
        logger.error(f"Sheets init xatosi: {e}")
        raise

def save_user_data(user_id, data):
    """Foydalanuvchi ma'lumotlarini Google Sheets'ga saqlash"""
    try:
        if not all(key in data for key in ["name", "phone", "address", "role"]):
            logger.error(f"Missing required user data fields: {data}")
            return
        HARIDORLAR_SHEET.append_row([
            str(user_id),
            data["name"],
            data["phone"],
            data["address"],
            data["role"],
            data.get("bonus", 0),
            "",
            ""
        ])
        USER_CACHE[user_id] = data
        logger.info(f"Haridor saqlandi: ID={user_id}, Bonus={data.get('bonus', 0)}")
    except Exception as e:
        logger.error(f"Haridor saqlash xatosi: {e}")

def update_user_data(user_id, data, edit_request=False):
    """Foydalanuvchi ma'lumotlarini yangilash"""
    try:
        records = HARIDORLAR_SHEET.get_all_records()
        for i, record in enumerate(records, start=2):
            if str(record["ID"]) == str(user_id):
                values = [
                    str(user_id),
                    data["name"],
                    data["phone"],
                    data["address"],
                    data["role"],
                    data.get("bonus", 0),
                    data.get("edit_request", ""),
                    data.get("edit_confirmed", "")
                ]
                HARIDORLAR_SHEET.update(f"A{i}:H{i}", [values])
                USER_CACHE[user_id] = data
                logger.info(f"Haridor yangilandi: ID={user_id}, Bonus={data.get('bonus', 0)}")
                return True
        if edit_request:
            save_user_data(user_id, data)
        logger.error(f"Haridor topilmadi: ID={user_id}")
        return False
    except Exception as e:
        logger.error(f"Haridor yangilash xatosi: {e}")
        return False

def get_user_data(user_id):
    """Foydalanuvchi ma'lumotlarini olish (kesh bilan)"""
    try:
        if user_id in USER_CACHE:
            return USER_CACHE[user_id]
        records = HARIDORLAR_SHEET.get_all_records()
        for record in records:
            if str(record["ID"]) == str(user_id):
                user_data = {
                    "id": str(record["ID"]),
                    "name": record["Ism"],
                    "phone": record["Telefon"],
                    "address": record["Manzil"],
                    "role": record["Faoliyat turi"],
                    "bonus": float(record["Bonus"] or 0),
                    "edit_request": record["Tahrir So‘rovi"],
                    "edit_confirmed": record["Tahrir Tasdiqlangan"]
                }
                USER_CACHE[user_id] = user_data
                return user_data
        return None
    except Exception as e:
        logger.error(f"Haridor ma'lumotlarini olish xatosi: {e}")
        return None

def save_product(data):
    """Mahsulot ma'lumotlarini Google Sheets'ga saqlash"""
    try:
        if not all(key in data for key in ["group_name", "name", "price", "bonus_percent"]):
            logger.error(f"Missing required product data fields: {data}")
            return
        MAHSULOTLAR_SHEET.append_row([
            data["group_name"],
            data["name"],
            data["price"],
            data["bonus_percent"],
            data.get("quantity", 0)
        ])
        cache_key = data["group_name"] or "all"
        PRODUCT_CACHE.pop(cache_key, None)
        logger.info(f"Mahsulot qo'shildi: {data['name']} ({data['group_name']})")
    except Exception as e:
        logger.error(f"Mahsulot saqlash xatosi: {e}")

def update_product(old_name, group_name, data):
    """Mahsulot ma'lumotlarini yangilash"""
    try:
        records = MAHSULOTLAR_SHEET.get_all_records()
        for i, record in enumerate(records, start=2):
            if record["Mahsulot nomi"] == old_name and record["Guruh nomi"] == group_name:
                MAHSULOTLAR_SHEET.update(f"A{i}:E{i}", [[
                    data["group_name"],
                    data["name"],
                    data["price"],
                    data["bonus_percent"],
                    data.get("quantity", 0)
                ]])
                PRODUCT_CACHE.pop(group_name, None)
                logger.info(f"Mahsulot yangilandi: {old_name} -> {data['name']} ({data['group_name']})")
                return True
        logger.error(f"Mahsulot topilmadi: {old_name} ({group_name})")
        return False
    except Exception as e:
        logger.error(f"Mahsulot yangilash xatosi: {e}")
        return False

def get_products(group_name=None):
    """Mahsulotlar ro'yxatini olish (kesh bilan)"""
    try:
        cache_key = group_name or "all"
        if cache_key in PRODUCT_CACHE:
            return PRODUCT_CACHE[cache_key]
        records = MAHSULOTLAR_SHEET.get_all_records()
        products = []
        for record in records:
            if group_name is None or record["Guruh nomi"] == group_name:
                products.append({
                    "group_name": record["Guruh nomi"],
                    "name": record["Mahsulot nomi"],
                    "price": float(record["Narx"]),
                    "bonus_percent": float(record["Bonus foizi"]),
                    "quantity": float(record["Miqdori"] or 0)
                })
        PRODUCT_CACHE[cache_key] = products
        return products
    except Exception as e:
        logger.error(f"Mahsulotlar olish xatosi: {e}")
        return []

def get_groups():
    """Guruhlar ro'yxatini olish (Guruhlar varag'idan)"""
    global GROUP_CACHE
    try:
        if GROUP_CACHE is not None:
            return GROUP_CACHE
        records = GURUHLAR_SHEET.get_all_records()
        GROUP_CACHE = list(set(record["Guruh Nomi"] for record in records if record["Guruh Nomi"]))
        return GROUP_CACHE
    except Exception as e:
        logger.error(f"Guruhlar olish xatosi: {e}")
        return []

def save_group(group_name):
    """Yangi guruh qo'shish"""
    try:
        if not group_name.strip():
            logger.error("Empty group name provided")
            return
        GURUHLAR_SHEET.append_row([group_name])
        global GROUP_CACHE
        GROUP_CACHE = None
        logger.info(f"Yangi guruh qo'shildi: {group_name}")
    except Exception as e:
        logger.error(f"Guruh qo'shish xatosi: {e}")

def save_order(user_id, cart, address, group_name):
    """Buyurtmani Google Sheets'ga saqlash"""
    try:
        user_data = get_user_data(user_id)
        if not user_data:
            logger.error(f"Haridor topilmadi: ID={user_id}")
            return None
        total_sum = sum(item["price"] * item["quantity"] for item in cart)
        total_bonus = sum(item["price"] * item["quantity"] * (item["bonus_percent"] / 100) for item in cart) if user_data["role"] == "Usta" else 0
        cart_text = "\n".join([f"{item['name']} - {item['quantity']} dona, narxi: {format_currency(item['price'])}, jami: {format_currency(item['price'] * item['quantity'])}" for item in cart])
        
        BUYURTMALAR_SHEET.append_row([
            str(user_id),
            user_data["name"],
            user_data["phone"],
            address,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            group_name,
            cart_text,
            total_sum,
            total_bonus,
            "Yes"
        ])
        if total_bonus > 0:
            update_bonus(user_id, total_bonus)
        logger.info(f"Buyurtma saqlandi: ID={user_id}, Guruh={group_name}, Bonus={total_bonus}")
        return BUYURTMALAR_SHEET.row_count
    except Exception as e:
        logger.error(f"Buyurtma saqlash xatosi: {e}")
        return None

def update_bonus(user_id, bonus_amount):
    """Haridorning bonusini yangilash"""
    try:
        records = HARIDORLAR_SHEET.get_all_records()
        for i, record in enumerate(records, start=2):
            if str(record["ID"]) == str(user_id):
                current_bonus = float(record["Bonus"] or 0)
                new_bonus = current_bonus + bonus_amount
                HARIDORLAR_SHEET.update(f"F{i}", new_bonus)
                USER_CACHE[user_id]["bonus"] = new_bonus
                logger.info(f"Bonus yangilandi: ID={user_id}, Qo'shilgan={bonus_amount}, Umumiy={new_bonus}")
                return True
        logger.error(f"Haridor topilmadi bonus yangilashda: ID={user_id}")
        return False
    except Exception as e:
        logger.error(f"Bonus yangilash xatosi: {e}")
        return False

def get_orders_by_date(date):
    """Sanadagi buyurtmalarni olish"""
    try:
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
            logger.error(f"Invalid date format: {date}")
            return []
        records = BUYURTMALAR_SHEET.get_all_records()
        orders = []
        for i, record in enumerate(records, start=2):
            order_date = record["Sana"].split()[0]
            if order_date == date:
                orders.append({
                    "row": i,
                    "user_id": str(record["Haridor ID"]),
                    "user_name": record["Buyurtmachi ismi"],
                    "phone": record["Telefon"],
                    "address": record["Manzil"],
                    "date": record["Sana"],
                    "group_name": record["Guruh nomi"],
                    "cart_text": record["Mahsulotlar"],
                    "total_sum": float(record["Umumiy summa"]),
                    "bonus_sum": float(record["Bonus summasi"] or 0),
                    "confirmed": record["Confirmed"]
                })
        return orders
    except Exception as e:
        logger.error(f"Buyurtmalar olish xatosi: {e}")
        return []

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Botni boshlash"""
    user_id = str(update.effective_user.id)
    if user_id in ADMINS:
        keyboard = [
            ["Yangi guruh qo'shish", "Mahsulot qo'shish"],
            ["Mahsulotlar ma'lumotlarini o'zgartirish", "Mahsulot ro'yxati"],
            ["Buyurtmalar ro'yxati", "Haridorlar ro'yxati"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text("Xush kelibsiz, Admin! Quyidagi amallarni bajarishingiz mumkin:", reply_markup=reply_markup)
    else:
        user_data = get_user_data(user_id)
        if not user_data:
            keyboard = [[KeyboardButton("Ma'lumotlaringizni saqlang")]]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text("Iltimos, ma'lumotlaringizni saqlang.", reply_markup=reply_markup)
        else:
            keyboard = [
                ["Shaxsiy ma'lumotlarni o'zgartirish", "Mahsulot buyurtma qilish"],
                ["Mening buyurtmalarim"]
            ]
            if user_data["role"] == "Usta":
                keyboard.append(["Umumiy Bonus", "Bonusni yechish"])
            keyboard.append(["Admin bilan bog'lanish"])
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(f"Xush kelibsiz, {user_data['name']}!", reply_markup=reply_markup)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Foydalanuvchi xabarlarini qayta ishlash"""
    user_id = str(update.effective_user.id)
    text = update.message.text.strip()
    logger.info(f"User {user_id} xabari: {text}")

    try:
        if user_id in ADMINS:
            await handle_admin(update, context)
            return

        user_data = get_user_data(user_id)
        if text == "Ma'lumotlaringizni saqlang" and not user_data:
            USER_STATE[user_id] = {"step": "name"}
            await update.message.reply_text("Ismingizni kiriting:")
        elif text == "Shaxsiy ma'lumotlarni o'zgartirish" and user_data:
            USER_STATE[user_id] = {"step": "edit_name", "bonus": user_data["bonus"]}
            await update.message.reply_text(f"Joriy ism: {user_data['name']}\nYangi ismingizni kiriting:")
        elif text == "Mahsulot buyurtma qilish":
            CART[user_id] = []
            groups = get_groups()
            if not groups:
                await update.message.reply_text("Hozirda guruhlar mavjud emas.")
                return
            keyboard = [[InlineKeyboardButton(group, callback_data=f"group_{group}")] for group in groups]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("Mahsulot buyurtma qilish uchun guruhni tanlang:", reply_markup=reply_markup)
        elif text == "Mening buyurtmalarim":
            records = BUYURTMALAR_SHEET.get_all_records()
            orders = []
            for record in records:
                if str(record["Haridor ID"]) == user_id:
                    orders.append(
                        f"Sana: {record['Sana']}\n"
                        f"Guruh: {record['Guruh nomi']}\n"
                        f"Mahsulotlar:\n{record['Mahsulotlar']}\n"
                        f"Umumiy summa: {format_currency(record['Umumiy summa'])}\n"
                        f"Holat: {'Tasdiqlangan' if record['Confirmed'] == 'Yes' else 'Rad etildi' if record['Confirmed'] == 'Rejected' else 'Tasdiqlanmagan'}"
                    )
            if orders:
                await update.message.reply_text("\n\n".join(orders))
            else:
                await update.message.reply_text("Sizda buyurtmalar yo'q.")
        elif text == "Umumiy Bonus" and user_data and user_data["role"] == "Usta":
            await update.message.reply_text(f"Sizning umumiy bonusingiz: {format_currency(user_data['bonus'])}")
        elif text == "Bonusni yechish" and user_data and user_data["role"] == "Usta":
            if user_data["bonus"] <= 0:
                await update.message.reply_text("Sizda yechish uchun bonus mavjud emas.")
                return
            BONUS_REQUESTS[user_id] = user_data["bonus"]
            await context.bot.send_message(
                chat_id=ADMINS[0],
                text=f"Foydalanuvchi {user_id} ({user_data['name']}) {format_currency(user_data['bonus'])} bonusni yechmoqchi. Tasdiqlaysizmi?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Tasdiqlash", callback_data=f"approve_bonus_{user_id}"),
                     InlineKeyboardButton("Rad etish", callback_data=f"reject_bonus_{user_id}")]
                ])
            )
            await update.message.reply_text("Bonusni yechish so'rovi adminga yuborildi.")
        elif text == "Admin bilan bog'lanish":
            await update.message.reply_text(f"Admin bilan bog'lanish uchun: [{ADMINS[0]}](tg://user?id={ADMINS[0]})", parse_mode="Markdown")
        elif user_id in USER_STATE:
            state = USER_STATE[user_id]
            if state["step"] == "name":
                if not text.strip():
                    await update.message.reply_text("Iltimos, ismingizni kiriting (bo'sh bo'lmasligi kerak).")
                    return
                USER_STATE[user_id]["name"] = text.strip()
                USER_STATE[user_id]["step"] = "phone"
                await update.message.reply_text("Telefon raqamingizni kiriting (+998XXXXXXXXX):")
            elif state["step"] == "phone":
                if re.match(r"^\+998\d{9}$", text):
                    USER_STATE[user_id]["phone"] = text
                    USER_STATE[user_id]["step"] = "location"
                    keyboard = [[KeyboardButton("Lokatsiyani yuborish", request_location=True)]]
                    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                    await update.message.reply_text("Lokatsiyangizni yuboring:", reply_markup=reply_markup)
                else:
                    await update.message.reply_text("Iltimos, to'g'ri telefon raqamini kiriting (+998XXXXXXXXX):")
            elif state["step"] == "role":
                if text in ["Do'kon egasi", "Qurilish kompaniyasi", "Uy egasi", "Usta"]:
                    USER_STATE[user_id]["role"] = text
                    data = {
                        "name": USER_STATE[user_id]["name"],
                        "phone": USER_STATE[user_id]["phone"],
                        "address": USER_STATE[user_id]["address"],
                        "role": text,
                        "bonus": 0
                    }
                    save_user_data(user_id, data)
                    del USER_STATE[user_id]
                    keyboard = [
                        ["Shaxsiy ma'lumotlarni o'zgartirish", "Mahsulot buyurtma qilish"],
                        ["Mening buyurtmalarim"]
                    ]
                    if text == "Usta":
                        keyboard.append(["Umumiy Bonus", "Bonusni yechish"])
                    keyboard.append(["Admin bilan bog'lanish"])
                    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                    await update.message.reply_text("Ma'lumotlaringiz saqlandi!", reply_markup=reply_markup)
                else:
                    await update.message.reply_text("Iltimos, quyidagi variantlardan birini tanlang: Do'kon egasi, Qurilish kompaniyasi, Uy egasi, Usta")
            elif state["step"] == "quantity":
                try:
                    quantity = int(text)
                    if quantity <= 0:
                        await update.message.reply_text("Iltimos, 0 dan katta miqdor kiriting.")
                        return
                    product_name = USER_STATE[user_id]["product_name"]
                    group_name = USER_SELECTED_GROUP.get(user_id, "")
                    products = get_products(group_name)
                    product = next((p for p in products if p["name"] == product_name), None)
                    if product:
                        CART[user_id].append({"name": product_name, "quantity": quantity, "price": product["price"], "bonus_percent": product["bonus_percent"]})
                        keyboard = [[InlineKeyboardButton(f"{p['name']} ({format_currency(p['price'])})", callback_data=f"product_{p['name']}")] for p in products]
                        keyboard.append([InlineKeyboardButton("Savatni tasdiqlash", callback_data="confirm_cart")])
                        reply_markup = InlineKeyboardMarkup(keyboard)
                        await update.message.reply_text(f"{product_name} ({quantity} dona) savatga qo'shildi. Yana mahsulot qo'shasizmi yoki savatni tasdiqlaysizmi?", reply_markup=reply_markup)
                    else:
                        await update.message.reply_text("Mahsulot topilmadi. Iltimos, qaytadan urinib ko'ring.")
                    del USER_STATE[user_id]
                except ValueError:
                    await update.message.reply_text("Iltimos, to'g'ri miqdor kiriting (butun son).")
            elif state["step"] == "order_location":
                await update.message.reply_text("Iltimos, buyurtma yetkazib beriladigan lokatsiyani yuboring:", reply_markup=ReplyKeyboardMarkup([[KeyboardButton("Lokatsiyani yuborish", request_location=True)]], resize_keyboard=True))
            elif state["step"] == "edit_name":
                if not text.strip():
                    await update.message.reply_text("Iltimos, ismingizni kiriting (bo'sh bo'lmasligi kerak).")
                    return
                USER_STATE[user_id]["name"] = text.strip()
                USER_STATE[user_id]["step"] = "edit_phone"
                await update.message.reply_text("Telefon raqamingizni kiriting (+998XXXXXXXXX):")
            elif state["step"] == "edit_phone":
                if re.match(r"^\+998\d{9}$", text):
                    USER_STATE[user_id]["phone"] = text
                    USER_STATE[user_id]["step"] = "edit_location"
                    keyboard = [[KeyboardButton("Lokatsiyani yuborish", request_location=True)]]
                    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                    await update.message.reply_text("Lokatsiyangizni yuboring:", reply_markup=reply_markup)
                else:
                    await update.message.reply_text("Iltimos, to'g'ri telefon raqamini kiriting (+998XXXXXXXXX):")
            elif state["step"] == "edit_role":
                if text in ["Do'kon egasi", "Qurilish kompaniyasi", "Uy egasi", "Usta"]:
                    USER_STATE[user_id]["role"] = text
                    data = {
                        "name": USER_STATE[user_id]["name"],
                        "phone": USER_STATE[user_id]["phone"],
                        "address": USER_STATE[user_id]["address"],
                        "role": text,
                        "bonus": USER_STATE[user_id]["bonus"],
                        "edit_request": f"{USER_STATE[user_id]['name']}, {USER_STATE[user_id]['phone']}, {USER_STATE[user_id]['address']}, {text}",
                        "edit_confirmed": "No"
                    }
                    update_user_data(user_id, data, edit_request=True)
                    await context.bot.send_message(
                        chat_id=ADMINS[0],
                        text=f"Foydalanuvchi {user_id} ({user_data['name']}) shaxsiy ma'lumotlarini o'zgartirmoqchi:\n"
                             f"Yangi ma'lumotlar: {data['edit_request']}\nTasdiqlaysizmi?",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("Tasdiqlash", callback_data=f"approve_edit_{user_id}"),
                             InlineKeyboardButton("Rad etish", callback_data=f"reject_edit_{user_id}")]
                        ])
                    )
                    await update.message.reply_text("Ma'lumotlarni o'zgartirish so'rovi adminga yuborildi. Tasdiqlanishini kuting.")
                    del USER_STATE[user_id]
                else:
                    await update.message.reply_text("Iltimos, quyidagi variantlardan birini tanlang: Do'kon egasi, Qurilish kompaniyasi, Uy egasi, Usta")
    except (TimedOut, NetworkError) as e:
        logger.error(f"TimedOut in handle_message: {e}")
        await update.message.reply_text("Tarmoq xatosi yuz berdi, iltimos, keyinroq urinib ko'ring.")
    except Exception as e:
        logger.error(f"Umumiy xato in handle_message: {e}", exc_info=True)
        await update.message.reply_text("Xato yuz berdi, admin bilan bog'laning.")

async def handle_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lokatsiya qabul qilish"""
    user_id = str(update.effective_user.id)
    if user_id in USER_STATE and USER_STATE[user_id]["step"] in ["location", "order_location", "edit_location"]:
        location = update.message.location
        address = f"Lat: {location.latitude}, Lon: {location.longitude}"
        maps_link = f"https://maps.google.com/?q={location.latitude},{location.longitude}"
        if USER_STATE[user_id]["step"] == "location":
            USER_STATE[user_id]["address"] = address
            USER_STATE[user_id]["step"] = "role"
            keyboard = [
                ["Do'kon egasi", "Qurilish kompaniyasi"],
                ["Uy egasi", "Usta"]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text("Faoliyat turini tanlang:", reply_markup=reply_markup)
        elif USER_STATE[user_id]["step"] == "order_location":
            user_data = get_user_data(user_id)
            if not user_data:
                await update.message.reply_text("Xato: Haridor ma'lumotlari topilmadi.")
                return
            group_name = USER_SELECTED_GROUP.get(user_id, "")
            total_sum = sum(item["price"] * item["quantity"] for item in CART[user_id])
            total_bonus = sum(item["price"] * item["quantity"] * (item["bonus_percent"] / 100) for item in CART[user_id]) if user_data["role"] == "Usta" else 0
            cart_text = "\n".join([f"{item['name']} - {item['quantity']} dona, narxi: {format_currency(item['price'])}, jami: {format_currency(item['price'] * item['quantity'])}" for item in CART[user_id]])
            temp_order = {
                "user_id": user_id,
                "user_name": user_data["name"],
                "phone": user_data["phone"],
                "address": address,
                "group_name": group_name,
                "cart_text": cart_text,
                "total_sum": total_sum,
                "bonus_sum": total_bonus,
                "maps_link": maps_link
            }
            ORDER_CACHE[user_id] = temp_order
            bonus_text = f"\nUshbu buyurtma uchun yig'ilgan bonus: {format_currency(total_bonus)}" if user_data["role"] == "Usta" else ""
            await context.bot.send_message(
                chat_id=ADMINS[0],
                text=f"Yangi buyurtma:\nHaridor ID: {user_id}\nHaridor: [{user_data['name']}](tg://user?id={user_id})\nTelefon: {user_data['phone']}\nManzil: [{address}]({maps_link})\nGuruh: {group_name}\nMahsulotlar:\n{cart_text}\nUmumiy summa: {format_currency(total_sum)}{bonus_text}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Tasdiqlash", callback_data=f"confirm_order_{user_id}"),
                     InlineKeyboardButton("Rad etish", callback_data=f"reject_order_{user_id}")]
                ])
            )
            keyboard = [
                ["Shaxsiy ma'lumotlarni o'zgartirish", "Mahsulot buyurtma qilish"],
                ["Mening buyurtmalarim"]
            ]
            if user_data["role"] == "Usta":
                keyboard.append(["Umumiy Bonus", "Bonusni yechish"])
            keyboard.append(["Admin bilan bog'lanish"])
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text("Buyurtmangiz adminga yuborildi. Tasdiqlanishini kuting.", reply_markup=reply_markup)
            del USER_STATE[user_id]
        elif USER_STATE[user_id]["step"] == "edit_location":
            USER_STATE[user_id]["address"] = address
            USER_STATE[user_id]["step"] = "edit_role"
            keyboard = [
                ["Do'kon egasi", "Qurilish kompaniyasi"],
                ["Uy egasi", "Usta"]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text("Faoliyat turini tanlang:", reply_markup=reply_markup)

ORDER_CACHE = {}

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Foydalanuvchi callback so'rovlarini qayta ishlash"""
    query = update.callback_query
    user_id = str(query.from_user.id)
    data = query.data
    logger.info(f"Callback query from {user_id}: {data}")

    try:
        if data.startswith("group_"):
            group_name = data[len("group_"):]
            USER_SELECTED_GROUP[user_id] = group_name
            products = get_products(group_name)
            if not products:
                await query.message.reply_text(f"{group_name} guruhida mahsulotlar yo'q.")
                await query.answer()
                return
            keyboard = [[InlineKeyboardButton(f"{p['name']} ({format_currency(p['price'])})", callback_data=f"product_{p['name']}")] for p in products]
            keyboard.append([InlineKeyboardButton("Savatni tasdiqlash", callback_data="confirm_cart")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text(f"{group_name} guruhidagi mahsulotlar:", reply_markup=reply_markup)
            await query.answer()
        elif data.startswith("product_"):
            product_name = data[len("product_"):]
            USER_STATE[user_id] = {"step": "quantity", "product_name": product_name}
            await query.message.reply_text(f"{product_name} uchun miqdorni kiriting:")
            await query.answer()
        elif data == "confirm_cart":
            if not CART.get(user_id):
                await query.message.reply_text("Savat bo'sh! Iltimos, avval mahsulot qo'shing.")
                await query.answer()
                return
            USER_STATE[user_id] = {"step": "order_location"}
            await query.message.reply_text("Buyurtma yetkazib beriladigan lokatsiyani yuboring:", reply_markup=ReplyKeyboardMarkup([[KeyboardButton("Lokatsiyani yuborish", request_location=True)]], resize_keyboard=True))
            await query.answer()
    except (TimedOut, NetworkError) as e:
        logger.error(f"TimedOut in handle_callback_query: {e}")
        await query.message.reply_text("Tarmoq xatosi yuz berdi, iltimos, keyinroq urinib ko'ring.")
        await query.answer()
    except Exception as e:
        logger.error(f"Unexpected error in handle_callback_query: {e}", exc_info=True)
        await query.message.reply_text("Xato yuz berdi, iltimos, keyinroq urinib ko'ring.")
        await query.answer()

async def handle_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin callback so'rovlarini qayta ishlash"""
    query = update.callback_query
    data = query.data
    user_id = str(query.from_user.id)
    logger.info(f"Admin callback from {user_id}: {data}")

    try:
        if data.startswith("confirm_order_"):
            order_user_id = data[len("confirm_order_"):]
            if order_user_id not in ORDER_CACHE:
                await query.message.reply_text("Xato: Buyurtma topilmadi!")
                logger.error(f"confirm_order: Buyurtma topilmadi: User ID={order_user_id}")
                await query.answer()
                return
            order = ORDER_CACHE[order_user_id]
            order_row = save_order(order["user_id"], CART[order_user_id], order["address"], order["group_name"])
            if order_row is None:
                await query.message.reply_text("Xato: Buyurtma saqlanmadi!")
                logger.error(f"confirm_order: Buyurtma saqlanmadi: User ID={order_user_id}")
                await query.answer()
                return
            user_data = get_user_data(order_user_id)
            if not user_data:
                await query.message.reply_text("Xato: Foydalanuvchi topilmadi!")
                logger.error(f"confirm_order: Haridor topilmadi: ID={order_user_id}")
                await query.answer()
                return
            bonus_text = f"\nUshbu buyurtma uchun yig'ilgan bonus: {format_currency(order['bonus_sum'])}\nUmumiy bonus: {format_currency(user_data['bonus'])}" if user_data["role"] == "Usta" else ""
            await context.bot.send_message(
                chat_id=order_user_id,
                text=f"Sizning buyurtmangiz tasdiqlandi, hamkorligingizdan hursandmiz!\nGuruh: {order['group_name']}\nMahsulotlar:\n{order['cart_text']}\nUmumiy summa: {format_currency(order['total_sum'])}{bonus_text}",
                parse_mode="Markdown"
            )
            await query.message.edit_text(
                text=query.message.text + "\n\n**Holati: Tasdiqlangan**",
                parse_mode="Markdown",
                reply_markup=None
            )
            await query.message.reply_text(f"Buyurtma tasdiqlandi.")
            logger.info(f"Buyurtma tasdiqlandi: User ID={order_user_id}, Bonus={order['bonus_sum']}")
            del ORDER_CACHE[order_user_id]
            del CART[order_user_id]
            del USER_SELECTED_GROUP[order_user_id]
            await query.answer()
        elif data.startswith("reject_order_"):
            order_user_id = data[len("reject_order_"):]
            if order_user_id not in ORDER_CACHE:
                await query.message.reply_text("Xato: Buyurtma topilmadi!")
                logger.error(f"reject_order: Buyurtma topilmadi: User ID={order_user_id}")
                await query.answer()
                return
            order = ORDER_CACHE[order_user_id]
            await context.bot.send_message(
                chat_id=order_user_id,
                text="Sizning buyurtmangiz rad etildi. Qo'shimcha ma'lumot uchun admin bilan bog'laning."
            )
            await query.message.edit_text(
                text=query.message.text + "\n\n**Holati: Rad etildi**",
                parse_mode="Markdown",
                reply_markup=None
            )
            await query.message.reply_text(f"Buyurtma rad etildi.")
            logger.info(f"Buyurtma rad etildi: User ID={order_user_id}")
            del ORDER_CACHE[order_user_id]
            del CART[order_user_id]
            del USER_SELECTED_GROUP[order_user_id]
            await query.answer()
        elif data.startswith("approve_bonus_"):
            user_id = data[len("approve_bonus_"):]
            user_data = get_user_data(user_id)
            if not user_data:
                await query.message.reply_text("Xato: Foydalanuvchi topilmadi!")
                logger.error(f"approve_bonus: Haridor topilmadi: ID={user_id}")
                await query.answer()
                return
            user_data["bonus"] = 0
            if not update_user_data(user_id, user_data):
                await query.message.reply_text("Xato: Bonus yangilanmadi!")
                logger.error(f"approve_bonus: Bonus yangilanmadi: ID={user_id}")
                await query.answer()
                return
            await context.bot.send_message(
                chat_id=user_id,
                text="Sizning bonus yechish so'rovingiz tasdiqlandi. Bonus summangiz 0 ga tenglashtirildi."
            )
            await query.message.reply_text(f"Bonus yechish tasdiqlandi.")
            logger.info(f"Bonus yechish tasdiqlandi: ID={user_id}")
            del BONUS_REQUESTS[user_id]
            await query.answer()
        elif data.startswith("reject_bonus_"):
            user_id = data[len("reject_bonus_"):]
            user_data = get_user_data(user_id)
            if not user_data:
                await query.message.reply_text("Xato: Foydalanuvchi topilmadi!")
                logger.error(f"reject_bonus: Haridor topilmadi: ID={user_id}")
                await query.answer()
                return
            await context.bot.send_message(
                chat_id=user_id,
                text="Sizning bonus yechish so'rovingiz rad etildi. Qo'shimcha ma'lumot uchun admin bilan bog'laning."
            )
            await query.message.reply_text(f"Bonus yechish rad etildi.")
            logger.info(f"Bonus yechish rad etildi: ID={user_id}")
            del BONUS_REQUESTS[user_id]
            await query.answer()
        elif data.startswith("approve_edit_"):
            user_id = data[len("approve_edit_"):]
            user_data = get_user_data(user_id)
            if not user_data:
                await query.message.reply_text("Xato: Foydalanuvchi topilmadi!")
                logger.error(f"approve_edit: Haridor topilmadi: ID={user_id}")
                await query.answer()
                return
            new_data = user_data["edit_request"].split(", ")
            if len(new_data) != 4:
                await query.message.reply_text("Xato: Tahrir so‘rovi noto‘g‘ri formatda!")
                logger.error(f"approve_edit: Noto‘g‘ri tahrir so‘rovi: {user_data['edit_request']}")
                await query.answer()
                return
            user_data.update({
                "name": new_data[0],
                "phone": new_data[1],
                "address": new_data[2],
                "role": new_data[3],
                "edit_request": "",
                "edit_confirmed": "Yes"
            })
            if not update_user_data(user_id, user_data):
                await query.message.reply_text("Xato: Ma'lumotlar yangilanmadi!")
                logger.error(f"approve_edit: Ma'lumotlar yangilanmadi: ID={user_id}")
                await query.answer()
                return
            await context.bot.send_message(
                chat_id=user_id,
                text="Sizning shaxsiy ma'lumotlaringiz yangilandi!"
            )
            await query.message.edit_text(
                text=query.message.text + "\n\n**Holati: Tasdiqlangan**",
                parse_mode="Markdown",
                reply_markup=None
            )
            await query.message.reply_text(f"Ma'lumotlarni o'zgartirish tasdiqlandi.")
            logger.info(f"Ma'lumotlarni o'zgartirish tasdiqlandi: ID={user_id}")
            await query.answer()
        elif data.startswith("reject_edit_"):
            user_id = data[len("reject_edit_"):]
            user_data = get_user_data(user_id)
            if not user_data:
                await query.message.reply_text("Xato: Foydalanuvchi topilmadi!")
                logger.error(f"reject_edit: Haridor topilmadi: ID={user_id}")
                await query.answer()
                return
            user_data["edit_request"] = ""
            user_data["edit_confirmed"] = "Rejected"
            if not update_user_data(user_id, user_data):
                await query.message.reply_text("Xato: Ma'lumotlar yangilanmadi!")
                logger.error(f"reject_edit: Ma'lumotlar yangilanmadi: ID={user_id}")
                await query.answer()
                return
            await context.bot.send_message(
                chat_id=user_id,
                text="Ma'lumotlarni o'zgartirish so'rovingiz rad etildi. Qo'shimcha ma'lumot uchun admin bilan bog'laning."
            )
            await query.message.edit_text(
                text=query.message.text + "\n\n**Holati: Rad etildi**",
                parse_mode="Markdown",
                reply_markup=None
            )
            await query.message.reply_text(f"Ma'lumotlarni o'zgartirish rad etildi.")
            logger.info(f"Ma'lumotlarni o'zgartirish rad etildi: ID={user_id}")
            await query.answer()
        elif data.startswith("edit_product_"):
            product_name = data[len("edit_product_"):]
            group_name = USER_SELECTED_GROUP.get(user_id, "")
            product = next((p for p in get_products(group_name) if p["name"] == product_name), None)
            if not product:
                await query.message.reply_text("Xato: Mahsulot topilmadi!")
                logger.error(f"edit_product: Mahsulot topilmadi: {product_name} ({group_name})")
                await query.answer()
                return
            USER_STATE[user_id] = {
                "step": "edit_product_name",
                "old_product_name": product_name,
                "old_group_name": group_name,
                "current_name": product["name"],
                "current_price": product["price"],
                "current_bonus_percent": product["bonus_percent"],
                "current_quantity": product["quantity"]
            }
            await query.message.reply_text(
                f"Joriy mahsulot: {product_name} ({group_name})\n"
                f"Nom: {product['name']}\n"
                f"Narx: {format_currency(product['price'])}\n"
                f"Bonus foizi: {product['bonus_percent']}%\n"
                f"Miqdori: {product['quantity']} dona\n"
                f"Yangi nom kiriting (yoki o'zgartirmaslik uchun joriy nomni qaytaring):"
            )
            logger.info(f"Admin {user_id} mahsulotni tahrirlashni boshladi: {product_name} ({group_name})")
            await query.answer()
        elif data.startswith("select_group_edit_"):
            group_name = data[len("select_group_edit_"):]
            USER_SELECTED_GROUP[user_id] = group_name
            products = get_products(group_name)
            if not products:
                await query.message.reply_text(f"{group_name} guruhida mahsulotlar yo'q.")
                await query.answer()
                return
            keyboard = [[InlineKeyboardButton(f"{p['name']} ({format_currency(p['price'])})", callback_data=f"edit_product_{p['name']}")] for p in products]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text(f"{group_name} guruhidagi mahsulotlarni tanlang:", reply_markup=reply_markup)
            await query.answer()
        elif data.startswith("select_group_add_"):
            group_name = data[len("select_group_add_"):]
            USER_SELECTED_GROUP[user_id] = group_name
            USER_STATE[user_id] = {"step": "product_name"}
            await query.message.reply_text(f"{group_name} guruhiga yangi mahsulot nomini kiriting:")
            logger.info(f"Admin {user_id} mahsulot qo'shishni boshladi: Guruh={group_name}")
            await query.answer()
        elif data.startswith("group_"):
            group_name = data[len("group_"):]
            USER_SELECTED_GROUP[user_id] = group_name
            products = get_products(group_name)
            if not products:
                await query.message.reply_text(f"{group_name} guruhida mahsulotlar yo'q.")
                await query.answer()
                return
            text = f"{group_name} guruhidagi mahsulotlar:\n"
            for p in products:
                text += f"{p['name']}: {p['quantity']} dona, Bonus: {p['bonus_percent']}%\n"
            await query.message.reply_text(text)
            await query.answer()
    except (TimedOut, NetworkError) as e:
        logger.error(f"TimedOut in handle_admin_callback: {e}")
        await query.message.reply_text("Tarmoq xatosi yuz berdi, iltimos, keyinroq urinib ko'ring.")
        await query.answer()
    except Exception as e:
        logger.error(f"Unexpected error in handle_admin_callback: {e}", exc_info=True)
        await query.message.reply_text("Xato yuz berdi, iltimos, keyinroq urinib ko'ring.")
        await query.answer()

async def handle_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin funksiyalari"""
    user_id = str(update.effective_user.id)
    text = update.message.text.strip()
    logger.info(f"Admin {user_id} xabari: {text}")

    try:
        if text == "Yangi guruh qo'shish":
            USER_STATE[user_id] = {"step": "group_name"}
            await update.message.reply_text("Yangi guruh nomini kiriting:")
            logger.info(f"Admin {user_id} guruh qo'shishni boshladi")
        elif text == "Mahsulot qo'shish":
            groups = get_groups()
            if not groups:
                await update.message.reply_text("Hozirda guruhlar mavjud emas. Avval guruh qo'shing.")
                logger.info(f"Admin {user_id} mahsulot qo'shishni so'radi, lekin guruhlar yo'q")
                return
            keyboard = [[InlineKeyboardButton(group, callback_data=f"select_group_add_{group}")] for group in groups]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("Mahsulot qo'shish uchun guruhni tanlang:", reply_markup=reply_markup)
            logger.info(f"Admin {user_id} mahsulot qo'shish uchun guruh tanlashni boshladi")
        elif text == "Mahsulotlar ma'lumotlarini o'zgartirish":
            groups = get_groups()
            if not groups:
                await update.message.reply_text("Hozirda guruhlar mavjud emas.")
                logger.info(f"Admin {user_id} mahsulot o'zgartirishni so'radi, lekin guruhlar yo'q")
                return
            keyboard = [[InlineKeyboardButton(group, callback_data=f"select_group_edit_{group}")] for group in groups]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("Tahrirlamoqchi bo'lgan mahsulot guruhini tanlang:", reply_markup=reply_markup)
            logger.info(f"Admin {user_id} mahsulot o'zgartirish uchun guruh tanlashni boshladi")
        elif text == "Mahsulot ro'yxati":
            groups = get_groups()
            if not groups:
                await update.message.reply_text("Hozirda guruhlar mavjud emas.")
                return
            keyboard = [[InlineKeyboardButton(group, callback_data=f"group_{group}")] for group in groups]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("Mahsulotlar ro'yxatini ko'rish uchun guruhni tanlang:", reply_markup=reply_markup)
        elif text == "Buyurtmalar ro'yxati":
            USER_STATE[user_id] = {"step": "order_date"}
            await update.message.reply_text("Sanani kiriting (YYYY-MM-DD):")
            logger.info(f"Admin {user_id} buyurtmalar ro'yxatini so'radi")
        elif text == "Haridorlar ro'yxati":
            users = HARIDORLAR_SHEET.get_all_records()
            if users:
                users_text = "\n".join([f"ID: {u['ID']}, Ism: {u['Ism']}, Bonus: {format_currency(float(u['Bonus'] or 0))}" for u in users])
                await update.message.reply_text(users_text)
                logger.info(f"Admin {user_id} haridorlar ro'yxatini oldi")
            else:
                await update.message.reply_text("Haridorlar yo'q.")
                logger.info(f"Admin {user_id} haridorlar ro'yxatini so'radi, lekin haridorlar yo'q")
        elif user_id in USER_STATE:
            state = USER_STATE.get(user_id)
            if not state:
                await update.message.reply_text("Xato: Holat topilmadi. Iltimos, /start orqali qaytadan boshlang.")
                logger.error(f"Admin {user_id} uchun USER_STATE topilmadi")
                return
            logger.info(f"Admin {user_id} holati: {state['step']}, kiritilgan matn: {text}")
            if state["step"] == "group_name":
                if not text.strip():
                    await update.message.reply_text("Iltimos, guruh nomini kiriting (bo'sh bo'lmasligi kerak).")
                    return
                try:
                    save_group(text)
                    await update.message.reply_text(f"Guruh qo'shildi: {text}")
                    logger.info(f"Admin {user_id} yangi guruh qo'shdi: {text}")
                    del USER_STATE[user_id]
                except Exception as e:
                    await update.message.reply_text("Guruh qo'shishda xato yuz berdi.")
                    logger.error(f"Guruh qo'shish xatosi: {e}")
            elif state["step"] == "product_name":
                if not text.strip():
                    await update.message.reply_text("Iltimos, mahsulot nomini kiriting (bo'sh bo'lmasligi kerak).")
                    return
                USER_STATE[user_id]["product_name"] = text
                USER_STATE[user_id]["step"] = "product_price"
                await update.message.reply_text("Mahsulot narxini kiriting:")
                logger.info(f"Admin {user_id} mahsulot nomi kiritdi: {text}")
            elif state["step"] == "product_price":
                try:
                    price = float(text)
                    if price <= 0:
                        await update.message.reply_text("Iltimos, 0 dan katta narx kiriting.")
                        logger.warning(f"Admin {user_id} noto'g'ri narx kiritdi: {text}")
                        return
                    USER_STATE[user_id]["product_price"] = price
                    USER_STATE[user_id]["step"] = "product_bonus"
                    await update.message.reply_text("Usta uchun bonus foizini kiriting (%):")
                    logger.info(f"Admin {user_id} mahsulot narxini kiritdi: {price}")
                except ValueError:
                    await update.message.reply_text("Iltimos, to'g'ri narx kiriting (masalan, 40000).")
                    logger.warning(f"Admin {user_id} noto'g'ri narx formati kiritdi: {text}")
            elif state["step"] == "product_bonus":
                try:
                    bonus_percent = float(text)
                    if bonus_percent < 0:
                        await update.message.reply_text("Iltimos, 0 yoki undan katta foiz kiriting.")
                        logger.warning(f"Admin {user_id} noto'g'ri bonus foizi kiritdi: {text}")
                        return
                    USER_STATE[user_id]["product_bonus"] = bonus_percent
                    USER_STATE[user_id]["step"] = "product_quantity"
                    await update.message.reply_text("Mahsulot miqdorini kiriting (dona):")
                    logger.info(f"Admin {user_id} bonus foizini kiritdi: {bonus_percent}")
                except ValueError:
                    await update.message.reply_text("Iltimos, to'g'ri foiz kiriting (masalan, 12.5).")
                    logger.warning(f"Admin {user_id} noto'g'ri bonus foizi formati kiritdi: {text}")
            elif state["step"] == "product_quantity":
                try:
                    quantity = float(text)
                    if quantity < 0:
                        await update.message.reply_text("Iltimos, 0 yoki undan katta miqdor kiriting.")
                        logger.warning(f"Admin {user_id} noto'g'ri miqdor kiritdi: {text}")
                        return
                    data = {
                        "group_name": USER_SELECTED_GROUP.get(user_id, ""),
                        "name": USER_STATE[user_id]["product_name"],
                        "price": USER_STATE[user_id]["product_price"],
                        "bonus_percent": USER_STATE[user_id]["product_bonus"],
                        "quantity": quantity
                    }
                    save_product(data)
                    del USER_STATE[user_id]
                    del USER_SELECTED_GROUP[user_id]
                    await update.message.reply_text(f"Mahsulot qo'shildi: {data['name']} ({data['group_name']})")
                    logger.info(f"Admin {user_id} yangi mahsulot qo'shdi: {data['name']} ({data['group_name']})")
                except ValueError:
                    await update.message.reply_text("Iltimos, to'g'ri miqdor kiriting (masalan, 50).")
                    logger.warning(f"Admin {user_id} noto'g'ri miqdor formati kiritdi: {text}")
                except Exception as e:
                    await update.message.reply_text("Mahsulot qo'shishda xato yuz berdi.")
                    logger.error(f"Mahsulot qo'shish xatosi: {e}")
            elif state["step"] == "order_date":
                orders = get_orders_by_date(text)
                if orders:
                    for order in orders:
                        user_data = get_user_data(order["user_id"])
                        if not user_data:
                            await update.message.reply_text(f"Buyurtma uchun foydalanuvchi topilmadi: {order['user_name']}")
                            logger.error(f"order_date: Haridor topilmadi: ID={order['user_id']}")
                            continue
                        bonus_text = f"Bonus summasi: {format_currency(order['bonus_sum'])}" if user_data["role"] == "Usta" else ""
                        maps_link = f"https://maps.google.com/?q={order['address'].split('Lat: ')[1].split(', Lon: ')[0]},{order['address'].split(', Lon: ')[1]}" if "Lat:" in order["address"] else order["address"]
                        buttons = []
                        if order["confirmed"] == "No":
                            buttons = [
                                [InlineKeyboardButton("Tasdiqlash", callback_data=f"confirm_order_{order['row']}"),
                                 InlineKeyboardButton("Rad etish", callback_data=f"reject_order_{order['row']}")]
                            ]
                        await update.message.reply_text(
                            f"Buyurtma:\n"
                            f"Haridor ID: {order['user_id']}\n"
                            f"Haridor: [{order['user_name']}](tg://user?id={order['user_id']})\n"
                            f"Telefon: {order['phone']}\n"
                            f"Manzil: [{order['address']}]({maps_link})\n"
                            f"Guruh: {order['group_name']}\n"
                            f"Sana: {order['date']}\n"
                            f"Mahsulotlar:\n{order['cart_text']}\n"
                            f"Umumiy summa: {format_currency(order['total_sum'])}\n"
                            f"{bonus_text}\n"
                            f"Holat: {'Tasdiqlangan' if order['confirmed'] == 'Yes' else 'Rad etildi' if order['confirmed'] == 'Rejected' else 'Tasdiqlanmagan'}",
                            parse_mode="Markdown",
                            reply_markup=InlineKeyboardMarkup(buttons)
                        )
                    logger.info(f"Admin {user_id} sanadagi buyurtmalarni oldi: {text}")
                else:
                    await update.message.reply_text("Bu sanada buyurtmalar yo'q.")
                    logger.info(f"Admin {user_id} sanada buyurtmalar yo'q: {text}")
                del USER_STATE[user_id]
            elif state["step"] == "edit_product_name":
                if not text.strip():
                    await update.message.reply_text("Iltimos, mahsulot nomini kiriting (bo'sh bo'lmasligi kerak).")
                    logger.warning(f"Admin {user_id} bo'sh mahsulot nomi kiritdi")
                    return
                USER_STATE[user_id]["new_product_name"] = text.strip()
                USER_STATE[user_id]["step"] = "edit_product_price"
                await update.message.reply_text(
                    f"Yangi nom saqlandi: {text.strip()}\n"
                    f"Joriy narx: {format_currency(state['current_price'])}\n"
                    f"Yangi narx kiriting (yoki o'zgartirmaslik uchun joriy narxni qaytaring):"
                )
                logger.info(f"Admin {user_id} yangi mahsulot nomi kiritdi: {text.strip()}")
            elif state["step"] == "edit_product_price":
                try:
                    price = float(text)
                    if price <= 0:
                        await update.message.reply_text("Iltimos, 0 dan katta narx kiriting.")
                        logger.warning(f"Admin {user_id} noto'g'ri narx kiritdi: {text}")
                        return
                    USER_STATE[user_id]["new_product_price"] = price
                    USER_STATE[user_id]["step"] = "edit_product_bonus"
                    await update.message.reply_text(
                        f"Yangi narx saqlandi: {format_currency(price)}\n"
                        f"Joriy bonus foizi: {state['current_bonus_percent']}%\n"
                        f"Yangi bonus foizini kiriting (yoki o'zgartirmaslik uchun joriy foizni qaytaring):"
                    )
                    logger.info(f"Admin {user_id} yangi narx kiritdi: {price}")
                except ValueError:
                    await update.message.reply_text("Iltimos, to'g'ri narx kiriting (masalan, 45000).")
                    logger.warning(f"Admin {user_id} noto'g'ri narx formati kiritdi: {text}")
            elif state["step"] == "edit_product_bonus":
                try:
                    bonus_percent = float(text)
                    if bonus_percent < 0:
                        await update.message.reply_text("Iltimos, 0 yoki undan katta foiz kiriting.")
                        logger.warning(f"Admin {user_id} noto'g'ri bonus foizi kiritdi: {text}")
                        return
                    USER_STATE[user_id]["new_product_bonus"] = bonus_percent
                    USER_STATE[user_id]["step"] = "edit_product_quantity"
                    await update.message.reply_text(
                        f"Yangi bonus foizi saqlandi: {bonus_percent}%\n"
                        f"Joriy miqdor: {state['current_quantity']} dona\n"
                        f"Yangi miqdor kiriting (yoki o'zgartirmaslik uchun joriy miqdorni qaytaring):"
                    )
                    logger.info(f"Admin {user_id} yangi bonus foizi kiritdi: {bonus_percent}")
                except ValueError:
                    await update.message.reply_text("Iltimos, to'g'ri foiz kiriting (masalan, 15).")
                    logger.warning(f"Admin {user_id} noto'g'ri bonus foizi formati kiritdi: {text}")
            elif state["step"] == "edit_product_quantity":
                try:
                    quantity = float(text)
                    if quantity < 0:
                        await update.message.reply_text("Iltimos, 0 yoki undan katta miqdor kiriting.")
                        logger.warning(f"Admin {user_id} noto'g'ri miqdor kiritdi: {text}")
                        return
                    data = {
                        "group_name": USER_STATE[user_id]["old_group_name"],
                        "name": USER_STATE[user_id]["new_product_name"],
                        "price": USER_STATE[user_id]["new_product_price"],
                        "bonus_percent": USER_STATE[user_id]["new_product_bonus"],
                        "quantity": quantity
                    }
                    if update_product(USER_STATE[user_id]["old_product_name"], USER_STATE[user_id]["old_group_name"], data):
                        await update.message.reply_text(f"Mahsulot ma'lumotlari muvaffaqiyatli o'zgartirildi: {data['name']} ({data['group_name']})")
                        logger.info(f"Admin {user_id} mahsulotni yangiladi: {USER_STATE[user_id]['old_product_name']} -> {data['name']} ({data['group_name']})")
                    else:
                        await update.message.reply_text("Xato: Mahsulot yangilanmadi!")
                        logger.error(f"Admin {user_id} mahsulotni yangilay olmadi: {USER_STATE[user_id]['old_product_name']}")
                    del USER_STATE[user_id]
                    del USER_SELECTED_GROUP[user_id]
                except ValueError:
                    await update.message.reply_text("Iltimos, to'g'ri miqdor kiriting (masalan, 50).")
                    logger.warning(f"Admin {user_id} noto'g'ri miqdor formati kiritdi: {text}")
                except Exception as e:
                    await update.message.reply_text("Mahsulot yangilashda xato yuz berdi.")
                    logger.error(f"Mahsulot yangilash xatosi: {e}")
        else:
            await update.message.reply_text("Iltimos, menyudan biror amalni tanlang.")
            logger.warning(f"Admin {user_id} noma'lum xabar yubordi: {text}")
    except (TimedOut, NetworkError) as e:
        logger.error(f"TimedOut in handle_admin: {e}")
        await update.message.reply_text("Tarmoq xatosi yuz berdi, iltimos, keyinroq urinib ko'ring.")
    except Exception as e:
        logger.error(f"Unexpected error in handle_admin: {e}", exc_info=True)
        await update.message.reply_text("Xato yuz berdi, iltimos, keyinroq urinib ko'ring.")

async def get_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Foydalanuvchi ID'sini ko'rsatish"""
    await update.message.reply_text(f"Sizning ID: {update.effective_user.id}")

class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot is running")
    
    def do_POST(self):
        self.send_response(200)
        self.end_headers()
    
    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()

def run_http_server():
    server_address = ('0.0.0.0', 8080)
    httpd = HTTPServer(server_address, DummyHandler)
    logger.info("Starting HTTP server on port 8080")
    httpd.serve_forever()

async def clear_webhook(bot):
    """Webhookni o'chirish uchun funksiya"""
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook muvaffaqiyatli o'chirildi")
    except Exception as e:
        logger.error(f"Webhook o'chirishda xato: {e}")

def main():
    """Botni ishga tushirish"""
    try:
        init_sheets()
        request = HTTPXRequest(
            connection_pool_size=10,
            read_timeout=120.0,
            write_timeout=120.0,
            connect_timeout=120.0,
            pool_timeout=120.0
        )
        application = Application.builder().token(BOT_TOKEN).request(request).build()
        
        async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
            logger.error(f"Update {update} caused error {context.error}", exc_info=True)
            try:
                if isinstance(context.error, Conflict):
                    logger.warning("Conflict error detected, attempting to clear webhook")
                    await clear_webhook(application.bot)
                    await asyncio.sleep(5)
                    return
                if update and update.message:
                    await update.message.reply_text("Tarmoq xatosi yuz berdi, iltimos, keyinroq urinib ko'ring.")
                elif update and update.callback_query:
                    await update.callback_query.message.reply_text("Tarmoq xatosi yuz berdi, iltimos, keyinroq urinib ko'ring.")
                else:
                    logger.warning("No message or callback query available to send error response.")
            except Exception as e:
                logger.error(f"Error sending error message: {e}", exc_info=True)
        
        application.add_error_handler(error_handler)
        
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("id", get_id))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        application.add_handler(MessageHandler(filters.LOCATION, handle_location))
        application.add_handler(CallbackQueryHandler(handle_callback_query, pattern="^(group_|product_|confirm_cart)"))
        application.add_handler(CallbackQueryHandler(handle_admin_callback, pattern="^(confirm_order_|reject_order_|approve_bonus_|reject_bonus_|approve_edit_|reject_edit_|edit_product_|select_group_|group_)"))
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(clear_webhook(application.bot))
        
        http_thread = threading.Thread(target=run_http_server, daemon=True)
        http_thread.start()
        
        application.run_polling(
            poll_interval=3.0,
            timeout=120,
            drop_pending_updates=True,
            bootstrap_retries=5
        )
    except Exception as e:
        logger.error(f"Main function error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()