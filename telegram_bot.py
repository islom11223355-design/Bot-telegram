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
from dotenv import load_dotenv

# Logging sozlamalari
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# .env faylidan o‘qish
load_dotenv()
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
ADMINS = [str(x) for x in os.getenv("ADMIN_IDS").split(",") if x]

# Global o‘zgaruvchilar
USER_STATE = {}
CART = {}
BONUS_REQUESTS = {}
USER_SELECTED_GROUP = {}
USER_CACHE = {}
PRODUCT_CACHE = {}
GROUP_CACHE = None
ORDER_CACHE = {}

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
        # Haridorlar varag‘i
        haridorlar_headers = ["ID", "Ism", "Telefon", "Manzil", "Faoliyat turi", "Bonus", "Tahrir So‘rovi", "Tahrir Tasdiqlangan"]
        if not HARIDORLAR_SHEET.row_values(1):
            HARIDORLAR_SHEET.append_row(haridorlar_headers)
        else:
            current_headers = HARIDORLAR_SHEET.row_values(1)
            if current_headers != haridorlar_headers:
                logger.warning(f"Haridorlar varag‘i sarlavhalari noto‘g‘ri: {current_headers}")
                HARIDORLAR_SHEET.update(range_name="A1:H1", values=[haridorlar_headers])

        # Mahsulotlar varag‘i
        mahsulotlar_headers = ["Guruh nomi", "Mahsulot nomi", "Narx", "Bonus foizi", "Miqdori"]
        if not MAHSULOTLAR_SHEET.row_values(1):
            MAHSULOTLAR_SHEET.append_row(mahsulotlar_headers)
        else:
            current_headers = MAHSULOTLAR_SHEET.row_values(1)
            if current_headers != mahsulotlar_headers:
                logger.warning(f"Mahsulotlar varag‘i sarlavhalari noto‘g‘ri: {current_headers}")
                MAHSULOTLAR_SHEET.update(range_name="A1:E1", values=[mahsulotlar_headers])

        # Buyurtmalar varag‘i
        buyurtmalar_headers = ["Haridor ID", "Buyurtmachi ismi", "Telefon", "Manzil", "Sana", "Guruh nomi", "Mahsulotlar", "Umumiy summa", "Bonus summasi", "Confirmed"]
        if not BUYURTMALAR_SHEET.row_values(1):
            BUYURTMALAR_SHEET.append_row(buyurtmalar_headers)
        else:
            current_headers = BUYURTMALAR_SHEET.row_values(1)
            if current_headers != buyurtmalar_headers:
                logger.warning(f"Buyurtmalar varag‘i sarlavhalari noto‘g‘ri: {current_headers}")
                BUYURTMALAR_SHEET.update(range_name="A1:J1", values=[buyurtmalar_headers])

        # Guruhlar varag‘i
        guruhlar_headers = ["Guruh Nomi"]
        if not GURUHLAR_SHEET.row_values(1):
            GURUHLAR_SHEET.append_row(guruhlar_headers)
        else:
            current_headers = GURUHLAR_SHEET.row_values(1)
            if current_headers != guruhlar_headers:
                logger.warning(f"Guruhlar varag‘i sarlavhalari noto‘g‘ri: {current_headers}")
                GURUHLAR_SHEET.update(range_name="A1:A1", values=[guruhlar_headers])
    except Exception as e:
        logger.error(f"Sheets init xatosi: {e}")
        raise

def save_user_data(user_id, data):
    """Foydalanuvchi ma'lumotlarini Google Sheets'ga saqlash"""
    try:
        if not all(key in data for key in ["name", "phone", "address", "role"]):
            logger.error(f"Missing required user data fields: {data}")
            return False
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
        return True
    except Exception as e:
        logger.error(f"Haridor saqlash xatosi: {e}")
        return False

def update_user_data(user_id, data, edit_request=False):
    """Foydalanuvchi ma'lumotlarini yangilash"""
    try:
        all_values = HARIDORLAR_SHEET.get_all_values()
        headers = all_values[0]
        for i, row in enumerate(all_values[1:], start=2):
            if row[0] == str(user_id):
                values = [
                    str(user_id),
                    data["name"],
                    data["phone"],
                    data["address"],
                    data["role"],
                    data.get("bonus", float(row[5]) if len(row) > 5 else 0),
                    data.get("edit_request", ""),
                    data.get("edit_confirmed", "")
                ]
                HARIDORLAR_SHEET.update(f"A{i}:H{i}", [values])
                USER_CACHE[user_id] = data
                logger.info(f"Haridor yangilandi: ID={user_id}, Bonus={data.get('bonus', 0)}")
                return True
        if edit_request:
            return save_user_data(user_id, data)
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
        all_values = HARIDORLAR_SHEET.get_all_values()
        headers = all_values[0]
        for row in all_values[1:]:
            if row[0] == str(user_id):
                user_data = {
                    "id": str(row[0]),
                    "name": row[1] if len(row) > 1 else "",
                    "phone": row[2] if len(row) > 2 else "",
                    "address": row[3] if len(row) > 3 else "",
                    "role": row[4] if len(row) > 4 else "",
                    "bonus": float(row[5] or 0) if len(row) > 5 else 0,
                    "edit_request": row[6] if len(row) > 6 else "",
                    "edit_confirmed": row[7] if len(row) > 7 else ""
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
            return False
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
        return True
    except Exception as e:
        logger.error(f"Mahsulot saqlash xatosi: {e}")
        return False

def update_product(old_name, group_name, data):
    """Mahsulot ma'lumotlarini yangilash"""
    try:
        all_values = MAHSULOTLAR_SHEET.get_all_values()
        headers = all_values[0]
        for i, row in enumerate(all_values[1:], start=2):
            if row[1] == old_name and row[0] == group_name:
                MAHSULOTLAR_SHEET.update(f"A{i}:E{i}", [[
                    data["group_name"],
                    data["name"],
                    data["price"],
                    data["bonus_percent"],
                    data.get("quantity", 0)
                ]])
                PRODUCT_CACHE.pop(group_name, None)
                PRODUCT_CACHE.pop(data["group_name"], None)
                logger.info(f"Mahsulot yangilandi: {old_name} -> {data['name']} ({data['group_name']})")
                return True
        logger.error(f"Mahsulot topilmadi: {old_name} ({group_name})")
        return False
    except Exception as e:
        logger.error(f"Mahsulot yangilash xatosi: {e}")
        return False

def delete_product(product_name, group_name):
    """Mahsulotni o‘chirish"""
    try:
        all_values = MAHSULOTLAR_SHEET.get_all_values()
        for i, row in enumerate(all_values[1:], start=2):
            if row[1] == product_name and row[0] == group_name:
                MAHSULOTLAR_SHEET.delete_rows(i)
                PRODUCT_CACHE.pop(group_name, None)
                logger.info(f"Mahsulot o‘chirildi: {product_name} ({group_name})")
                return True
        logger.error(f"Mahsulot topilmadi: {product_name} ({group_name})")
        return False
    except Exception as e:
        logger.error(f"Mahsulot o‘chirish xatosi: {e}")
        return False

def save_group(group_name):
    """Yangi guruh qo'shish"""
    try:
        if not group_name.strip():
            logger.error("Empty group name provided")
            return False
        GURUHLAR_SHEET.append_row([group_name.strip()])
        global GROUP_CACHE
        GROUP_CACHE = None
        logger.info(f"Yangi guruh qo'shildi: {group_name}")
        return True
    except Exception as e:
        logger.error(f"Guruh qo'shish xatosi: {e}")
        return False

def delete_group(group_name):
    """Guruhni o‘chirish"""
    try:
        all_values = GURUHLAR_SHEET.get_all_values()
        for i, row in enumerate(all_values[1:], start=2):
            if row[0] == group_name:
                GURUHLAR_SHEET.delete_rows(i)
                global GROUP_CACHE
                GROUP_CACHE = None
                PRODUCT_CACHE.pop(group_name, None)
                logger.info(f"Guruh o‘chirildi: {group_name}")
                return True
        logger.error(f"Guruh topilmadi: {group_name}")
        return False
    except Exception as e:
        logger.error(f"Guruh o‘chirish xatosi: {e}")
        return False

def get_products(group_name=None):
    """Mahsulotlar ro'yxatini olish (kesh bilan)"""
    try:
        cache_key = group_name or "all"
        if cache_key in PRODUCT_CACHE:
            return PRODUCT_CACHE[cache_key]
        all_values = MAHSULOTLAR_SHEET.get_all_values()
        headers = all_values[0]
        products = []
        for row in all_values[1:]:
            if group_name is None or row[0].strip() == group_name.strip():
                products.append({
                    "group_name": row[0] if len(row) > 0 else "",
                    "name": row[1] if len(row) > 1 else "",
                    "price": float(row[2] or 0) if len(row) > 2 else 0,
                    "bonus_percent": float(row[3] or 0) if len(row) > 3 else 0,
                    "quantity": float(row[4] or 0) if len(row) > 4 else 0
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
        all_values = GURUHLAR_SHEET.get_all_values()
        GROUP_CACHE = list(set(row[0].strip() for row in all_values[1:] if row and row[0]))
        return GROUP_CACHE
    except Exception as e:
        logger.error(f"Guruhlar olish xatosi: {e}")
        return []

def save_order(user_id, cart, address, group_name):
    """Buyurtmani saqlash"""
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
            datetime.now().strftime("%Y-%m-%d"),
            group_name,
            cart_text,
            total_sum,
            total_bonus,
            "No"
        ])
        order_id = BUYURTMALAR_SHEET.row_count
        if total_bonus > 0:
            update_bonus(user_id, total_bonus)
        logger.info(f"Buyurtma saqlandi: ID={user_id}, Guruh={group_name}, Bonus={total_bonus}, Order ID={order_id}")
        return order_id
    except Exception as e:
        logger.error(f"Buyurtma saqlash xatosi: {e}")
        return None

def update_bonus(user_id, bonus_amount):
    """Haridorning bonusini yangilash"""
    try:
        all_values = HARIDORLAR_SHEET.get_all_values()
        headers = all_values[0]
        for i, row in enumerate(all_values[1:], start=2):
            if row[0] == str(user_id):
                current_bonus = float(row[5] or 0) if len(row) > 5 else 0
                new_bonus = current_bonus + bonus_amount
                HARIDORLAR_SHEET.update_cell(i, 6, new_bonus)
                if user_id in USER_CACHE:
                    USER_CACHE[user_id]["bonus"] = new_bonus
                logger.info(f"Bonus yangilandi: ID={user_id}, Qo'shilgan={bonus_amount}, Umumiy={new_bonus}")
                return True
        logger.error(f"Haridor topilmadi bonus yangilashda: ID={user_id}")
        return False
    except Exception as e:
        logger.error(f"Bonus yangilash xatosi: {e}")
        return False

def get_orders_by_user(user_id):
    """Foydalanuvchi bo'yicha buyurtmalarni olish"""
    try:
        all_values = BUYURTMALAR_SHEET.get_all_values()
        headers = all_values[0]
        orders = []
        for i, row in enumerate(all_values[1:], start=2):
            if row[0] == str(user_id):
                orders.append({
                    "row": i,
                    "user_id": str(row[0]),
                    "user_name": row[1] if len(row) > 1 else "",
                    "phone": row[2] if len(row) > 2 else "",
                    "address": row[3] if len(row) > 3 else "",
                    "date": row[4] if len(row) > 4 else "",
                    "group_name": row[5] if len(row) > 5 else "",
                    "cart_text": row[6] if len(row) > 6 else "",
                    "total_sum": float(row[7] or 0) if len(row) > 7 else 0,
                    "bonus_sum": float(row[8] or 0) if len(row) > 8 else 0,
                    "confirmed": row[9] if len(row) > 9 else "No"
                })
        return orders
    except Exception as e:
        logger.error(f"Foydalanuvchi buyurtmalarini olish xatosi: {e}")
        return []

def get_all_orders():
    """Barcha buyurtmalarni olish"""
    try:
        all_values = BUYURTMALAR_SHEET.get_all_values()
        headers = all_values[0]
        orders = []
        for i, row in enumerate(all_values[1:], start=2):
            orders.append({
                "row": i,
                "user_id": str(row[0]),
                "user_name": row[1] if len(row) > 1 else "",
                "phone": row[2] if len(row) > 2 else "",
                "address": row[3] if len(row) > 3 else "",
                "date": row[4] if len(row) > 4 else "",
                "group_name": row[5] if len(row) > 5 else "",
                "cart_text": row[6] if len(row) > 6 else "",
                "total_sum": float(row[7] or 0) if len(row) > 7 else 0,
                "bonus_sum": float(row[8] or 0) if len(row) > 8 else 0,
                "confirmed": row[9] if len(row) > 9 else "No"
            })
        return orders
    except Exception as e:
        logger.error(f"Barcha buyurtmalarni olish xatosi: {e}")
        return []

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Botni boshlash"""
    user_id = str(update.effective_user.id)
    user_data = get_user_data(user_id)
    
    if user_id in ADMINS:
        keyboard = [
            ["Yangi guruh qo'shish", "Mahsulot qo'shish"],
            ["Mahsulotlar ma'lumotlarini o'zgartirish", "Mahsulot ro'yxati"],
            ["Buyurtmalar ro'yxati", "Haridorlar ro'yxati"],
            ["Guruh o‘chirish"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text("Xush kelibsiz, Admin! Quyidagi amallarni bajarishingiz mumkin:", reply_markup=reply_markup)
    else:
        if user_data:
            keyboard = [
                ["Shaxsiy ma'lumotlarni o'zgartirish", "Mahsulot buyurtma qilish"],
                ["Mening buyurtmalarim"]
            ]
            if user_data["role"] == "Usta":
                keyboard.append(["Umumiy Bonus", "Bonusni yechish"])
            keyboard.append(["Admin bilan bog'lanish"])
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(f"Xush kelibsiz, {user_data['name']}!", reply_markup=reply_markup)
        else:
            keyboard = [[KeyboardButton("Ma'lumotlaringizni saqlang")]]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text("Iltimos, ma'lumotlaringizni saqlang.", reply_markup=reply_markup)

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
        if not user_data and text != "Ma'lumotlaringizni saqlang":
            await update.message.reply_text("Iltimos, avval ma'lumotlaringizni saqlang.")
            return

        if text == "Ma'lumotlaringizni saqlang":
            USER_STATE[user_id] = {"step": "name"}
            await update.message.reply_text("Ismingizni kiriting:")
        elif text == "Shaxsiy ma'lumotlarni o'zgartirish":
            USER_STATE[user_id] = {
                "step": "edit_name",
                "bonus": user_data["bonus"],
                "current_name": user_data["name"],
                "current_phone": user_data["phone"],
                "current_address": user_data["address"],
                "current_role": user_data["role"]
            }
            await update.message.reply_text(f"Joriy ism: {user_data['name']}\nYangi ismingizni kiriting (yoki o'zgartirmaslik uchun joriy ismni qaytaring):")
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
            orders = get_orders_by_user(user_id)
            if orders:
                orders_text = []
                for order in orders:
                    bonus_text = f"Bonus summasi: {format_currency(order['bonus_sum'])}" if order["bonus_sum"] > 0 else ""
                    orders_text.append(
                        f"Sana: {order['date']}\n"
                        f"Guruh: {order['group_name']}\n"
                        f"Mahsulotlar:\n{order['cart_text']}\n"
                        f"Umumiy summa: {format_currency(order['total_sum'])}\n"
                        f"{bonus_text}\n"
                        f"Holat: {'Tasdiqlangan' if order['confirmed'] == 'Yes' else 'Rad etildi' if order['confirmed'] == 'Rejected' else 'Tasdiqlanmagan'}"
                    )
                await update.message.reply_text("\n\n".join(orders_text))
            else:
                await update.message.reply_text("Sizda buyurtmalar yo'q.")
            logger.info(f"User {user_id} buyurtmalarini ko'rdi")
        elif text == "Umumiy Bonus" and user_data["role"] == "Usta":
            await update.message.reply_text(f"Sizning umumiy bonusingiz: {format_currency(user_data['bonus'])}")
        elif text == "Bonusni yechish" and user_data["role"] == "Usta":
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
                    if save_user_data(user_id, data):
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
                        await update.message.reply_text("Ma'lumotlarni saqlashda xato yuz berdi.")
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
            elif state["step"] == "edit_name":
                if not text.strip():
                    await update.message.reply_text("Iltimos, ismingizni kiriting (bo'sh bo'lmasligi kerak).")
                    return
                USER_STATE[user_id]["name"] = text.strip()
                USER_STATE[user_id]["step"] = "edit_phone"
                await update.message.reply_text(f"Joriy telefon: {state['current_phone']}\nYangi telefon raqamingizni kiriting (yoki o'zgartirmaslik uchun joriy telefonni qaytaring):")
            elif state["step"] == "edit_phone":
                if re.match(r"^\+998\d{9}$", text) or text == state["current_phone"]:
                    USER_STATE[user_id]["phone"] = text
                    USER_STATE[user_id]["step"] = "edit_location"
                    keyboard = [[KeyboardButton("Lokatsiyani yuborish", request_location=True)]]
                    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                    await update.message.reply_text(f"Joriy manzil: {state['current_address']}\nYangi lokatsiyangizni yuboring (yoki o'zgartirmaslik uchun /skip buyrug'ini yuboring):", reply_markup=reply_markup)
                else:
                    await update.message.reply_text("Iltimos, to'g'ri telefon raqamini kiriting (+998XXXXXXXXX):")
            elif state["step"] == "edit_role":
                if text in ["Do'kon egasi", "Qurilish kompaniyasi", "Uy egasi", "Usta"] or text == state["current_role"]:
                    USER_STATE[user_id]["role"] = text
                    edit_request_str = f"{USER_STATE[user_id]['name']}|{USER_STATE[user_id]['phone']}|{USER_STATE[user_id]['address']}|{text}"
                    data = {
                        "name": USER_STATE[user_id]["name"],
                        "phone": USER_STATE[user_id]["phone"],
                        "address": USER_STATE[user_id]["address"],
                        "role": text,
                        "bonus": USER_STATE[user_id]["bonus"],
                        "edit_request": edit_request_str,
                        "edit_confirmed": "No"
                    }
                    if update_user_data(user_id, data, edit_request=True):
                        await context.bot.send_message(
                            chat_id=ADMINS[0],
                            text=f"Foydalanuvchi {user_id} ({user_data['name']}) shaxsiy ma'lumotlarini o'zgartirmoqchi:\n"
                                 f"Yangi ma'lumotlar: {data['edit_request'].replace('|', ', ')}\nTasdiqlaysizmi?",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("Tasdiqlash", callback_data=f"approve_edit_{user_id}"),
                                 InlineKeyboardButton("Rad etish", callback_data=f"reject_edit_{user_id}")]
                            ])
                        )
                        await update.message.reply_text("Ma'lumotlarni o'zgartirish so'rovi adminga yuborildi. Tasdiqlanishini kuting.")
                        del USER_STATE[user_id]
                    else:
                        await update.message.reply_text("Ma'lumotlarni o'zgartirish so'rovini yuborishda xato yuz berdi.")
                else:
                    await update.message.reply_text("Iltimos, quyidagi variantlardan birini tanlang: Do'kon egasi, Qurilish kompaniyasi, Uy egasi, Usta")
            elif text == "/skip" and state["step"] == "edit_location":
                USER_STATE[user_id]["address"] = state["current_address"]
                USER_STATE[user_id]["step"] = "edit_role"
                keyboard = [
                    ["Do'kon egasi", "Qurilish kompaniyasi"],
                    ["Uy egasi", "Usta"]
                ]
                reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                await update.message.reply_text(f"Joriy faoliyat turi: {state['current_role']}\nYangi faoliyat turini tanlang (yoki o'zgartirmaslik uchun joriy turni qaytaring):", reply_markup=reply_markup)
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
        address = f"Lat:{location.latitude} Lon:{location.longitude}"
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
                "maps_link": maps_link,
                "cart": CART[user_id]
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
            await update.message.reply_text(f"Joriy faoliyat turi: {USER_STATE[user_id]['current_role']}\nYangi faoliyat turini tanlang (yoki o'zgartirmaslik uchun joriy turni qaytaring):", reply_markup=reply_markup)

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Foydalanuvchi callback so'rovlarini qayta ishlash"""
    query = update.callback_query
    user_id = str(query.from_user.id)
    data = query.data
    logger.info(f"Callback query from {user_id}: {data}")

    try:
        await query.answer()
        if data.startswith("group_"):
            group_name = data[len("group_"): ]
            USER_SELECTED_GROUP[user_id] = group_name
            products = get_products(group_name)
            if not products:
                await query.message.reply_text(f"{group_name} guruhida mahsulotlar yo'q.")
                return
            keyboard = [[InlineKeyboardButton(f"{p['name']} ({format_currency(p['price'])})", callback_data=f"product_{p['name']}")] for p in products]
            keyboard.append([InlineKeyboardButton("Savatni tasdiqlash", callback_data="confirm_cart")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text(f"{group_name} guruhidagi mahsulotlar:", reply_markup=reply_markup)
        elif data.startswith("product_"):
            product_name = data[len("product_"): ]
            USER_STATE[user_id] = {"step": "quantity", "product_name": product_name}
            await query.message.reply_text(f"{product_name} uchun miqdorni kiriting:")
        elif data == "confirm_cart":
            if not CART.get(user_id):
                await query.message.reply_text("Savat bo'sh! Iltimos, avval mahsulot qo'shing.")
                return
            USER_STATE[user_id] = {"step": "order_location"}
            await query.message.reply_text("Buyurtma yetkazib beriladigan lokatsiyani yuboring:", reply_markup=ReplyKeyboardMarkup([[KeyboardButton("Lokatsiyani yuborish", request_location=True)]], resize_keyboard=True))
    except (TimedOut, NetworkError) as e:
        logger.error(f"TimedOut in handle_callback_query: {e}")
        await query.message.reply_text("Tarmoq xatosi yuz berdi, iltimos, keyinroq urinib ko'ring.")
    except Exception as e:
        logger.error(f"Unexpected error in handle_callback_query: {e}", exc_info=True)
        await query.message.reply_text("Xato yuz berdi, iltimos, keyinroq urinib ko'ring.")

async def handle_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin callback so'rovlarini qayta ishlash"""
    query = update.callback_query
    user_id = str(query.from_user.id)
    data = query.data
    logger.info(f"Admin callback from {user_id}: {data}")

    try:
        await query.answer()
        if user_id not in ADMINS:
            await query.message.reply_text("Sizda admin huquqlari yo'q.")
            return
        if data.startswith("confirm_order_"):
            order_user_id = data[len("confirm_order_"): ]
            if order_user_id not in ORDER_CACHE:
                await query.message.reply_text("Xato: Buyurtma topilmadi!")
                logger.error(f"confirm_order: Buyurtma topilmadi: User ID={order_user_id}")
                return
            order = ORDER_CACHE[order_user_id]
            order_row = save_order(order["user_id"], order["cart"], order["address"], order["group_name"])
            if order_row is None:
                await query.message.reply_text("Xato: Buyurtma saqlanmadi!")
                logger.error(f"confirm_order: Buyurtma saqlanmadi: User ID={order_user_id}")
                return
            user_data = get_user_data(order_user_id)
            if not user_data:
                await query.message.reply_text("Xato: Foydalanuvchi topilmadi!")
                logger.error(f"confirm_order: Haridor topilmadi: ID={order_user_id}")
                return
            bonus_text = f"\nUshbu buyurtma uchun yig'ilgan bonus: {format_currency(order['bonus_sum'])}\nUmumiy bonus: {format_currency(user_data['bonus'])}" if user_data["role"] == "Usta" else ""
            await context.bot.send_message(
                chat_id=order_user_id,
                text=f"Sizning buyurtmangiz tasdiqlandi, hamkorligingizdan hursandmiz!\nGuruh: {order['group_name']}\nMahsulotlar:\n{order['cart_text']}\nUmumiy summa: {format_currency(order['total_sum'])}{bonus_text}",
                parse_mode="Markdown"
            )
            await query.edit_message_text(
                text=query.message.text + "\n\n**Holati: Tasdiqlangan**",
                parse_mode="Markdown",
                reply_markup=None
            )
            await query.message.reply_text(f"Buyurtma tasdiqlandi.")
            logger.info(f"Buyurtma tasdiqlandi: User ID={order_user_id}, Bonus={order['bonus_sum']}")
            del ORDER_CACHE[order_user_id]
            del CART[order_user_id]
            del USER_SELECTED_GROUP[order_user_id]
        elif data.startswith("reject_order_"):
            order_user_id = data[len("reject_order_"): ]
            if order_user_id not in ORDER_CACHE:
                await query.message.reply_text("Xato: Buyurtma topilmadi!")
                logger.error(f"reject_order: Buyurtma topilmadi: User ID={order_user_id}")
                return
            order = ORDER_CACHE[order_user_id]
            await context.bot.send_message(
                chat_id=order_user_id,
                text="Sizning buyurtmangiz rad etildi. Qo'shimcha ma'lumot uchun admin bilan bog'laning."
            )
            await query.edit_message_text(
                text=query.message.text + "\n\n**Holati: Rad etildi**",
                parse_mode="Markdown",
                reply_markup=None
            )
            await query.message.reply_text(f"Buyurtma rad etildi.")
            logger.info(f"Buyurtma rad etildi: User ID={order_user_id}")
            del ORDER_CACHE[order_user_id]
            del CART[order_user_id]
            del USER_SELECTED_GROUP[order_user_id]
        elif data.startswith("approve_bonus_"):
            user_id = data[len("approve_bonus_"): ]
            user_data = get_user_data(user_id)
            if not user_data:
                await query.message.reply_text("Xato: Foydalanuvchi topilmadi!")
                logger.error(f"approve_bonus: Haridor topilmadi: ID={user_id}")
                return
            user_data["bonus"] = 0
            if not update_user_data(user_id, user_data):
                await query.message.reply_text("Xato: Bonus yangilanmadi!")
                logger.error(f"approve_bonus: Bonus yangilanmadi: ID={user_id}")
                return
            await context.bot.send_message(
                chat_id=user_id,
                text="Sizning bonus yechish so'rovingiz tasdiqlandi. Bonus summangiz 0 ga tenglashtirildi."
            )
            await query.edit_message_text(
                text=query.message.text + "\n\n**Holati: Tasdiqlangan**",
                parse_mode="Markdown",
                reply_markup=None
            )
            await query.message.reply_text(f"Bonus yechish tasdiqlandi.")
            logger.info(f"Bonus yechish tasdiqlandi: ID={user_id}")
            del BONUS_REQUESTS[user_id]
        elif data.startswith("reject_bonus_"):
            user_id = data[len("reject_bonus_"): ]
            user_data = get_user_data(user_id)
            if not user_data:
                await query.message.reply_text("Xato: Foydalanuvchi topilmadi!")
                logger.error(f"reject_bonus: Haridor topilmadi: ID={user_id}")
                return
            await context.bot.send_message(
                chat_id=user_id,
                text="Sizning bonus yechish so'rovingiz rad etildi. Qo'shimcha ma'lumot uchun admin bilan bog'laning."
            )
            await query.edit_message_text(
                text=query.message.text + "\n\n**Holati: Rad etildi**",
                parse_mode="Markdown",
                reply_markup=None
            )
            await query.message.reply_text(f"Bonus yechish rad etildi.")
            logger.info(f"Bonus yechish rad etildi: ID={user_id}")
            del BONUS_REQUESTS[user_id]
        elif data.startswith("approve_edit_"):
            user_id = data[len("approve_edit_"): ]
            user_data = get_user_data(user_id)
            if not user_data:
                await query.message.reply_text("Xato: Foydalanuvchi topilmadi!")
                logger.error(f"approve_edit: Haridor topilmadi: ID={user_id}")
                return
            try:
                new_data = user_data["edit_request"].split("|")
                if len(new_data) != 4:
                    await query.message.reply_text("Xato: Tahrir so‘rovi noto‘g‘ri formatda!")
                    logger.error(f"approve_edit: Noto‘g‘ri tahrir so‘rovi: {user_data['edit_request']}")
                    return
                updated_data = {
                    "name": new_data[0].strip(),
                    "phone": new_data[1].strip(),
                    "address": new_data[2].strip(),
                    "role": new_data[3].strip(),
                    "bonus": user_data["bonus"],
                    "edit_request": "",
                    "edit_confirmed": "Yes"
                }
                if update_user_data(user_id, updated_data):
                    await context.bot.send_message(
                        chat_id=user_id,
                        text="Sizning shaxsiy ma'lumotlaringiz muvaffaqiyatli yangilandi!"
                    )
                    await query.edit_message_text(
                        text=query.message.text + "\n\n**Holati: Tasdiqlangan**",
                        parse_mode="Markdown",
                        reply_markup=None
                    )
                    await query.message.reply_text(f"Ma'lumotlarni o'zgartirish tasdiqlandi.")
                    logger.info(f"Ma'lumotlarni o'zgartirish tasdiqlandi: ID={user_id}")
                else:
                    await query.message.reply_text("Xato: Ma'lumotlar yangilanmadi!")
                    logger.error(f"approve_edit: Ma'lumotlar yangilanmadi: ID={user_id}")
            except Exception as e:
                await query.message.reply_text("Xato: Ma'lumotlarni yangilashda xato yuz berdi!")
                logger.error(f"approve_edit: Xato: {e}")
        elif data.startswith("reject_edit_"):
            user_id = data[len("reject_edit_"): ]
            user_data = get_user_data(user_id)
            if not user_data:
                await query.message.reply_text("Xato: Foydalanuvchi topilmadi!")
                logger.error(f"reject_edit: Haridor topilmadi: ID={user_id}")
                return
            user_data["edit_request"] = ""
            user_data["edit_confirmed"] = "Rejected"
            if not update_user_data(user_id, user_data):
                await query.message.reply_text("Xato: Ma'lumotlar yangilanmadi!")
                logger.error(f"reject_edit: Ma'lumotlar yangilanmadi: ID={user_id}")
                return
            await context.bot.send_message(
                chat_id=user_id,
                text="Ma'lumotlarni o'zgartirish so'rovingiz rad etildi. Qo'shimcha ma'lumot uchun admin bilan bog'laning."
            )
            await query.edit_message_text(
                text=query.message.text + "\n\n**Holati: Rad etildi**",
                parse_mode="Markdown",
                reply_markup=None
            )
            await query.message.reply_text(f"Ma'lumotlarni o'zgartirish rad etildi.")
            logger.info(f"Ma'lumotlarni o'zgartirish rad etildi: ID={user_id}")
        elif data.startswith("edit_product_"):
            product_name = data[len("edit_product_"): ]
            group_name = USER_SELECTED_GROUP.get(user_id, "")
            product = next((p for p in get_products(group_name) if p["name"] == product_name), None)
            if not product:
                await query.message.reply_text("Xato: Mahsulot topilmadi!")
                logger.error(f"edit_product: Mahsulot topilmadi: {product_name} ({group_name})")
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
        elif data.startswith("delete_product_"):
            product_name = data[len("delete_product_"): ]
            group_name = USER_SELECTED_GROUP.get(user_id, "")
            if delete_product(product_name, group_name):
                await query.message.reply_text(f"Mahsulot o‘chirildi: {product_name} ({group_name})")
                logger.info(f"Admin {user_id} mahsulotni o‘chirdi: {product_name} ({group_name})")
            else:
                await query.message.reply_text("Xato: Mahsulot o‘chirilmadi!")
                logger.error(f"Admin {user_id} mahsulot o‘chirishda xato: {product_name} ({group_name})")
        elif data.startswith("select_group_edit_"):
            group_name = data[len("select_group_edit_"): ]
            USER_SELECTED_GROUP[user_id] = group_name
            products = get_products(group_name)
            if not products:
                await query.message.reply_text(f"{group_name} guruhida mahsulotlar yo'q.")
                return
            keyboard = [
                [InlineKeyboardButton(f"{p['name']} ({format_currency(p['price'])})", callback_data=f"edit_product_{p['name']}"),
                 InlineKeyboardButton("O‘chirish", callback_data=f"delete_product_{p['name']}")]
                for p in products
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text(f"{group_name} guruhidagi mahsulotlarni tanlang:", reply_markup=reply_markup)
        elif data.startswith("select_group_add_"):
            group_name = data[len("select_group_add_"): ]
            USER_SELECTED_GROUP[user_id] = group_name
            USER_STATE[user_id] = {"step": "product_name"}
            await query.message.reply_text(f"{group_name} guruhiga yangi mahsulot nomini kiriting:")
            logger.info(f"Admin {user_id} mahsulot qo'shishni boshladi: Guruh={group_name}")
        elif data.startswith("group_"):
            group_name = data[len("group_"): ]
            USER_SELECTED_GROUP[user_id] = group_name
            products = get_products(group_name)
            if not products:
                await query.message.reply_text(f"{group_name} guruhida mahsulotlar yo'q.")
                return
            text = f"{group_name} guruhidagi mahsulotlar:\n"
            for p in products:
                text += f"  • {p['name']}: {p['quantity']} dona, Narx: {format_currency(p['price'])}, Bonus: {p['bonus_percent']}%\n"
            await query.message.reply_text(text, parse_mode="Markdown")
    except (TimedOut, NetworkError) as e:
        logger.error(f"TimedOut in handle_admin_callback: {e}")
        await query.message.reply_text("Tarmoq xatosi yuz berdi, iltimos, keyinroq urinib ko'ring.")
    except Exception as e:
        logger.error(f"Unexpected error in handle_admin_callback: {e}", exc_info=True)
        await query.message.reply_text("Xato yuz berdi, iltimos, keyinroq urinib ko'ring.")

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
                logger.info(f"Admin {user_id} mahsulot ro'yxatini so'radi, lekin guruhlar yo'q")
                return
            text = "Mahsulotlar ro'yxati:\n\n"
            for group in groups:
                products = get_products(group)
                if products:
                    text += f"**{group}**:\n"
                    for p in products:
                        text += f"  • {p['name']}: {p['quantity']} dona, Narx: {format_currency(p['price'])}, Bonus: {p['bonus_percent']}%\n"
                    text += "\n"
            if text == "Mahsulotlar ro'yxati:\n\n":
                await update.message.reply_text("Hozirda mahsulotlar mavjud emas.")
                logger.info(f"Admin {user_id} mahsulot ro'yxatini so'radi, lekin mahsulotlar yo'q")
            else:
                await update.message.reply_text(text, parse_mode="Markdown")
                logger.info(f"Admin {user_id} mahsulot ro'yxatini oldi")
        elif text == "Buyurtmalar ro'yxati":
            orders = get_all_orders()
            if orders:
                for order in orders:
                    user_data = get_user_data(order["user_id"])
                    if not user_data:
                        await update.message.reply_text(f"Buyurtma uchun foydalanuvchi topilmadi: {order['user_name']}")
                        logger.error(f"Buyurtmalar ro'yxati: Haridor topilmadi: ID={order['user_id']}")
                        continue
                    bonus_text = f"Bonus summasi: {format_currency(order['bonus_sum'])}" if order["bonus_sum"] > 0 else ""
                    maps_link = f"https://maps.google.com/?q={order['address'].split('Lat:')[1].split(' Lon:')[0]},{order['address'].split(' Lon:')[1]}" if "Lat:" in order["address"] else order["address"]
                    buttons = []
                    if order["confirmed"] == "No":
                        buttons = [
                            [InlineKeyboardButton("Tasdiqlash", callback_data=f"confirm_order_{order['user_id']}"),
                             InlineKeyboardButton("Rad etish", callback_data=f"reject_order_{order['user_id']}")]
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
                logger.info(f"Admin {user_id} barcha buyurtmalarni ko'rdi")
            else:
                await update.message.reply_text("Hozirda buyurtmalar yo'q.")
                logger.info(f"Admin {user_id} buyurtmalar ro'yxatini so'radi, lekin buyurtmalar yo'q")
        elif text == "Haridorlar ro'yxati":
            all_values = HARIDORLAR_SHEET.get_all_values()
            headers = all_values[0]
            users = []
            for row in all_values[1:]:
                users.append({
                    "ID": row[0] if len(row) > 0 else "",
                    "Ism": row[1] if len(row) > 1 else "",
                    "Bonus": float(row[5] or 0) if len(row) > 5 else 0
                })
            if users:
                users_text = "\n".join([f"ID: {u['ID']}, Ism: {u['Ism']}, Bonus: {format_currency(u['Bonus'])}" for u in users])
                await update.message.reply_text(users_text)
                logger.info(f"Admin {user_id} haridorlar ro'yxatini oldi")
            else:
                await update.message.reply_text("Haridorlar yo'q.")
                logger.info(f"Admin {user_id} haridorlar ro'yxatini so'radi, lekin haridorlar yo'q")
        elif text == "Guruh o‘chirish":
            groups = get_groups()
            if not groups:
                await update.message.reply_text("Hozirda guruhlar mavjud emas.")
                logger.info(f"Admin {user_id} guruh o'chirishni so'radi, lekin guruhlar yo'q")
                return
            keyboard = [[InlineKeyboardButton(group, callback_data=f"delete_group_{group}")] for group in groups]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("O‘chiriladigan guruhni tanlang:", reply_markup=reply_markup)
            logger.info(f"Admin {user_id} guruh o'chirish uchun guruh tanlashni boshladi")
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
                if save_group(text):
                    await update.message.reply_text(f"Guruh qo'shildi: {text}")
                    logger.info(f"Admin {user_id} yangi guruh qo'shdi: {text}")
                else:
                    await update.message.reply_text("Guruh qo'shishda xato yuz berdi.")
                del USER_STATE[user_id]
            elif state["step"] == "product_name":
                if not text.strip():
                    await update.message.reply_text("Iltimos, mahsulot nomini kiriting (bo'sh bo'lmasligi kerak).")
                    return
                USER_STATE[user_id]["product_name"] = text.strip()
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
                    if save_product(data):
                        await update.message.reply_text(f"Mahsulot qo'shildi: {data['name']} ({data['group_name']})")
                        logger.info(f"Admin {user_id} yangi mahsulot qo'shdi: {data['name']} ({data['group_name']})")
                    else:
                        await update.message.reply_text("Mahsulot qo'shishda xato yuz berdi.")
                    del USER_STATE[user_id]
                    del USER_SELECTED_GROUP[user_id]
                except ValueError:
                    await update.message.reply_text("Iltimos, to'g'ri miqdor kiriting (masalan, 50).")
                    logger.warning(f"Admin {user_id} noto'g'ri miqdor formati kiritdi: {text}")
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
                    USER_STATE[user_id]["new_price"] = price
                    USER_STATE[user_id]["step"] = "edit_product_bonus"
                    await update.message.reply_text(
                        f"Yangi narx saqlandi: {format_currency(price)}\n"
                        f"Joriy bonus foizi: {state['current_bonus_percent']}%\n"
                        f"Yangi bonus foizini kiriting (yoki o'zgartirmaslik uchun joriy foizni qaytaring):"
                    )
                    logger.info(f"Admin {user_id} yangi narx kiritdi: {price}")
                except ValueError:
                    await update.message.reply_text("Iltimos, to'g'ri narx kiriting (masalan, 40000).")
                    logger.warning(f"Admin {user_id} noto'g'ri narx formati kiritdi: {text}")
            elif state["step"] == "edit_product_bonus":
                try:
                    bonus_percent = float(text)
                    if bonus_percent < 0:
                        await update.message.reply_text("Iltimos, 0 yoki undan katta foiz kiriting.")
                        logger.warning(f"Admin {user_id} noto'g'ri bonus foizi kiritdi: {text}")
                        return
                    USER_STATE[user_id]["new_bonus_percent"] = bonus_percent
                    USER_STATE[user_id]["step"] = "edit_product_quantity"
                    await update.message.reply_text(
                        f"Yangi bonus foizi saqlandi: {bonus_percent}%\n"
                        f"Joriy miqdor: {state['current_quantity']} dona\n"
                        f"Yangi miqdorni kiriting (yoki o'zgartirmaslik uchun joriy miqdorni qaytaring):"
                    )
                    logger.info(f"Admin {user_id} yangi bonus foizi kiritdi: {bonus_percent}")
                except ValueError:
                    await update.message.reply_text("Iltimos, to'g'ri foiz kiriting (masalan, 12.5).")
                    logger.warning(f"Admin {user_id} noto'g'ri bonus foizi formati kiritdi: {text}")
            elif state["step"] == "edit_product_quantity":
                try:
                    quantity = float(text)
                    if quantity < 0:
                        await update.message.reply_text("Iltimos, 0 yoki undan katta miqdor kiriting.")
                        logger.warning(f"Admin {user_id} noto'g'ri miqdor kiritdi: {text}")
                        return
                    data = {
                        "group_name": USER_SELECTED_GROUP.get(user_id, ""),
                        "name": USER_STATE[user_id]["new_product_name"],
                        "price": USER_STATE[user_id]["new_price"],
                        "bonus_percent": USER_STATE[user_id]["new_bonus_percent"],
                        "quantity": quantity
                    }
                    if update_product(state["old_product_name"], state["old_group_name"], data):
                        await update.message.reply_text(
                            f"Mahsulot yangilandi:\n"
                            f"Nom: {data['name']}\n"
                            f"Guruh: {data['group_name']}\n"
                            f"Narx: {format_currency(data['price'])}\n"
                            f"Bonus foizi: {data['bonus_percent']}%\n"
                            f"Miqdori: {data['quantity']} dona"
                        )
                        logger.info(f"Admin {user_id} mahsulotni yangiladi: {data['name']} ({data['group_name']})")
                    else:
                        await update.message.reply_text("Mahsulotni yangilashda xato yuz berdi.")
                        logger.error(f"Admin {user_id} mahsulotni yangilashda xato: {data['name']} ({data['group_name']})")
                    del USER_STATE[user_id]
                    del USER_SELECTED_GROUP[user_id]
                except ValueError:
                    await update.message.reply_text("Iltimos, to'g'ri miqdor kiriting (masalan, 50).")
                    logger.warning(f"Admin {user_id} noto'g'ri miqdor formati kiritdi: {text}")

    except Exception as e:
        logger.error(f"Xato admin funksiyasida: {e}", exc_info=True)
        await update.message.reply_text("Xato yuz berdi, iltimos, keyinroq urinib ko'ring.")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.error(f"Update {update} caused error {context.error}", exc_info=True)
        if update and hasattr(update, 'message') and update.message:
            await update.message.reply_text("Xato yuz berdi, iltimos, keyinroq urinib ko'ring yoki admin bilan bog'laning.")
    except Exception as e:
        logger.error(f"Error in error_handler: {e}", exc_info=True)

class HealthCheckHandler(BaseHTTPRequestHandler):
    """Render platformasi uchun /health endpointi"""
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

def run_health_check_server():
    """Health check serverini ishga tushirish"""
    server_address = ("", 8000)  # 8000 o'rniga 8080
    httpd = HTTPServer(server_address, HealthCheckHandler)
    logger.info("Starting health check server on port 8000...")
    httpd.serve_forever()

def main():
    """Botni ishga tushirish"""
    try:
        init_sheets()
        application = Application.builder().token(BOT_TOKEN).build()

        application.add_handler(CommandHandler("start", start))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        application.add_handler(MessageHandler(filters.LOCATION, handle_location))
        application.add_handler(CallbackQueryHandler(handle_admin_callback, pattern="^(confirm_order_|reject_order_|approve_bonus_|reject_bonus_|approve_edit_|reject_edit_|edit_product_|delete_product_|select_group_edit_|select_group_add_|delete_group_)"))
        application.add_handler(CallbackQueryHandler(handle_callback_query))
        application.add_error_handler(error_handler)

        # Health check serverini ishga tushirish
        threading.Thread(target=run_health_check_server, daemon=True).start()

        # Drop pending updates bilan polling
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            close_loop=False
        )
            
    except Conflict:
        logger.error("Bot is already running elsewhere. Terminating.")
    except Exception as e:
        logger.error(f"Botni ishga tushirishda xato: {e}", exc_info=True)

if __name__ == "__main__":
    main()