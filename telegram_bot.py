import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from flask import Flask, request
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import json

# Logging sozlamalari
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Atrof-muhit o‘zgaruvchilarini o‘qish
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.error("BOT_TOKEN topilmadi")
    raise ValueError("BOT_TOKEN atrof-muhit o'zgaruvchisi o'rnatilmagan")

SHEET_ID = os.getenv("SHEET_ID")
if not SHEET_ID:
    logger.error("SHEET_ID topilmadi")
    raise ValueError("SHEET_ID atrof-muhit o'zgaruvchisi o'rnatilmagan")

GOOGLE_SHEETS_CREDS = os.getenv("GOOGLE_SHEETS_CREDS")
if not GOOGLE_SHEETS_CREDS:
    logger.error("GOOGLE_SHEETS_CREDS topilmadi")
    raise ValueError("GOOGLE_SHEETS_CREDS atrof-muhit o'zgaruvchisi o'rnatilmagan")

# Flask ilovasini yaratish
app = Flask(__name__)

# Telegram Application obyekti
application = Application.builder().token(BOT_TOKEN).build()
logger.info("Application muvaffaqiyatli inicializatsiya qilindi")

# Google Sheets sozlamalari
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(GOOGLE_SHEETS_CREDS)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID)

# Google Sheets jadvallari
buyers_sheet = sheet.worksheet("Haridorlar")
products_sheet = sheet.worksheet("Mahsulotlar")
orders_sheet = sheet.worksheet("Buyurtmalar")

# Admin ID
ADMIN_ID = 1163346232

# Webhook so‘rovi
@app.route('/webhook', methods=['POST'])
async def webhook():
    logger.info("Webhook so'rovi qabul qilindi")
    try:
        update = Update.de_json(request.get_json(force=True), application.bot)
        if update is None:
            logger.error("Update obyekti yaratilmadi")
            return '', 400
        await application.process_update(update)
        logger.info("Update muvaffaqiyatli qayta ishlandi")
        return '', 200
    except Exception as e:
        logger.error(f"Webhook xatosi: {str(e)}")
        return '', 500

# /start buyrug‘i
async def start(update: Update, context):
    user_id = update.effective_user.id
    if user_id == ADMIN_ID:
        keyboard = [
            [InlineKeyboardButton("Yangi guruh qo‘shish", callback_data='add_group')],
            [InlineKeyboardButton("Mahsulot qo‘shish", callback_data='add_product')],
            [InlineKeyboardButton("Buyurtmalar ro‘yxati", callback_data='list_orders')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Xush kelibsiz, Admin! Quyidagi amallarni bajarishingiz mumkin:", reply_markup=reply_markup)
    else:
        if not is_user_registered(user_id):
            await update.message.reply_text("Iltimos, ma'lumotlaringizni saqlang.")
            await register_user(update, context)
        else:
            user_data = get_user_data(user_id)
            keyboard = [[InlineKeyboardButton("Mahsulot buyurtma qilish", callback_data='order_product')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(f"Xush kelibsiz, {user_data[1]}!", reply_markup=reply_markup)

# /id buyrug‘i
async def get_id(update: Update, context):
    user_id = update.effective_user.id
    await update.message.reply_text(f"Sizning ID: {user_id}")

# Foydalanuvchi ro‘yxatdan o‘tganligini tekshirish
def is_user_registered(user_id):
    try:
        buyers = buyers_sheet.get_all_records()
        return any(str(row["ID"]) == str(user_id) for row in buyers)
    except Exception as e:
        logger.error(f"Foydalanuvchi tekshirishda xato: {str(e)}")
        return False

# Foydalanuvchi ma'lumotlarini olish
def get_user_data(user_id):
    try:
        buyers = buyers_sheet.get_all_records()
        for row in buyers:
            if str(row["ID"]) == str(user_id):
                return row
        return None
    except Exception as e:
        logger.error(f"Foydalanuvchi ma'lumotlarini olishda xato: {str(e)}")
        return None

# Foydalanuvchi ro‘yxatdan o‘tishi
async def register_user(update: Update, context):
    user_id = update.effective_user.id
    context.user_data["register_step"] = "name"
    await update.message.reply_text("Ismingizni kiriting:")

async def handle_message(update: Update, context):
    user_id = update.effective_user.id
    text = update.message.text

    if "register_step" in context.user_data:
        step = context.user_data["register_step"]
        if step == "name":
            context.user_data["name"] = text
            context.user_data["register_step"] = "phone"
            await update.message.reply_text("Telefon raqamingizni kiriting:")
        elif step == "phone":
            context.user_data["phone"] = text
            context.user_data["register_step"] = "address"
            await update.message.reply_text("Manzilingizni kiriting:")
        elif step == "address":
            context.user_data["address"] = text
            context.user_data["register_step"] = "activity"
            await update.message.reply_text("Faoliyat turini kiriting:")
        elif step == "activity":
            try:
                buyers_sheet.append_row([user_id, context.user_data["name"], context.user_data["phone"], context.user_data["address"], text, 0])
                context.user_data.clear()
                keyboard = [[InlineKeyboardButton("Mahsulot buyurtma qilish", callback_data='order_product')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text("Ro‘yxatdan o‘tish muvaffaqiyatli yakunlandi!", reply_markup=reply_markup)
            except Exception as e:
                logger.error(f"Ro‘yxatdan o‘tishda xato: {str(e)}")
                await update.message.reply_text("Xato yuz berdi, qayta urinib ko‘ring.")
    elif context.user_data.get("order_step") == "quantity":
        try:
            quantity = int(text)
            if quantity <= 0:
                await update.message.reply_text("Iltimos, musbat son kiriting.")
                return
            context.user_data["quantity"] = quantity
            context.user_data["order_step"] = "location"
            await update.message.reply_text("Lokatsiyangizni yuboring:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Lokatsiya yuborish", request_location=True)]))
        except ValueError:
            await update.message.reply_text("Iltimos, to‘g‘ri son kiriting.")
    elif context.user_data.get("add_product_step") == "product_name":
        context.user_data["product_name"] = text
        context.user_data["add_product_step"] = "price"
        await update.message.reply_text("Mahsulot narxini kiriting:")
    elif context.user_data.get("add_product_step") == "price":
        try:
            context.user_data["price"] = float(text)
            context.user_data["add_product_step"] = "bonus_percent"
            await update.message.reply_text("Bonus foizini kiriting (masalan, 5):")
        except ValueError:
            await update.message.reply_text("Iltimos, to‘g‘ri narx kiriting.")
    elif context.user_data.get("add_product_step") == "bonus_percent":
        try:
            bonus_percent = float(text)
            products_sheet.append_row([context.user_data["group_name"], context.user_data["product_name"], context.user_data["price"], bonus_percent])
            context.user_data.clear()
            await update.message.reply_text("Mahsulot muvaffaqiyatli qo‘shildi!")
        except Exception as e:
            logger.error(f"Mahsulot qo‘shishda xato: {str(e)}")
            await update.message.reply_text("Xato yuz berdi, qayta urinib ko‘ring.")
    elif context.user_data.get("add_group_step") == "group_name":
        context.user_data["group_name"] = text
        products_sheet.append_row([text, "", 0, 0])  # Bo‘sh mahsulot qatori
        context.user_data.clear()
        await update.message.reply_text("Guruh muvaffaqiyatli qo‘shildi!")
    elif context.user_data.get("list_orders_step") == "date":
        try:
            date = text.strip()
            orders = orders_sheet.get_all_records()
            filtered_orders = [order for order in orders if order["Sana"].startswith(date)]
            if not filtered_orders:
                await update.message.reply_text(f"{date} sanasida buyurtmalar topilmadi.")
                return
            response = f"{date} sanasidagi buyurtmalar:\n"
            for order in filtered_orders:
                response += f"Haridor: {order['Buyurtmachi ismi']}, Mahsulotlar: {order['Mahsulotlar']}, Summa: {order['Umumiy summa']}\n"
            await update.message.reply_text(response)
            context.user_data.clear()
        except Exception as e:
            logger.error(f"Buyurtmalarni ko‘rishda xato: {str(e)}")
            await update.message.reply_text("Xato yuz berdi, qayta urinib ko‘ring.")

# Tugma bosilishi
async def button(update: Update, context):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    if query.data == "order_product":
        groups = list(set(row["Guruh nomi"] for row in products_sheet.get_all_records() if row["Guruh nomi"]))
        if not groups:
            await query.message.reply_text("Hozirda guruhlar mavjud emas.")
            return
        keyboard = [[InlineKeyboardButton(group, callback_data=f"group_{group}")] for group in groups]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text("Guruhni tanlang:", reply_markup=reply_markup)
    elif query.data.startswith("group_"):
        group_name = query.data[len("group_"):]
        context.user_data["group_name"] = group_name
        products = [row for row in products_sheet.get_all_records() if row["Guruh nomi"] == group_name and row["Mahsulot nomi"]]
        if not products:
            await query.message.reply_text("Bu guruhda mahsulotlar mavjud emas.")
            return
        keyboard = [[InlineKeyboardButton(product["Mahsulot nomi"], callback_data=f"product_{product['Mahsulot nomi']}")] for product in products]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text("Mahsulotni tanlang:", reply_markup=reply_markup)
    elif query.data.startswith("product_"):
        product_name = query.data[len("product_"):]
        context.user_data["product_name"] = product_name
        context.user_data["order_step"] = "quantity"
        await query.message.reply_text(f"{product_name} uchun miqdorni kiriting:")
    elif query.data == "add_group" and user_id == ADMIN_ID:
        context.user_data["add_group_step"] = "group_name"
        await query.message.reply_text("Yangi guruh nomini kiriting:")
    elif query.data == "add_product" and user_id == ADMIN_ID:
        groups = list(set(row["Guruh nomi"] for row in products_sheet.get_all_records() if row["Guruh nomi"]))
        if not groups:
            await query.message.reply_text("Hozirda guruhlar mavjud emas. Avval guruh qo‘shing.")
            return
        keyboard = [[InlineKeyboardButton(group, callback_data=f"add_product_group_{group}")] for group in groups]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text("Mahsulot qo‘shish uchun guruhni tanlang:", reply_markup=reply_markup)
    elif query.data.startswith("add_product_group_") and user_id == ADMIN_ID:
        context.user_data["group_name"] = query.data[len("add_product_group_"):]
        context.user_data["add_product_step"] = "product_name"
        await query.message.reply_text("Mahsulot nomini kiriting:")
    elif query.data == "list_orders" and user_id == ADMIN_ID:
        context.user_data["list_orders_step"] = "date"
        await query.message.reply_text("Buyurtmalar sanasini kiriting (YYYY-MM-DD):")

# Lokatsiya qabul qilish
async def handle_location(update: Update, context):
    if context.user_data.get("order_step") == "location":
        user_id = update.effective_user.id
        location = update.message.location
        user_data = get_user_data(user_id)
        if not user_data:
            await update.message.reply_text("Foydalanuvchi ma'lumotlari topilmadi.")
            return

        group_name = context.user_data["group_name"]
        product_name = context.user_data["product_name"]
        quantity = context.user_data["quantity"]
        product = next((row for row in products_sheet.get_all_records() if row["Guruh nomi"] == group_name and row["Mahsulot nomi"] == product_name), None)
        if not product:
            await update.message.reply_text("Mahsulot topilmadi.")
            return

        total_price = product["Narx"] * quantity
        bonus = total_price * (product["Bonus foizi"] / 100)
        order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        order_data = [user_id, user_data["Ism"], user_data["Telefon"], f"Lat: {location.latitude}, Lon: {location.longitude}", order_date, group_name, f"{product_name} ({quantity} dona)", total_price, bonus]
        
        try:
            orders_sheet.append_row(order_data)
            await update.message.reply_text(f"Buyurtma muvaffaqiyatli qabul qilindi!\nMahsulot: {product_name}\nMiqdor: {quantity}\nUmumiy summa: {total_price}\nBonus: {bonus}")
            await context.bot.send_message(ADMIN_ID, f"Yangi buyurtma:\nHaridor: {user_data['Ism']}\nMahsulot: {product_name} ({quantity} dona)\nSumma: {total_price}\nBonus: {bonus}")
            context.user_data.clear()
        except Exception as e:
            logger.error(f"Buyurtma saqlashda xato: {str(e)}")
            await update.message.reply_text("Xato yuz berdi, qayta urinib ko‘ring.")

# Handlerlarni qo‘shish
application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("id", get_id))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
application.add_handler(CallbackQueryHandler(button))
application.add_handler(MessageHandler(filters.LOCATION, handle_location))

# Flask ilovasini ishga tushirish
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 10000)))