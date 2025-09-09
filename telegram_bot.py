import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)
import httpx

# Logging sozlamalari
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Google Sheets sozlamalari
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds_dict = json.loads(os.getenv('GOOGLE_SHEETS_CREDS'))
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
SHEET_ID = os.getenv('SHEET_ID')
SHEET = gspread.authorize(creds).open_by_key(SHEET_ID)

# Telegram bot token
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Admin ID (o‘zingizning Telegram ID’ngizni kiriting)
ADMIN_ID = 123456789  # O‘zingizning ID’ngiz bilan almashtiring

# ConversationHandler holatlari
GROUP, PRODUCT, ORDER, CONFIRM_ORDER = range(4)

# Google Sheets varaqlarini boshlash
def init_sheets():
    try:
        # Haridorlar varag‘i
        try:
            customers = SHEET.worksheet('Haridorlar')
        except gspread.exceptions.WorksheetNotFound:
            customers = SHEET.add_worksheet('Haridorlar', 1000, 3)
            customers.append_row(['Telegram ID', 'Ism', 'Telefon'])
        
        # Mahsulotlar varag‘i
        try:
            products = SHEET.worksheet('Mahsulotlar')
        except gspread.exceptions.WorksheetNotFound:
            products = SHEET.add_worksheet('Mahsulotlar', 1000, 3)
            products.append_row(['Mahsulot Nomi', 'Narxi', 'Guruh'])
        
        # Buyurtmalar varag‘i
        try:
            orders = SHEET.worksheet('Buyurtmalar')
            # Status ustunini tekshirish va qo‘shish
            headers = orders.row_values(1)
            if 'Status' not in headers:
                orders.append_row(headers + ['Status'])
        except gspread.exceptions.WorksheetNotFound:
            orders = SHEET.add_worksheet('Buyurtmalar', 1000, 5)
            orders.append_row(['Buyurtma ID', 'Haridor ID', 'Mahsulot', 'Soni', 'Status'])
        
        # Guruhlar varag‘i (qo‘lda yaratilgan bo‘lsa, faqat tekshiriladi)
        try:
            groups = SHEET.worksheet('Guruhlar')
            # Sarlavha tekshiruvi
            if not groups.row_values(1):
                groups.append_row(['Guruh Nomi'])
        except gspread.exceptions.WorksheetNotFound:
            groups = SHEET.add_worksheet('Guruhlar', 1000, 1)
            groups.append_row(['Guruh Nomi'])
        
        logger.info("Google Sheets varaqlari muvaffaqiyatli boshlandi")
    except Exception as e:
        logger.error(f"Google Sheets varaqlarini boshlashda xato: {e}")

# Boshlang‘ich buyruq
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id == ADMIN_ID:
        keyboard = [
            [InlineKeyboardButton("Yangi guruh qo‘shish", callback_data='add_group')],
            [InlineKeyboardButton("Yangi mahsulot qo‘shish", callback_data='add_product')],
            [InlineKeyboardButton("Buyurtmalarni ko‘rish", callback_data='view_orders')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text('Admin paneliga xush kelibsiz!', reply_markup=reply_markup)
        return GROUP
    else:
        await update.message.reply_text("Sizda admin ruxsati yo‘q.")
        return ConversationHandler.END

# Guruh qo‘shish
async def add_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.reply_text("Yangi guruh nomini kiriting:")
    return GROUP

async def save_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    group_name = update.message.text
    try:
        groups = SHEET.worksheet('Guruhlar')
        groups.append_row([group_name])
        await update.message.reply_text(f"'{group_name}' guruhi muvaffaqiyatli qo‘shildi!")
    except Exception as e:
        logger.error(f"Guruh qo‘shishda xato: {e}")
        await update.message.reply_text("Guruh qo‘shishda xato yuz berdi.")
    return ConversationHandler.END

# Mahsulot qo‘shish
async def add_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    groups = SHEET.worksheet('Guruhlar').get_all_values()[1:]  # Birinchi qator sarlavha
    if not groups:
        await query.message.reply_text("Hozirda guruhlar mavjud emas. Iltimos, avval guruh qo‘shing.")
        return ConversationHandler.END
    keyboard = [[InlineKeyboardButton(group[0], callback_data=f'group_{group[0]}')] for group in groups]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.message.reply_text("Mahsulot qo‘shish uchun guruhni tanlang:", reply_markup=reply_markup)
    return PRODUCT

async def select_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['selected_group'] = query.data.split('_')[1]
    await query.message.reply_text("Mahsulot nomini kiriting:")
    return PRODUCT

async def save_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    product_name = update.message.text
    context.user_data['product_name'] = product_name
    await update.message.reply_text("Mahsulot narxini kiriting (raqamlarda):")
    return PRODUCT

async def save_product_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        price = float(update.message.text)
        product_name = context.user_data['product_name']
        group_name = context.user_data['selected_group']
        products = SHEET.worksheet('Mahsulotlar')
        products.append_row([product_name, price, group_name])
        await update.message.reply_text(f"'{product_name}' mahsuloti '{group_name}' guruhiga qo‘shildi!")
    except Exception as e:
        logger.error(f"Mahsulot qo‘shishda xato: {e}")
        await update.message.reply_text("Mahsulot qo‘shishda xato yuz berdi.")
    return ConversationHandler.END

# Buyurtmalarni ko‘rish
async def view_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        orders = SHEET.worksheet('Buyurtmalar').get_all_values()[1:]  # Birinchi qator sarlavha
        if not orders:
            await query.message.reply_text("Hozirda buyurtmalar mavjud emas.")
            return ConversationHandler.END
        for order in orders:
            order_id, customer_id, product, quantity, *status = order  # Status ustuni ixtiyoriy
            status = status[0] if status else "Kutilmoqda"
            keyboard = [[InlineKeyboardButton("Tasdiqlash", callback_data=f'confirm_{order_id}')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text(
                f"Buyurtma ID: {order_id}\nHaridor ID: {customer_id}\nMahsulot: {product}\nSoni: {quantity}\nStatus: {status}",
                reply_markup=reply_markup
            )
        return CONFIRM_ORDER
    except Exception as e:
        logger.error(f"Buyurtmalarni ko‘rishda xato: {e}")
        await query.message.reply_text("Buyurtmalarni ko‘rishda xato yuz berdi.")
        return ConversationHandler.END

# Buyurtma tasdiqlash
async def confirm_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    order_id = query.data.split('_')[1]
    try:
        orders = SHEET.worksheet('Buyurtmalar')
        order_data = orders.get_all_values()[1:]  # Birinchi qator sarlavha
        order_found = False
        for i, order in enumerate(order_data, start=2):  # 2-qator dan boshlanadi
            if order[0] == order_id:
                orders.update_cell(i, 5, 'Tasdiqlangan')  # Status ustuni (5-ustun)
                await query.message.reply_text(f"Buyurtma {order_id} tasdiqlandi!")
                order_found = True
                break
        if not order_found:
            await query.message.reply_text(f"Xato: Buyurtma {order_id} topilmadi.")
    except Exception as e:
        logger.error(f"Buyurtma tasdiqlashda xato: {e}")
        await query.message.reply_text("Buyurtma tasdiqlashda xato yuz berdi.")
    return ConversationHandler.END

# Foydalanuvchi buyurtma berishi
async def order_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    groups = SHEET.worksheet('Guruhlar').get_all_values()[1:]  # Birinchi qator sarlavha
    if not groups:
        await update.message.reply_text("Hozirda guruhlar mavjud emas.")
        return ConversationHandler.END
    keyboard = [[InlineKeyboardButton(group[0], callback_data=f'order_group_{group[0]}')] for group in groups]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Buyurtma berish uchun guruhni tanlang:", reply_markup=reply_markup)
    return ORDER

async def select_order_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    group_name = query.data.split('_')[2]
    products = SHEET.worksheet('Mahsulotlar').get_all_values()[1:]  # Birinchi qator sarlavha
    group_products = [p for p in products if p[2] == group_name]
    if not group_products:
        await query.message.reply_text(f"'{group_name}' guruhida mahsulotlar mavjud emas.")
        return ConversationHandler.END
    keyboard = [[InlineKeyboardButton(f"{p[0]} - {p[1]} UZS", callback_data=f'product_{p[0]}')] for p in group_products]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.message.reply_text("Mahsulotni tanlang:", reply_markup=reply_markup)
    context.user_data['selected_group'] = group_name
    return ORDER

async def select_product(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['selected_product'] = query.data.split('_')[1]
    await query.message.reply_text("Buyurtma sonini kiriting (raqamlarda):")
    return ORDER

async def save_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        quantity = int(update.message.text)
        user_id = update.effective_user.id
        product_name = context.user_data['selected_product']
        orders = SHEET.worksheet('Buyurtmalar')
        order_id = str(len(orders.get_all_values()) - 1 + 1)  # Oddiy ID generatsiyasi
        orders.append_row([order_id, str(user_id), product_name, str(quantity), 'Kutilmoqda'])
        await update.message.reply_text(f"Buyurtma muvaffaqiyatli qabul qilindi! Buyurtma ID: {order_id}")
        # Adminga xabar yuborish
        keyboard = [[InlineKeyboardButton("Tasdiqlash", callback_data=f'confirm_{order_id}')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"Yangi buyurtma!\nID: {order_id}\nHaridor ID: {user_id}\nMahsulot: {product_name}\nSoni: {quantity}",
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Buyurtma saqlashda xato: {e}")
        await update.message.reply_text("Buyurtma berishda xato yuz berdi.")
    return ConversationHandler.END

# Xato ishlovchisi
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error {context.error}")
    if update and update.message:
        await update.message.reply_text("Xato yuz berdi, iltimos qayta urinib ko‘ring.")

def main():
    init_sheets()
    application = Application.builder().token(BOT_TOKEN).build()
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', start),
            CommandHandler('order', order_product)
        ],
        states={
            GROUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_group)],
            PRODUCT: [
                CallbackQueryHandler(select_group, pattern='^group_'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_product),
                MessageHandler(filters.Regex(r'^\d+(\.\d+)?$'), save_product_price)
            ],
            ORDER: [
                CallbackQueryHandler(select_order_group, pattern='^order_group_'),
                CallbackQueryHandler(select_product, pattern='^product_'),
                MessageHandler(filters.Regex(r'^\d+$'), save_order)
            ],
            CONFIRM_ORDER: [CallbackQueryHandler(confirm_order, pattern='^confirm_')]
        },
        fallbacks=[]
    )
    application.add_handler(conv_handler)
    application.add_error_handler(error_handler)
    application.run_polling()

if __name__ == '__main__':
    main()