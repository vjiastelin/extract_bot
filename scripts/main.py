from pathlib import Path
import os
from telegram import Update
from telegram.ext import MessageHandler, Filters, CallbackContext, CommandHandler, Updater
from utils import pdf_to_excel, acics_price
from decouple import config

dir_store = './tmp_storage'
Path(dir_store).mkdir(parents=True, exist_ok=True)
updater = Updater(token=config('BOT_TOKEN'))


def start(update: Update, context: CallbackContext):
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='''Привет, путник!
    Я умею парсить PDF файлы, которые содержат в себе таблицы.
    Для начала работы сбрось мне PDF''')


def echo(update: Update, context: CallbackContext):
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=update.message.text)


def parse(update: Update, context: CallbackContext):
    from_asic_store: bool = False
    if update.message.forward_from_chat is not None:
        from_asic_store = True if update.message.forward_from_chat.username == 'Asictradeshop' else False

    file_name = os.path.join(dir_store, str(
        update.message.from_user.username)) + '_file.pdf'

    if update.message.document.mime_type != 'application/pdf':
        context.bot.send_message(
            chat_id=update.effective_chat.id, text='PDF files only allowed')
        return
    file_info = context.bot.get_file(update.message.document)
    file_info.download(file_name)
    
    try:
        if from_asic_store:
            context.bot.send_message(               
                chat_id=config('CHAT_ID'),
                text=acics_price(file_name))
                
        else:
            for file in pdf_to_excel(file_name, update.message.from_user.username):
                with open(file, 'rb') as f:
                    context.bot.send_document(update.effective_chat.id, document=f)
    except Exception as e:
        context.bot.send_message(update.effective_chat.id,text='There is some error occured. Please see the log')
        raise e

updater.dispatcher.add_handler(CommandHandler('start', start))
updater.dispatcher.add_handler(MessageHandler(Filters.document, parse))
updater.start_polling()
updater.idle()
