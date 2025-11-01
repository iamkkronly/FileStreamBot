# © 2025 Kaustav Ray. All rights reserved.
# Licensed under the MIT License.

import logging
import asyncio
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    ContextTypes,
    filters,
    PicklePersistence,
)
from telegram.error import TelegramError
from fuzzywuzzy import fuzz
import math
import random
import re
import io
import time
import httpx
import uuid
import datetime
import html
import subprocess
from flask import Flask
from threading import Thread
import os
import sys
import sys
from functools import lru_cache
from werkzeug.serving import make_server

# ========================
# CONFIG
# ========================
BOT_TOKEN = os.environ.get("BOT_TOKEN")  # Bot Token
DB_CHANNEL = int(os.environ.get("DB_CHANNEL", 0))  # Database channel
LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", 0))  # Channel to log user queries
# Channels users must join for access
JOIN_CHECK_CHANNEL = [int(x) for x in os.environ.get("JOIN_CHECK_CHANNEL", "").split()]
ADMINS = [int(x) for x in os.environ.get("ADMINS", "").split()]        # Admin IDs
PM_SEARCH_ENABLED = os.environ.get("PM_SEARCH_ENABLED", "True") == "True"   # Controls whether non-admins can search in PM

# Words to ignore in search queries for better matching
SEARCH_STOP_WORDS = [
   "tamil", "telegu", "Bengali", "movie", "movies", "web", "series", "hd", "720p", "1080p", "480p", "4k",
    "hindi", "english", "dual", "audio", "bluray", "webrip", "hdrip", "hdtc"
]

# Characters to remove from filenames and search queries for flexible matching
CHARS_TO_REMOVE = r"~±×÷•°`_{}@#₹%&*-=()!\"':+/?৳$£€©®^π[]@#₹%&*-=()\"<>;|\\¿"

# Custom promotional message (Simplified as per the last request)
REACTIONS = ["👀", "😱", "🔥", "😍", "🎉", "🥰", "😇", "⚡"]
PROMO_CHANNELS = [
    {"name": "@filestore4u", "link": "https://t.me/filestore4u", "id": -1002692055617},
]
CUSTOM_PROMO_MESSAGE = (
    "Credit to Prince Kaustav Ray\n\n"
    "Join our main channel: @filestore4u"
)

HELP_TEXT = (
    "**👋 Here is a list of available commands:**\n\n"
    "**User Commands:**\n"
    "• `/start` - 🚀 Start the bot.\n"
    "• `/dl` - 🔗 Get a direct download link for a file.\n"
    "• `/help` - ℹ️ Show this help message.\n"
    "• `/info` - 🤖 Get bot information.\n"
    "• `/refer` - 🎁 Get your referral link to earn premium access.\n"
    "• `/request <name>` - 🙏 Request a file.\n"
    "• `/request_index` - 📂 Request a file or channel to be indexed.\n"
    "• Send any text to search for a file (admins only in private chat).\n\n"
    "**Admin Commands:**\n"
    "• `/log` - 📜 Show recent error logs.\n"
    "• `/total_users` - 👥 Get the total number of users.\n"
    "• `/total_files` - 🗂️ Get the total number of files in the current DB.\n"
    "• `/stats` - 📊 Get bot and database statistics.\n"
    "• `/findfile <name>` - 🔎 Find a file's ID by name.\n"
    "• `/recent` - ✨ Show the 10 most recently uploaded files.\n"
    "• `/deletefile <id>` - 🗑️ Delete a file from the database.\n"
    "• `/deleteall` - 💥 Delete all files from the current database.\n"
    "• `/ban <user_id>` - 🚫 Ban a user.\n"
    "• `/unban <user_id>` - ✅ Unban a user.\n"
    "• `/freeforall` - 🆓 Grant 12-hour premium access to all users.\n"
    "• `/broadcast <msg>` - 📣 Send a message to all users.\n"
    "• `/grp_broadcast <msg>` - 📢 Send a message to all connected groups where the bot is an admin.\n"
    "• `/index_channel <channel_id> [skip]` - ✍️ Index files from a channel.\n"
    "• Send a file to me in a private message to index it."
)


# A list of MongoDB URIs to use. Add as many as you need.
MONGO_URIS = os.environ.get("MONGO_URIS", "").split()
GROUPS_DB_URIS = os.environ.get("GROUPS_DB_URIS", "").split()
REFERRAL_DB_URI = os.environ.get("REFERRAL_DB_URI")
current_uri_index = 0

# Centralized connection manager
mongo_clients = {} # Will store MongoClient instances for each URI

# Pointers to the collections of the *currently active* database
db = None
files_col = None
users_col = None
banned_users_col = None
groups_col = None
referrals_col = None
referred_users_col = None


# In-memory caches for performance
banned_user_cache = {}    # {user_id: bool}


# Logging setup with an in-memory buffer for the /log command
log_stream = io.StringIO()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO, stream=log_stream
)
logger = logging.getLogger(__name__)

# Flask web server for Render health checks
app = Flask(__name__)

@app.route('/')
def home():
    """A simple route to confirm the web server is running."""
    return "Bot is alive and running!"


# ========================
# HELPERS
# ========================

async def get_random_file_from_db():
    """
    Fetches a single random file document from any of the available file databases
    using the centralized connection pool.
    """
    # Shuffle URIs to distribute the load for random queries
    shuffled_uris = random.sample(MONGO_URIS, len(MONGO_URIS))

    for uri in shuffled_uris:
        client = mongo_clients.get(uri)
        if not client:
            logger.warning(f"Skipping disconnected DB for random file fetch: ...{uri[-20:]}")
            continue

        try:
            db = client["telegram_files"]
            files_col = db["files"]
            # Use $sample for efficient random document retrieval
            pipeline = [{"$sample": {"size": 1}}]
            result = list(files_col.aggregate(pipeline))
            if result:
                return result[0]  # Return the first document found
        except Exception as e:
            logger.error(f"DB Error while fetching random file from ...{uri[-20:]}: {e}")
            continue # Try the next URI

    return None # Return None if no file is found in any DB

def escape_markdown(text: str) -> str:
    """Helper function to escape special characters in Markdown V2."""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return "".join('\\' + char if char in escape_chars else char for char in text)

def format_size(size_in_bytes: int) -> str:
    """Converts a size in bytes to a human-readable format."""
    if size_in_bytes is None:
        return "N/A"

    if size_in_bytes == 0:
        return "0 B"

    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_in_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_in_bytes / p, 2)
    return f"{s} {size_name[i]}"


def format_filename_for_display(filename: str) -> str:
    """Splits a long filename into two lines for better display."""
    if len(filename) < 40:
        return filename

    mid = len(filename) // 2
    split_point = -1

    # Try to find a space near the midpoint
    for i in range(mid, 0, -1):
        if filename[i] == ' ':
            split_point = i
            break

    if split_point == -1:
        for i in range(mid, len(filename)):
            if filename[i] == ' ':
                split_point = i
                break

    if split_point != -1:
        return filename[:split_point] + '\n' + filename[split_point+1:]
    else:
        # Fallback if no space is found (e.g., a single long word)
        return filename[:mid] + '\n' + filename[mid:]


def sanitize_text(text: str) -> str:
    """Sanitizes a string by removing special characters and normalizing spaces."""
    if not text:
        return ""
    # A translation table is more efficient than repeated re.sub for this task
    # Replace each character in CHARS_TO_REMOVE with a space
    translator = str.maketrans(CHARS_TO_REMOVE, ' ' * len(CHARS_TO_REMOVE))
    sanitized = text.translate(translator)
    # Also replace common separators that might not be in the list with a space
    sanitized = sanitized.replace("_", " ").replace(".", " ").replace("-", " ")
    # Condense multiple spaces into one and strip leading/trailing spaces
    name = re.sub(r"\s+", " ", sanitized).strip()
    return name


async def check_member_status(user_id, context: ContextTypes.DEFAULT_TYPE):
    """Check if the user is a member of ALL required promotional channels."""
    for channel in PROMO_CHANNELS:
        try:
            member = await context.bot.get_chat_member(chat_id=channel['id'], user_id=user_id)
            if member.status not in ["member", "administrator", "creator"]:
                return False
        except TelegramError as e:
            logger.error(f"Error checking member status for user {user_id} in channel {channel['id']}: {e}")
            return False # If we can't check one, we assume they are not a member.
    return True

async def is_banned(user_id):
    """Check if the user is banned, with in-memory caching."""
    # 1. Check cache first
    if user_id in banned_user_cache:
        return banned_user_cache[user_id]

    # 2. If not in cache, check DB
    if banned_users_col is not None:
        is_banned_status = banned_users_col.find_one({"_id": user_id}) is not None
        # 3. Store result in cache
        banned_user_cache[user_id] = is_banned_status
        return is_banned_status

    # Default to not banned if DB is unavailable
    return False

async def handle_file_request(user, file_id_str, context: ContextTypes.DEFAULT_TYPE, source_chat_id: int):
    """A centralized function to handle a file request from a user."""
    if await is_banned(user.id):
        await send_and_delete_message(context, source_chat_id, "🚫 You are banned from using this bot. 🚫")
        return

    await save_user_info(user)
    if not await check_member_status(user.id, context):
        buttons = [[InlineKeyboardButton(f"Join {ch['name']}", url=ch['link'])] for ch in PROMO_CHANNELS]
        keyboard = InlineKeyboardMarkup(buttons)
        await send_and_delete_message(context, source_chat_id, "❗️ You must join ALL our channels to use this bot! ❗️", reply_markup=keyboard)
        return

    file_data = None
    for uri in MONGO_URIS:
        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            temp_db = client["telegram_files"]
            temp_files_col = temp_db["files"]
            file_data = temp_files_col.find_one({"_id": ObjectId(file_id_str)})
            if file_data:
                break
        except Exception as e:
            logger.error(f"DB Error while fetching file {file_id_str}: {e}")

    if file_data:
        asyncio.create_task(send_file_task(user.id, source_chat_id, context, file_data, user.mention_html()))
    else:
        await send_and_delete_message(context, source_chat_id, "🤷‍♀️ File not found or an error occurred. 🤷‍♂️")


async def bot_can_respond(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Check if the bot should respond in a group chat.
    - Allows all private chats.
    - In groups, responds only if the bot is an administrator.
    """
    chat = update.effective_chat

    if chat.type == "private":
        return True

    if chat.type in ["group", "supergroup"]:
        try:
            bot_member = await context.bot.get_chat_member(chat.id, context.bot.id)
            if bot_member.status in ["administrator", "creator"]:
                return True
            else:
                logger.info(f"Bot is not an admin in group {chat.id}, ignoring message.")
                return False
        except TelegramError as e:
            logger.error(f"Could not check bot status in group {chat.id}: {e}")
            return False

    return False



async def send_and_delete_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    reply_markup=None,
    parse_mode=None,
    reply_to_message_id=None,
    auto_delete: bool = True
):
    """Sends a message and optionally schedules its deletion after 5 minutes."""
    try:
        if reply_to_message_id:
            sent_message = await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                reply_to_message_id=reply_to_message_id
            )
        else:
            sent_message = await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )

        deletion_task = None
        if auto_delete:
            deletion_task = asyncio.create_task(delete_message_after_delay(context, chat_id, sent_message.message_id, 5 * 60))

        return sent_message, deletion_task
    except TelegramError as e:
        logger.error(f"Error in send_and_delete_message to chat {chat_id}: {e}")
        return None, None

async def delete_message_after_delay(context, chat_id, message_id, delay):
    """Awaits a delay and then deletes a message."""
    await asyncio.sleep(delay)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"Auto-deleted message {message_id} from chat {chat_id}.")
    except TelegramError as e:
        logger.warning(f"Failed to auto-delete message {message_id} from chat {chat_id}: {e}")


def connect_to_mongo():
    """
    Initializes connection pools for all database URIs specified in the config.
    It also sets the initial active database connection.
    """
    global mongo_clients, db, files_col, users_col, banned_users_col, groups_col, referrals_col, referred_users_col, clones_col, current_uri_index

    # Consolidate all unique URIs
    all_uris = set(MONGO_URIS + GROUPS_DB_URIS)
    if REFERRAL_DB_URI:
        all_uris.add(REFERRAL_DB_URI)

    for uri in all_uris:
        try:
            # Create a client with connection pooling
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            # The ismaster command is cheap and forces the client to check the connection.
            client.admin.command('ismaster')
            mongo_clients[uri] = client
            logger.info(f"Successfully created connection pool for ...{uri[-20:]}")
        except PyMongoError as e:
            logger.critical(f"FATAL: Could not connect to MongoDB at {uri}. Error: {e}")
            mongo_clients[uri] = None # Mark as failed

    # Set the initial active database for file operations
    initial_uri = MONGO_URIS[current_uri_index]
    initial_client = mongo_clients.get(initial_uri)

    if initial_client:
        db = initial_client["telegram_files"]
        files_col = db["files"]
        users_col = db["users"]
        banned_users_col = db["banned_users"]
        # groups_col is managed separately as it's in a different database

        # Connect to the referral database
        if REFERRAL_DB_URI:
            referral_client = mongo_clients.get(REFERRAL_DB_URI)
            if referral_client:
                referral_db = referral_client["referral_db"]
                referrals_col = referral_db["referrals"]
                referred_users_col = referral_db["referred_users"]
                logger.info("Successfully connected to Referral MongoDB.")
            else:
                logger.critical("Failed to connect to the Referral MongoDB URI. Referral system will not function.")
        else:
            logger.warning("REFERRAL_DB_URI not set. Referral system will be disabled.")

        logger.info(f"Successfully connected to initial MongoDB at index {current_uri_index}.")
        return True
    else:
        logger.critical(f"Failed to connect to the initial MongoDB URI at index {current_uri_index}. Bot may not function correctly.")
        return False

async def save_user_info(user: Update.effective_user):
    """Saves user information to the database if not already present."""
    if users_col is not None:
        try:
            users_col.update_one(
                {"_id": user.id},
                {
                    "$set": {
                        "first_name": user.first_name,
                        "last_name": user.last_name,
                        "username": user.username,
                    }
                },
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error saving user info for {user.id}: {e}")


# ========================
# TASK FUNCTIONS (FOR BACKGROUND EXECUTION)
# ========================

async def react_to_message_task(update: Update):
    """Background task to react to a message without blocking."""
    try:
        message = update.effective_message
        if message:
            await message.react(reaction=random.choice(REACTIONS))
    except TelegramError as e:
        logger.warning(f"Could not react to message: {e}")


async def send_file_task(user_id: int, source_chat_id: int, context: ContextTypes.DEFAULT_TYPE, file_data: dict, user_mention: str):
    """Background task to send a single file to the user's private chat and auto-delete it."""
    try:
        caption_text = file_data.get("file_name", "").strip()
        if not caption_text:
            caption_text = "Download File"

        warning_message = "⚠️ ❌👉This file automatically❗delete after 5 minute❗so please forward in another chat👈❌"

        final_caption_text = caption_text # Keep a mutable copy for truncation

        while True:
            # Construct the full caption with the deeplink
            new_caption = f'<a href="https://t.me/filestore4u">{html.escape(final_caption_text)}</a>\n\n{warning_message}'

            # Check byte length
            if len(new_caption.encode('utf-8')) <= 1024:
                break # Caption is valid

            # If too long, truncate the filename part
            final_caption_text = final_caption_text[:-1]

            # Fallback if filename becomes empty
            if not final_caption_text:
                # Fallback to a generic name that should fit
                new_caption = f'<a href="https://t.me/filestore4u">Download File</a>\n\n{warning_message}'
                break

        logger.info(f"Attempting to send file with caption: {new_caption}")
        sent_message = await context.bot.copy_message(
            chat_id=user_id,
            from_chat_id=file_data["channel_id"],
            message_id=file_data["file_id"],
            caption=new_caption,
            parse_mode="HTML"
        )

        if sent_message:
            await send_and_delete_message(context, user_id, CUSTOM_PROMO_MESSAGE)
            confirmation_text = f"✅ {user_mention}, I have sent the file to you in a private message. 🤫 It will be deleted automatically in 5 minutes. ⏳"
            await send_and_delete_message(context, source_chat_id, confirmation_text, parse_mode="HTML")

            await asyncio.sleep(5 * 60)
            await context.bot.delete_message(chat_id=user_id, message_id=sent_message.message_id)
            logger.info(f"Deleted message {sent_message.message_id} from chat {user_id}.")

    except TelegramError as e:
        if "Forbidden: bot can't initiate conversation with a user" in str(e):
             await send_and_delete_message(context, source_chat_id, f"❌ {user_mention}, I can't send you the file because you haven't started a private chat with me. 😔 Please start the bot privately and try again. 🙏", parse_mode="HTML")
        else:
            logger.error(f"Failed to send file to user {user_id}: {e}")
            await send_and_delete_message(context, source_chat_id, "🤷‍♀️ File not found or could not be sent. 🤷‍♂️")
    except Exception:
        logger.exception(f"An unexpected error occurred in send_file_task for user {user_id}")
        await send_and_delete_message(context, source_chat_id, "🆘 An unexpected error occurred. Please try again later. 🆘")


async def send_all_files_task(user_id: int, source_chat_id: int, context: ContextTypes.DEFAULT_TYPE, file_list: list, user_mention: str):
    """Background task to send multiple files to the user's private chat and auto-delete them."""
    sent_messages = []
    try:
        for file in file_list:
            caption_text = file.get("file_name", "").strip()
            if not caption_text:
                caption_text = "Download File"

            warning_message = "⚠️ ❌👉This file automatically❗delete after 5 minute❗so please forward in another chat👈❌"

            final_caption_text = caption_text # Keep a mutable copy for truncation

            while True:
                # Construct the full caption with the deeplink
                new_caption = f'<a href="https://t.me/filestore4u">{html.escape(final_caption_text)}</a>\n\n{warning_message}'

                # Check byte length
                if len(new_caption.encode('utf-8')) <= 1024:
                    break # Caption is valid

                # If too long, truncate the filename part
                final_caption_text = final_caption_text[:-1]

                # Fallback if filename becomes empty
                if not final_caption_text:
                    # Fallback to a generic name that should fit
                    new_caption = f'<a href="https://t.me/filestore4u">Download File</a>\n\n{warning_message}'
                    break

            logger.info(f"Attempting to send file in batch with caption: {new_caption}")
            sent_message = await context.bot.copy_message(
                chat_id=user_id,
                from_chat_id=file["channel_id"],
                message_id=file["file_id"],
                caption=new_caption,
                parse_mode="HTML"
            )
            sent_messages.append(sent_message.message_id)
            await send_and_delete_message(context, user_id, CUSTOM_PROMO_MESSAGE)
            await asyncio.sleep(0.5)

        confirmation_text = f"✅ {user_mention}, I have sent all files to you in a private message. 🤫 They will be deleted automatically in 5 minutes. ⏳"
        await send_and_delete_message(
            context,
            source_chat_id,
            confirmation_text,
            parse_mode="HTML"
        )

        await asyncio.sleep(5 * 60)
        for message_id in sent_messages:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=message_id)
                logger.info(f"Deleted message {message_id} from chat {user_id}.")
            except TelegramError as e:
                logger.warning(f"Failed to delete message {message_id} for user {user_id}: {e}")

    except TelegramError as e:
        if "Forbidden: bot can't initiate conversation with a user" in str(e):
             await send_and_delete_message(context, source_chat_id, f"❌ {user_mention}, I can't send you the files because you haven't started a private chat with me. 😔 Please start the bot privately and try again. 🙏", parse_mode="HTML")
        else:
            logger.error(f"Failed to send one or more files to user {user_id}: {e}")
            await send_and_delete_message(context, source_chat_id, "😥 One or more files could not be sent. 😥")
    except Exception:
        logger.exception(f"An unexpected error occurred in send_all_files_task for user {user_id}")
        await send_and_delete_message(context, source_chat_id, "🆘 An unexpected error occurred. Please try again later. 🆘")




# ========================
# COMMAND HANDLERS
# ========================


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command, including verification and referral deep links."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "🚫 You are banned from using this bot. 🚫")
        return

    user = update.effective_user

    # Save user info now, regardless of deep link status
    await save_user_info(user)

    # Handle deep links
    if context.args:
        payload = context.args[0]

        # File deep link
        if payload.startswith("files_"):
            file_id_str = payload.split("_", 1)[1]
            await handle_file_request(user, file_id_str, context, update.effective_chat.id)
            return

        # Referral deep link
        elif payload.startswith("ref_"):
            try:
                referrer_id = int(payload.split("_", 1)[1])
                is_already_referred = referred_users_col is not None and referred_users_col.find_one({"_id": user.id}) is not None
                if referrer_id != user.id and not is_already_referred:
                    if referrals_col is not None and referred_users_col is not None:
                        referrals_col.update_one({"_id": referrer_id}, {"$inc": {"referral_count": 1}}, upsert=True)
                        referred_users_col.insert_one({"_id": user.id})
                        referrer_data = referrals_col.find_one({"_id": referrer_id})
                        if referrer_data and referrer_data.get("referral_count", 0) >= 10:
                            referrals_col.update_one(
                                {"_id": referrer_id},
                                {"$set": {
                                    "premium_until": datetime.datetime.utcnow() + datetime.timedelta(days=30),
                                    "referral_count": 0
                                }}
                            )
                            try:
                                await context.bot.send_message(
                                    chat_id=referrer_id,
                                    text="🎉 **Congratulations!** 🎉\n\nYou've successfully referred 10 users and earned **1 month of premium access**! 🥳 Enjoy the bot without ads or verification. 😎"
                                )
                            except TelegramError as e:
                                logger.warning(f"Could not notify referrer {referrer_id} about premium status: {e}")
            except (IndexError, ValueError) as e:
                logger.error(f"Could not parse referral link payload: {payload} - {e}")
            # After handling the referral, do not show the main start message.
            # The user has already started the bot.
            return

    # Standard start message only if there are no args
    bot_username = context.bot.username
    owner_id = ADMINS[0] if ADMINS else None
    user_mention = user.mention_html()

    welcome_text = (
        f"👋 Hello {user_mention}, I am an advanced filter bot. 🤖\n\n"
        "I can help you find files in this chat with ease. 🔎 Just send me the name of the file you're looking for!\n\n"
        "Click the buttons below to learn more about how I work. 👇"
    )

    keyboard = [
        [
            InlineKeyboardButton("ℹ️ About Bot", callback_data="start_about"),
            InlineKeyboardButton("❓ Help", callback_data="start_help")
        ],
        [
            InlineKeyboardButton("➕ Add Me To Your Group ➕", url=f"https://t.me/{bot_username}?startgroup=true")
        ],
        [
            InlineKeyboardButton("👑 Owner", url=f"tg://user?id={owner_id}") if owner_id else InlineKeyboardButton("👑 Owner", callback_data="no_owner")
        ],
        [
            InlineKeyboardButton("❌ Close", callback_data="start_close")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await send_and_delete_message(
        context,
        update.effective_chat.id,
        welcome_text,
        reply_markup=reply_markup,
        parse_mode="HTML"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows the help message and available commands."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "🚫 You are banned from using this bot. 🚫")
        return
    await send_and_delete_message(context, update.effective_chat.id, HELP_TEXT, parse_mode="Markdown")


async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows information about the bot."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "🚫 You are banned from using this bot. 🚫")
        return
    info_message = (
        "**🤖 About this Bot 🤖**\n\n"
        "This bot helps you find and share files on Telegram. 📁\n"
        "• Developed by Kaustav Ray. 👑"
    )
    await send_and_delete_message(context, update.effective_chat.id, info_message, parse_mode="Markdown")


async def rand_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /rand command to send a random file."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "🚫 You are banned from using this bot. 🚫")
        return

    await save_user_info(update.effective_user)
    if not await check_member_status(update.effective_user.id, context):
        buttons = [[InlineKeyboardButton(f"Join {ch['name']}", url=ch['link'])] for ch in PROMO_CHANNELS]
        keyboard = InlineKeyboardMarkup(buttons)
        await send_and_delete_message(context, update.effective_chat.id, "❗️ You must join ALL our channels to use this bot! ❗️", reply_markup=keyboard)
        return

    user_id = update.effective_user.id
    await send_and_delete_message(context, update.effective_chat.id, "🎲 Fetching a random file for you... 🎲")

    file_data = await get_random_file_from_db()

    if file_data:
        asyncio.create_task(send_file_task(user_id, update.effective_chat.id, context, file_data, update.effective_user.mention_html()))
    else:
        await send_and_delete_message(context, update.effective_chat.id, "🤷‍♀️ Could not find a random file. The database might be empty. 🤷‍♂️")


async def request_index_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Allows any user to request a channel to be indexed, or to request a specific file to be indexed by replying to it.
    """
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "🚫 You are banned from using this bot. 🚫")
        return

    user = update.effective_user
    replied_message = update.message.reply_to_message

    # Workflow for replying to a file to request its index
    if replied_message and (replied_message.document or replied_message.video or replied_message.audio):
        if ADMINS:
            primary_admin_id = ADMINS[0]
            requester_mention = user.mention_html()

            file_to_send = replied_message.document or replied_message.video or replied_message.audio

            try:
                # Send a new message with the file to the admin, handling different file types
                if replied_message.document:
                    sent_file_message = await context.bot.send_document(
                        chat_id=primary_admin_id,
                        document=file_to_send.file_id,
                        caption=replied_message.caption
                    )
                elif replied_message.video:
                    sent_file_message = await context.bot.send_video(
                        chat_id=primary_admin_id,
                        video=file_to_send.file_id,
                        caption=replied_message.caption
                    )
                elif replied_message.audio:
                    sent_file_message = await context.bot.send_audio(
                        chat_id=primary_admin_id,
                        audio=file_to_send.file_id,
                        caption=replied_message.caption
                    )
                else:
                    # Should not happen due to the check above, but as a fallback
                    await send_and_delete_message(context, update.effective_chat.id, "Unsupported file type for indexing. 😔")
                    return

                # Now send the approval instructions
                approval_caption = (
                    f"**File Index Request**\n\n"
                    f"**From User:** {requester_mention}\n"
                    f"**User ID:** `{user.id}`\n\n"
                    "Reply to the file above with `/done` to index it, or `/cancel` to reject it."
                )

                await context.bot.send_message(
                    chat_id=primary_admin_id,
                    text=approval_caption,
                    parse_mode="HTML"
                )
                await send_and_delete_message(context, update.effective_chat.id, "✅ Your request to index this file has been sent to the admin for approval. 👍")
            except TelegramError as e:
                logger.error(f"Failed to send file for indexing approval: {e}")
                await send_and_delete_message(context, update.effective_chat.id, "😥 Could not send the file to the admin for approval. Please try again later. 🙏")
        else:
            await send_and_delete_message(context, update.effective_chat.id, "🤷‍♀️ No admin configured to approve requests. 🤷‍♂️")
        return

    # Original workflow for requesting a channel index
    if not context.args:
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            "**💡 Usage:**\n"
            "1. Reply to a file with `/request_index` to request it to be indexed. ✍️\n"
            "2. Use `/request_index <channel_link>` to request a channel to be indexed. 📣",
            parse_mode="Markdown"
        )
        return

    request_text = " ".join(context.args)
    log_message = (
        f"🙏 **New Channel Index Request** 🙏\n\n"
        f"**From User:** {user.mention_html()}\n"
        f"**User ID:** `{user.id}`\n"
        f"**Username:** @{user.username or 'N/A'}\n\n"
        f"**Channel to Index:**\n`{request_text}`"
    )

    try:
        await context.bot.send_message(chat_id=LOG_CHANNEL, text=log_message, parse_mode="HTML")
        confirmation_text = f"✅ {user.mention_html()}, your request to index the channel has been sent to the admins. 📬"
        await send_and_delete_message(context, update.effective_chat.id, confirmation_text, parse_mode="HTML")
    except TelegramError as e:
        logger.error(f"Failed to process /request_index command for a channel: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "😥 Sorry, there was an error sending your request. 😥")

async def refer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /refer command to get a referral link."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "🚫 You are banned from using this bot. 🚫")
        return

    user_id = update.effective_user.id
    bot_username = context.bot.username
    referral_link = f"https://t.me/{bot_username}?start=ref_{user_id}"

    if referrals_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "😥 The referral system is currently unavailable. Please try again later. 🙏")
        return

    try:
        user_referral_data = referrals_col.find_one({"_id": user_id})
        referral_count = user_referral_data.get("referral_count", 0) if user_referral_data else 0

        referral_message = (
            "**🎁 Earn Free Premium Access! 🎁**\n\n"
            "Share your unique referral link with your friends. 📲 For every 10 users who join using your link, you'll receive **1 month of premium access** (no ads, no verification)! 🥳\n\n"
            f"**Your Referral Link:**\n`{referral_link}`\n\n"
            f"**Your Current Referral Count:** {referral_count}/10 📈"
        )
        await send_and_delete_message(context, update.effective_chat.id, referral_message, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error in /refer command for user {user_id}: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "🆘 An error occurred while fetching your referral data. 🆘")


async def request_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /request command for users to request files."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, update.effective_chat.id, "🚫 You are banned from using this bot. 🚫")
        return

    user = update.effective_user
    if not context.args:
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            "🤔 Please provide a movie or file name to request. 🤔\n\nUsage: `/request <name>`",
            parse_mode="Markdown"
        )
        return

    request_text = " ".join(context.args)

    # Format the message for the log channel
    log_message = (
        f"🙏 **New Request** 🙏\n\n"
        f"**From User:** {user.mention_html()}\n"
        f"**User ID:** `{user.id}`\n"
        f"**Username:** @{user.username or 'N/A'}\n\n"
        f"**Request:**\n`{request_text}`"
    )

    try:
        # Forward the request to the log channel
        await context.bot.send_message(
            chat_id=LOG_CHANNEL,
            text=log_message,
            parse_mode="HTML"
        )
        # Confirm to the user
        confirmation_text = f"✅ {user.mention_html()}, your request has been sent to the admins. They will be notified! 📬"
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            confirmation_text,
            parse_mode="HTML"
        )
    except TelegramError as e:
        logger.error(f"Failed to process /request command: {e}")
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            "😥 Sorry, there was an error sending your request. Please try again later. 🙏"
        )

async def dl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /dl command to generate a direct download link."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return

    user = update.effective_user
    if await is_banned(user.id):
        await send_and_delete_message(context, update.effective_chat.id, "🚫 You are banned from using this bot. 🚫")
        return

    if user.id not in ADMINS:
        if not await check_member_status(user.id, context):
            buttons = [[InlineKeyboardButton(f"Join {ch['name']}", url=ch['link'])] for ch in PROMO_CHANNELS]
            keyboard = InlineKeyboardMarkup(buttons)
            await send_and_delete_message(context, update.effective_chat.id, "❗️ You must join ALL our channels to use this bot! ❗️", reply_markup=keyboard)
            return

    replied_message = update.message.reply_to_message
    if not replied_message or not (replied_message.document or replied_message.video or replied_message.audio):
        await send_and_delete_message(context, update.effective_chat.id, "🤔 Please reply to a file with `/dl` to get its download link. 🤔")
        return

    try:
        file_to_dl = replied_message.document or replied_message.video or replied_message.audio
        file_obj = await context.bot.get_file(file_to_dl.file_id)

        # Correctly construct the full download URL
        download_link = f"https://api.telegram.org/file/bot{context.bot.token}/{file_obj.file_path}"

        file_name = file_to_dl.file_name or "your file"

        response_text = (
            f"✅ Here is the direct download link for **{html.escape(file_name)}**:\n\n"
            f"🔗 `{download_link}`\n\n"
            "**Note:** This link is temporary and will expire in about 1 hour. ⏳"
        )

        await send_and_delete_message(context, update.effective_chat.id, response_text, parse_mode="HTML")

    except TelegramError as e:
        logger.error(f"Error in /dl command: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "😥 Could not generate a download link for this file. 😥")


async def connect_to_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Allows a user to send a message to the admin."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return

    user = update.effective_user
    if not context.args:
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            "🤔 Please provide a message to send to the admin. 🤔\n\nUsage: `/connect_to_admin <your message>`",
            auto_delete=False
        )
        return

    if not ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "😥 Sorry, the admin is not configured. I can't deliver your message. 😥", auto_delete=False)
        return

    message_to_admin = " ".join(context.args)
    admin_id = ADMINS[0] # Send to the primary admin

    # Format the message for the admin, embedding the user's ID for easy replies
    forward_message = (
        f"📩 **New Message from User** 📩\n\n"
        f"**From:** {user.mention_html()}\n"
        f"**User ID for Reply:** <code>{user.id}</code>\n\n"  # For the native reply handler
        f"<b>Message:</b>\n{html.escape(message_to_admin)}\n\n"
        f"👇 **To reply, you can also click the command below to copy it:**\n"
        f"<code>/usm {user.id} Your message here...</code>"
    )

    try:
        await context.bot.send_message(
            chat_id=admin_id,
            text=forward_message,
            parse_mode="HTML"
        )
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            "✅ Your message has been sent to the admin. They will reply to you here if needed. 👍",
            auto_delete=False
        )
    except TelegramError as e:
        logger.error(f"Failed to send message to admin {admin_id}: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "😥 Sorry, there was an error sending your message. Please try again later. 🙏", auto_delete=False)


async def usm_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to send a message to a specific user."""
    asyncio.create_task(react_to_message_task(update))
    if update.effective_user.id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑", auto_delete=False)
        return

    if len(context.args) < 2:
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            "🤔 <b>Invalid format!</b> Please use: <code>/usm &lt;user_id&gt; &lt;your_message&gt;</code>",
            parse_mode="HTML",
            auto_delete=False
        )
        return

    try:
        user_id_to_message = int(context.args[0])
        message_to_send = " ".join(context.args[1:])

        # Format the message for the user
        formatted_message = (
            f"📨 <b>A Message from the Admin</b> 📨\n\n"
            f"Hello there! 👋 The admin has sent you a message:\n\n"
            f"<blockquote>{html.escape(message_to_send)}</blockquote>\n\n"
            f"Thank you for being a part of our community! 🙏"
        )

        await context.bot.send_message(
            chat_id=user_id_to_message,
            text=formatted_message,
            parse_mode="HTML"
        )

        await send_and_delete_message(
            context,
            update.effective_chat.id,
            f"✅ Your message has been sent to user <code>{user_id_to_message}</code> successfully! 👍",
            auto_delete=False
        )

    except ValueError:
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            "❌ <b>Invalid User ID!</b> Please provide a valid numerical User ID. ❌",
            parse_mode="HTML",
            auto_delete=False
        )
    except TelegramError as e:
        logger.error(f"Failed to send message via /usm to {context.args[0]}: {e}")
        await send_and_delete_message(
            context,
            update.effective_chat.id,
            f"😥 <b>Failed to send message!</b> The user might have blocked the bot, or the ID is incorrect. Error: <code>{e}</code>",
            auto_delete=False
        )


async def log_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to show recent error logs."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    # Retrieve all logs from the in-memory stream
    log_stream.seek(0)
    logs = log_stream.readlines()

    # Filter for ERROR and CRITICAL logs and get the last 20
    error_logs = [log.strip() for log in logs if "ERROR" in log or "CRITICAL" in log]
    recent_errors = error_logs[-20:]

    if not recent_errors:
        await send_and_delete_message(context, update.effective_chat.id, "✅ No recent errors found in the logs. ✨")
    else:
        log_text = "```\n📜 Recent Error Logs: 📜\n\n" + "\n".join(recent_errors) + "\n```"
        await send_and_delete_message(context, update.effective_chat.id, log_text, parse_mode="MarkdownV2")

    # Clear the log buffer to prevent it from growing too large
    log_stream.seek(0)
    log_stream.truncate(0)


async def total_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to get the total number of users."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    if users_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "😥 Database not connected. 😥")
        return

    try:
        user_count = users_col.count_documents({})
        await send_and_delete_message(context, update.effective_chat.id, f"👥 **Total Users:** {user_count} 👥")
    except Exception as e:
        logger.error(f"Error getting user count: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "🆘 Failed to retrieve user count. Please check the database connection. 🆘")


async def total_files_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to get the total number of files."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    if files_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "😥 Database not connected. 😥")
        return

    try:
        # NOTE: This only gives the count from the CURRENT active database.
        file_count = files_col.count_documents({})
        await send_and_delete_message(context, update.effective_chat.id, f"🗃️ **Total Files (Current DB):** {file_count} 🗃️")
    except Exception as e:
        logger.error(f"Error getting file count: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "🆘 Failed to retrieve file count. Please check the database connection. 🆘")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to get bot statistics, including per-URI file counts. (MODIFIED)"""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    await send_and_delete_message(context, update.effective_chat.id, "🔄 Collecting statistics, please wait... 🔄")

    user_count = 0
    total_file_count_all_db = 0 # Accumulator for total files across all URIs
    uri_stats = {}

    try:
        # 1. Get Total Users (from the currently connected DB)
        if users_col is not None:
            user_count = users_col.count_documents({})

        # 2. Get File Counts per URI and total file count
        for idx, uri in enumerate(MONGO_URIS):
            client = mongo_clients.get(uri)
            if not client:
                uri_stats[idx] = "❌ Not connected"
                continue
            try:
                # Use the existing client from the pool
                temp_db = client["telegram_files"]
                temp_files_col = temp_db["files"]
                # Get file count
                file_count = temp_files_col.estimated_document_count()

                # Get DB stats
                db_stats = temp_db.command('dbStats', 1)
                used_storage_mib = db_stats.get('dataSize', 0) / (1024 * 1024)
                total_storage_mib = db_stats.get('storageSize', 0) / (1024 * 1024)
                free_storage_mib = total_storage_mib - used_storage_mib

                uri_stats[idx] = (
                    f"✅ {file_count} files\n"
                    f"     ★ 𝚄𝚂𝙴𝙳 𝚂𝚃𝙾𝚁𝙰𝙶𝙴: <code>{used_storage_mib:.2f}</code> 𝙼𝚒𝙱\n"
                    f"     ★ 𝙵𝚁𝙴𝙴 𝚂𝚃𝙾𝚁𝙰𝙶𝙴: <code>{free_storage_mib:.2f}</code> 𝙼𝚒𝙱"
                )
                total_file_count_all_db += file_count # Accumulate count
            except Exception as e:
                logger.warning(f"Failed to connect or get file count for URI #{idx + 1}: {e}")
                uri_stats[idx] = "❌ Failed to read"

        # 3. Format the output message
        stats_message = (
            f"📊 <b>Bot Statistics</b> 📊\n"
            f"  • Total Users: {user_count}\n"
            f"  • Total Connected Groups: {len(JOIN_CHECK_CHANNEL)}\n" # Using the count of JOIN_CHECK_CHANNEL
            f"  • Total Files (All DB): {total_file_count_all_db}\n" # Total count from all URIs
            f"  • <b>Total MongoDB URIs:</b> {len(MONGO_URIS)}\n"
            f"  • <b>Current Active URI:</b> #{current_uri_index + 1}\n\n"
            f"<b>File Count per URI:</b>\n"
        )
        for idx, status in uri_stats.items():
            stats_message += f"  • URI #{idx + 1}: {status}\n"

        await send_and_delete_message(context, update.effective_chat.id, stats_message, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Error getting bot stats: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "🆘 Failed to retrieve statistics. Please check the database connection. 🆘")


async def delete_file_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to delete a file by its MongoDB ID."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    if files_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "😥 Database not connected. 😥")
        return

    if not context.args:
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /deletefile <MongoDB_ID>\nTip: Use /findfile <filename> to get the ID.")
        return

    try:
        file_id = context.args[0]
        # NOTE: This only deletes from the *current* active database.
        result = files_col.delete_one({"_id": ObjectId(file_id)})

        if result.deleted_count == 1:
            await send_and_delete_message(context, update.effective_chat.id, f"✅ File with ID `{file_id}` has been deleted from the database. 🗑️")
        else:
            await send_and_delete_message(context, update.effective_chat.id, f"🤷‍♀️ File with ID `{file_id}` not found in the database. 🤷‍♂️")
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "🤔 Invalid ID or an error occurred. Please provide a valid MongoDB ID. 🤔")


async def find_file_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to find a file by its name and show its ID. Searches ALL URIs."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    if not context.args:
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /findfile <filename>")
        return

    query_filename = " ".join(context.args)
    all_results = []

    await send_and_delete_message(context, update.effective_chat.id, f"🔎 Searching all {len(MONGO_URIS)} databases for `{query_filename}`... 🔎")

    # Iterate through all URIs
    for idx, uri in enumerate(MONGO_URIS):
        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            temp_db = client["telegram_files"]
            temp_files_col = temp_db["files"]

            # Use regex for case-insensitive search
            results = list(temp_files_col.find({"file_name": {"$regex": query_filename, "$options": "i"}}))
            all_results.extend(results)
            logger.info(f"Found {len(results)} files in URI #{idx + 1}")
        except Exception as e:
            logger.error(f"Error finding file on URI #{idx + 1}: {e}")


    if not all_results:
        await send_and_delete_message(context, update.effective_chat.id, f"🤷‍♀️ No files found with the name `{query_filename}` in any database. 🤷‍♂️")
        return

    response_text = f"📁 Found {len(all_results)} files matching `{query_filename}` across all databases:\n\n"
    for idx, file in enumerate(all_results):
        response_text += f"{idx + 1}. *{escape_markdown(file['file_name'])}*\n  `ID: {file['_id']}`\n\n"

    response_text += "Copy the ID of the file you want to delete and use the command:\n`/deletefile <ID>`\n\nNote: `/deletefile` only works on the currently *active* database. If the file is not found, you may need to manually update the `current_uri_index` and restart."

    await send_and_delete_message(context, update.effective_chat.id, response_text, parse_mode="Markdown")


async def recent_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to show the 20 most recently uploaded files."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    if files_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "😥 Database not connected. 😥")
        return

    try:
        # Fetch the last 20 documents, sorting by ObjectId which is chronological
        recent_files = list(files_col.find().sort("_id", -1).limit(20))

        if not recent_files:
            await send_and_delete_message(context, update.effective_chat.id, "🤷‍♀️ No files found in the database. 🤷‍♂️")
            return

        response_text = "📁 <b>Last 20 Uploaded Files:</b> ✨\n\n"
        for idx, file in enumerate(recent_files, start=1):
            file_name_escaped = html.escape(file['file_name'])
            response_text += f"{idx}. <code>{file_name_escaped}</code>\n"

        await send_and_delete_message(context, update.effective_chat.id, response_text, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Error fetching recent files: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "🆘 An error occurred while fetching recent files. 🆘")


async def delete_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to delete all files from the database."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    if files_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "😥 Database not connected. 😥")
        return

    try:
        # NOTE: This only deletes from the *current* active database.
        result = files_col.delete_many({})
        await send_and_delete_message(context, update.effective_chat.id, f"✅ Deleted {result.deleted_count} files from the **current** database. 💥")
    except Exception as e:
        logger.error(f"Error deleting all files: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "🆘 An error occurred while trying to delete all files from the current database. 🆘")


async def ban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to ban a user by their user ID."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    if not context.args or not context.args[0].isdigit():
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /ban <user_id>")
        return

    user_to_ban_id = int(context.args[0])
    if user_to_ban_id in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🤨 Cannot ban an admin. 🤨")
        return

    if banned_users_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "😥 Database not connected. 😥")
        return

    try:
        banned_users_col.update_one(
            {"_id": user_to_ban_id},
            {"$set": {"_id": user_to_ban_id}},
            upsert=True
        )
        # Update cache
        banned_user_cache[user_to_ban_id] = True
        await send_and_delete_message(context, update.effective_chat.id, f"🔨 User `{user_to_ban_id}` has been banned. 🚫")
    except Exception as e:
        logger.error(f"Error banning user: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "🆘 An error occurred while trying to ban the user. 🆘")


async def freeforall_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to grant 12-hour premium access to all users."""
    asyncio.create_task(react_to_message_task(update))
    if update.effective_user.id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    if users_col is None or referrals_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "😥 Database not connected. 😥")
        return

    await send_and_delete_message(context, update.effective_chat.id, " granting 12-hour premium access to all users... 🥳")

    users_cursor = users_col.find({}, {"_id": 1})
    user_ids = [user["_id"] for user in users_cursor]

    for user_id in user_ids:
        try:
            referrals_col.update_one(
                {"_id": user_id},
                {"$set": {"premium_until": datetime.datetime.utcnow() + datetime.timedelta(hours=12)}},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Failed to grant premium to user {user_id}: {e}")

    await send_and_delete_message(context, update.effective_chat.id, f"✅ Premium access granted to {len(user_ids)} users for 12 hours. Notifying users... 📬")

    broadcast_text = "🎉 You have been granted 12 hours of free premium access! 🥳"
    for user_id in user_ids:
        try:
            await context.bot.send_message(chat_id=user_id, text=broadcast_text)
            await asyncio.sleep(0.1)  # Avoid rate limiting
        except Exception as e:
            logger.warning(f"Could not send premium notification to user {user_id}: {e}")

    await send_and_delete_message(context, update.effective_chat.id, "✅ All users have been notified. 👍")


async def unban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to unban a user by their user ID."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    if not context.args or not context.args[0].isdigit():
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /unban <user_id>")
        return

    user_to_unban_id = int(context.args[0])

    if banned_users_col is None:
        await send_and_delete_message(context, update.effective_chat.id, "😥 Database not connected. 😥")
        return

    try:
        result = banned_users_col.delete_one({"_id": user_to_unban_id})

        if result.deleted_count == 1:
            # Update cache
            banned_user_cache[user_to_unban_id] = False
            await send_and_delete_message(context, update.effective_chat.id, f"✅ User `{user_to_unban_id}` has been unbanned. 🤗")
        else:
            await send_and_delete_message(context, update.effective_chat.id, f"🤷‍♀️ User `{user_to_unban_id}` was not found in the banned list. 🤷‍♂️")
    except Exception as e:
        logger.error(f"Error unbanning user: {e}")
        await send_and_delete_message(context, update.effective_chat.id, "🆘 An error occurred while trying to unban the user. 🆘")


async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Broadcasts a message to all users in the database.
    Usage: /broadcast <message>
    """
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    if not context.args:
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /broadcast <message>")
        return

    # To preserve original spacing, we get the text after the command entity.
    # The CommandHandler ensures the first entity is the command.
    command_entity = update.message.entities[0]
    broadcast_text = update.message.text[command_entity.length:].lstrip()

    # NOTE: This only broadcasts to users in the *current* active database's users_col.
    # To broadcast to ALL users, you'd need to query all URIs for user IDs.
    users_cursor = users_col.find({}, {"_id": 1})
    user_ids = [user["_id"] for user in users_cursor]
    sent_count = 0
    failed_count = 0

    await send_and_delete_message(context, update.effective_chat.id, f"🚀 Starting broadcast to {len(user_ids)} users... 🚀", auto_delete=False)

    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=broadcast_text)
            sent_count += 1
            await asyncio.sleep(0.1)
        except TelegramError as e:
            failed_count += 1
            logger.error(f"Failed to send broadcast to user {uid}: {e}")
        except Exception as e:
            failed_count += 1
            logger.error(f"Unknown error sending broadcast to user {uid}: {e}")

    await send_and_delete_message(context, update.effective_chat.id, f"✅ Broadcast complete! ✅\n\nSent to: {sent_count} 📬\nFailed: {failed_count} 😥", auto_delete=False)


async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to restart the bot."""
    asyncio.create_task(react_to_message_task(update))
    if update.effective_user.id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    await send_and_delete_message(context, update.effective_chat.id, "⏳ Restarting bot... ⏳")

    # Shut down the web server gracefully
    if "server" in context.bot_data:
        context.bot_data["server"].shutdown()

    # Using os.execl to restart the bot. This will replace the current process.
    os.execl(sys.executable, sys.executable, *sys.argv, "--restarted")


async def grp_broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to broadcast a message to all connected groups where the bot is an admin."""
    asyncio.create_task(react_to_message_task(update))
    if not await bot_can_respond(update, context):
        return
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    if not context.args:
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /grp_broadcast <message>")
        return

    # To preserve original spacing, we get the text after the command entity.
    command_entity = update.message.entities[0]
    broadcast_text = update.message.text[command_entity.length:].lstrip()

    # Fetch all unique group IDs from all configured groups databases
    all_group_ids = set()
    logger.info("Fetching all group IDs for group broadcast from all group DBs...")
    for uri in GROUPS_DB_URIS:
        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            temp_db = client["telegram_groups"]
            temp_groups_col = temp_db["groups"]

            group_docs = temp_groups_col.find({}, {"_id": 1})
            for doc in group_docs:
                all_group_ids.add(doc['_id'])
        except Exception as e:
            logger.error(f"Failed to fetch group IDs from ...{uri[-20:]}: {e}")

    if not all_group_ids:
        await send_and_delete_message(context, update.effective_chat.id, "🤷‍♀️ No groups found in the database to broadcast to. 🤷‍♂️")
        return

    # Send message to each group
    sent_count = 0
    failed_count = 0
    await send_and_delete_message(context, update.effective_chat.id, f"🚀 Starting group broadcast to {len(all_group_ids)} groups... 🚀", auto_delete=False)

    for group_id in all_group_ids:
        try:
            # Check for admin status before sending to be safe
            member = await context.bot.get_chat_member(group_id, context.bot.id)
            if member.status in ["administrator", "creator"]:
                await context.bot.send_message(chat_id=group_id, text=broadcast_text)
                sent_count += 1
                logger.info(f"Group broadcast sent to group {group_id}")
            else:
                logger.warning(f"Skipping broadcast to group {group_id}, bot is no longer an admin.")
                failed_count += 1
            await asyncio.sleep(0.1)  # Rate limiting
        except TelegramError as e:
            logger.error(f"Failed to send broadcast to group {group_id}: {e}")
            failed_count += 1

    await send_and_delete_message(context, update.effective_chat.id, f"✅ Group broadcast complete! ✅\n\nSent to: {sent_count} groups 📬\nFailed: {failed_count} groups 😥", auto_delete=False)


async def done_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to approve and index a user-submitted file."""
    asyncio.create_task(react_to_message_task(update))
    user = update.effective_user
    if user.id not in ADMINS:
        return # Silently ignore from non-admins

    replied_message = update.message.reply_to_message
    if not replied_message or not (replied_message.document or replied_message.video or replied_message.audio):
        await send_and_delete_message(context, update.effective_chat.id, "🤔 You must reply to a file message to use this command. 🤔")
        return

    # The replied message is the one to be indexed.
    try:
        forwarded_message = await replied_message.forward(DB_CHANNEL)

        file = forwarded_message.document or forwarded_message.video or forwarded_message.audio
        if file:
            if forwarded_message.caption:
                raw_name = forwarded_message.caption
            else:
                raw_name = getattr(file, "file_name", None) or getattr(file, "title", None) or file.file_unique_id

            clean_name = sanitize_text(raw_name) if raw_name else "Unknown"

            saved = False
            for i in range(len(MONGO_URIS)):
                idx = (current_uri_index + i) % len(MONGO_URIS)
                uri_to_try = MONGO_URIS[idx]
                client = mongo_clients.get(uri_to_try)
                if not client: continue
                try:
                    temp_db = client["telegram_files"]
                    temp_files_col = temp_db["files"]
                    temp_files_col.insert_one({
                        "file_name": clean_name,
                        "file_id": forwarded_message.message_id,
                        "channel_id": DB_CHANNEL,
                        "file_size": file.file_size,
                    })
                    saved = True
                    break
                except Exception as e:
                    logger.error(f"DB Error while indexing from /done command: {e}")

            if saved:
                await send_and_delete_message(context, update.effective_chat.id, f"✅ **Indexed:** {clean_name} 🎉")
            else:
                await send_and_delete_message(context, update.effective_chat.id, "🆘 **Failed:** Could not save the file to any database. 🆘")
        else:
            await send_and_delete_message(context, update.effective_chat.id, "😥 **Failed:** The replied message does not contain a valid file. 😥")
    except Exception as e:
        logger.error(f"Error during /done command: {e}")
        await send_and_delete_message(context, update.effective_chat.id, f"🆘 **Error:** An unexpected error occurred.\n`{e}` 🆘")

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to reject a user-submitted file."""
    asyncio.create_task(react_to_message_task(update))
    user = update.effective_user
    if user.id not in ADMINS:
        return # Silently ignore from non-admins

    replied_message = update.message.reply_to_message
    if not replied_message:
        await send_and_delete_message(context, update.effective_chat.id, "🤔 You must reply to a message to use this command. 🤔")
        return

    try:
        await replied_message.delete()
        await update.message.delete()
    except TelegramError as e:
        logger.warning(f"Could not delete messages on /cancel: {e}")

    await send_and_delete_message(context, update.effective_chat.id, "❌ Request cancelled. ❌")


async def index_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to index files from a given channel."""
    asyncio.create_task(react_to_message_task(update))
    if update.effective_user.id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return

    if len(context.args) < 1:
        await send_and_delete_message(context, update.effective_chat.id, "Usage: /index_channel <channel_id> [skip_messages]")
        return

    try:
        channel_id = int(context.args[0])
    except ValueError:
        await send_and_delete_message(context, update.effective_chat.id, "🤔 Invalid Channel ID. It should be a number. 🤔")
        return

    skip_messages = 0
    if len(context.args) > 1:
        try:
            skip_messages = int(context.args[1])
        except ValueError:
            await send_and_delete_message(context, update.effective_chat.id, "🤔 Invalid skip count. It should be a number. 🤔")
            return

    # Schedule the indexing task to run in the background
    asyncio.create_task(index_channel_task(context, channel_id, skip_messages, update.effective_chat.id))
    await send_and_delete_message(context, update.effective_chat.id, "✅ Indexing has started in the background. I will notify you when it's complete. ⏳")

async def pm_on_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to enable PM search for all users."""
    global PM_SEARCH_ENABLED
    asyncio.create_task(react_to_message_task(update))
    if update.effective_user.id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return
    PM_SEARCH_ENABLED = True
    await send_and_delete_message(context, update.effective_chat.id, "✅ Private message search has been enabled for all users. 👥")


async def pm_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to disable PM search for all users."""
    global PM_SEARCH_ENABLED
    asyncio.create_task(react_to_message_task(update))
    if update.effective_user.id not in ADMINS:
        await send_and_delete_message(context, update.effective_chat.id, "🛑 You do not have permission to use this command. 🛑")
        return
    PM_SEARCH_ENABLED = False
    await send_and_delete_message(context, update.effective_chat.id, "✅ Private message search has been disabled for all users. 🚫")


async def index_channel_task(context: ContextTypes.DEFAULT_TYPE, channel_id: int, skip: int, user_chat_id: int):
    """Background task to handle channel indexing."""
    last_message_id = 0
    try:
        # A bit of a hack to get the last message ID
        temp_msg = await context.bot.send_message(chat_id=channel_id, text=".")
        last_message_id = temp_msg.message_id
        await context.bot.delete_message(chat_id=channel_id, message_id=last_message_id)
    except Exception as e:
        logger.error(f"Could not get last message ID for channel {channel_id}: {e}")
        await send_and_delete_message(context, user_chat_id, f"😥 Failed to access channel {channel_id}. Make sure the bot is an admin there. 🙏")
        return

    indexed_count = 0
    for i in range(skip + 1, last_message_id):
        forwarded_message = None
        try:
            # Forward the message to the DB_CHANNEL to get a message object with file attributes
            forwarded_message = await context.bot.forward_message(
                chat_id=DB_CHANNEL,
                from_chat_id=channel_id,
                message_id=i
            )

            file = forwarded_message.document or forwarded_message.video or forwarded_message.audio
            if not file:
                continue

            # Get filename (note: original caption is lost on forward)
            raw_name = getattr(file, "file_name", None) or getattr(file, "title", None) or file.file_unique_id
            clean_name = sanitize_text(raw_name) if raw_name else "Unknown"

            # Save metadata to all file databases for redundancy
            saved_to_any_db = False
            for uri in MONGO_URIS:
                client = mongo_clients.get(uri)
                if not client:
                    continue
                try:
                    temp_db = client["telegram_files"]
                    temp_files_col = temp_db["files"]
                    # THE CRITICAL FIX: Save original message_id and channel_id
                    temp_files_col.insert_one({
                        "file_name": clean_name,
                        "file_id": i, # Original message ID
                        "channel_id": channel_id, # Original channel ID
                        "file_size": file.file_size,
                    })
                    saved_to_any_db = True
                except Exception as e:
                    logger.error(f"DB Error while indexing for URI ...{uri[-20:]}: {e}")

            if saved_to_any_db:
                indexed_count += 1
                logger.info(f"Indexed message {i} from channel {channel_id}: {clean_name}")

            # Send progress update every 100 files
            if indexed_count > 0 and indexed_count % 100 == 0:
                await send_and_delete_message(context, user_chat_id, f"✍️ Progress: Indexed {indexed_count} files so far... ✍️")

        except TelegramError as e:
            logger.warning(f"Could not process message {i} from channel {channel_id}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while indexing message {i}: {e}")
        finally:
            # CRITICAL: Delete the temporary forwarded message to keep DB channel clean
            if forwarded_message:
                await context.bot.delete_message(chat_id=DB_CHANNEL, message_id=forwarded_message.message_id)

    await send_and_delete_message(context, user_chat_id, f"✅✅ Finished indexing channel {channel_id}. Total files indexed: {indexed_count}. 🎉")


async def on_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the bot being added to or removed from a group."""
    my_chat_member = update.my_chat_member

    # Check if the update is for a group/supergroup and if the bot is the one being updated
    if my_chat_member.chat.type in ["group", "supergroup"] and my_chat_member.new_chat_member.user.id == context.bot.id:
        group_id = my_chat_member.chat.id
        new_status = my_chat_member.new_chat_member.status
        old_status = my_chat_member.old_chat_member.status

        # If the bot was promoted to administrator or is the creator
        if new_status in ["administrator", "creator"]:
            logger.info(f"Bot was added/promoted as admin in group {group_id}. Saving to all groups databases.")
            for uri in GROUPS_DB_URIS:
                client = mongo_clients.get(uri)
                if not client:
                    continue
                try:
                    temp_db = client["telegram_groups"]
                    temp_groups_col = temp_db["groups"]
                    temp_groups_col.update_one({"_id": group_id}, {"$set": {"_id": group_id}}, upsert=True)
                    logger.info(f"Successfully saved/updated group {group_id} in groups DB at ...{uri[-20:]}.")
                except Exception as e:
                    logger.error(f"Failed to save group {group_id} to groups DB at ...{uri[-20:]}: {e}")

        # If the bot was kicked, left, or demoted from admin
        elif old_status in ["administrator", "creator"] and new_status not in ["administrator", "creator"]:
            logger.info(f"Bot was removed or demoted from admin in group {group_id}. Removing from all groups databases.")
            for uri in GROUPS_DB_URIS:
                client = mongo_clients.get(uri)
                if not client:
                    continue
                try:
                    temp_db = client["telegram_groups"]
                    temp_groups_col = temp_db["groups"]
                    temp_groups_col.delete_one({"_id": group_id})
                    logger.info(f"Successfully removed group {group_id} from groups DB at ...{uri[-20:]}: {e}")
                except Exception as e:
                    logger.error(f"Failed to remove group {group_id} from groups DB at ...{uri[-20:]}: {e}")


# ========================
# FILE/SEARCH HANDLERS
# ========================

async def save_file_from_pm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin sends file to bot -> save to channel + DB. Uses connection pooling."""
    asyncio.create_task(react_to_message_task(update))
    user_id = update.message.from_user.id
    if user_id not in ADMINS:
        return

    file = update.message.document or update.message.video or update.message.audio
    if not file:
        return

    # Forward to database channel
    forwarded = await update.message.forward(DB_CHANNEL)

    # Get filename from caption, then from file_name, replacing underscores, dots, and hyphens with spaces
    # Otherwise, use a default value
    if update.message.caption:
        raw_name = update.message.caption
    else:
        raw_name = getattr(file, "file_name", None) or getattr(file, "title", None) or file.file_unique_id

    clean_name = sanitize_text(raw_name) if raw_name else "Unknown"

    global current_uri_index, db, files_col, users_col, banned_users_col

    saved = False
    # Start the loop from the current active index and wrap around to try all
    for i in range(len(MONGO_URIS)):
        idx = (current_uri_index + i) % len(MONGO_URIS)
        uri_to_try = MONGO_URIS[idx]

        client = mongo_clients.get(uri_to_try)
        if not client:
            logger.warning(f"Skipping disconnected DB for file save: ...{uri_to_try[-20:]}")
            continue

        try:
            temp_db = client["telegram_files"]
            temp_files_col = temp_db["files"]

            # Try to save metadata
            temp_files_col.insert_one({
                "file_name": clean_name,
                "file_id": forwarded.message_id,
                "channel_id": forwarded.chat.id,
                "file_size": file.file_size,
            })

            # If successful and this is not the current active DB, switch to it.
            if idx != current_uri_index:
                current_uri_index = idx
                db = temp_db
                files_col = temp_db["files"]
                users_col = temp_db["users"]
                banned_users_col = temp_db["banned_users"]
                logger.info(f"Switched active MongoDB connection to index {current_uri_index}.")

            await send_and_delete_message(context, update.effective_chat.id, f"✅ Saved to DB #{idx + 1}: {clean_name} 🎉")
            saved = True
            break # Exit loop on success
        except Exception as e:
            logger.error(f"Error saving file with URI #{idx + 1}: {e}")
            if idx == current_uri_index and len(MONGO_URIS) > 1:
                 await send_and_delete_message(context, update.effective_chat.id, f"⚠️ Primary DB failed. Trying next available URI... 🙏")

    if not saved:
        logger.error("All MongoDB URIs have been tried and failed.")
        await send_and_delete_message(context, update.effective_chat.id, "🆘 Failed to save file on all available databases. 🆘")


async def save_file_from_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin sends file directly to channel -> save to DB. Uses connection pooling."""
    user_id = update.message.from_user.id
    chat_id = update.message.chat.id

    # Only process files from admins in the database channel
    if chat_id != DB_CHANNEL or user_id not in ADMINS:
        return

    file = update.message.document or update.message.video or update.message.audio
    if not file:
        return

    # Get filename from caption, then from file_name, replacing underscores, dots, and hyphens with spaces
    # Otherwise, use a default value
    if update.message.caption:
        raw_name = update.message.caption
    else:
        raw_name = getattr(file, "file_name", None) or getattr(file, "title", None) or file.file_unique_id

    clean_name = sanitize_text(raw_name) if raw_name else "Unknown"

    global current_uri_index, db, files_col, users_col, banned_users_col

    saved = False
    # Start the loop from the current active index and wrap around to try all
    for i in range(len(MONGO_URIS)):
        idx = (current_uri_index + i) % len(MONGO_URIS)
        uri_to_try = MONGO_URIS[idx]

        client = mongo_clients.get(uri_to_try)
        if not client:
            logger.warning(f"Skipping disconnected DB for channel file save: ...{uri_to_try[-20:]}")
            continue

        try:
            temp_db = client["telegram_files"]
            temp_files_col = temp_db["files"]

            # Try to save metadata
            temp_files_col.insert_one({
                "file_name": clean_name,
                "file_id": update.message.message_id,
                "channel_id": chat_id,
                "file_size": file.file_size,
            })

            # If successful and this is not the current active DB, switch to it.
            if idx != current_uri_index:
                current_uri_index = idx
                db = temp_db
                files_col = temp_db["files"]
                users_col = temp_db["users"]
                banned_users_col = temp_db["banned_users"]
                logger.info(f"Switched active MongoDB connection to index {current_uri_index}.")

            # Send **INSTANT** success notification to the admin
            try:
                await send_and_delete_message(
                    context,
                    user_id,
                    f"✅ File **`{escape_markdown(clean_name)}`** has been indexed successfully from the database channel to DB #{idx + 1}. 🎉",
                    parse_mode="MarkdownV2"
                )
            except TelegramError as e:
                logger.error(f"Failed to send notification to admin {user_id}: {e}")
            saved = True
            break

        except Exception as e:
            logger.error(f"Error saving file from channel with URI #{idx + 1}: {e}")
            if idx == current_uri_index and len(MONGO_URIS) > 1:
                try:
                    await send_and_delete_message(context, user_id, "⚠️ Primary DB failed. Trying next available URI... 🙏")
                except TelegramError:
                    pass

    if not saved:
        logger.error("All MongoDB URIs have been tried and failed.")
        try:
            await send_and_delete_message(context, user_id, "🆘 Failed to save file on all available databases. 🆘")
        except TelegramError:
            pass


async def search_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Search ALL URIs and show results, sorted by relevance.
    Uses a broad regex for initial filtering and fuzzy matching for accurate ranking.
    """
    if not await bot_can_respond(update, context):
        return

    # In private chat, only admins can search for files unless PM search is enabled.
    if update.effective_chat.type == "private" and update.effective_user.id not in ADMINS and not PM_SEARCH_ENABLED:
        await send_and_delete_message(context, update.effective_chat.id, "🤫 Use this bot on any group. Sorry, (only admin) 🤫")
        return

    if await is_banned(update.effective_user.id):
        await update.message.reply_text("🚫 You are banned from using this bot. 🚫")
        return

    # Add reaction to user's message in the background
    asyncio.create_task(react_to_message_task(update))

    await save_user_info(update.effective_user)
    if not await check_member_status(update.effective_user.id, context):
        # NEW: Updated to show buttons for all channels
        buttons = [[InlineKeyboardButton(f"Join {ch['name']}", url=ch['link'])] for ch in PROMO_CHANNELS]
        keyboard = InlineKeyboardMarkup(buttons)
        await send_and_delete_message(context, update.effective_chat.id, "❗️ You must join ALL our channels to use this bot! ❗️", reply_markup=keyboard)
        return

    # Send initial status message that will be edited with progress
    status_message = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="⏳ Searching... ⏳"
    )

    # We get the query text first, then we can delete the user's message.
    raw_query = update.message.text.strip()
    try:
        await update.message.delete()
    except TelegramError as e:
        logger.warning(f"Could not delete user's query message: {e}")
    # Sanitize and normalize query for better fuzzy search
    normalized_query = sanitize_text(raw_query)

    # Filter out stop words for more accurate searching
    query_words = normalized_query.lower().split()
    filtered_words = [word for word in query_words if word not in SEARCH_STOP_WORDS]
    search_query = " ".join(filtered_words)

    # Use the original normalized query if filtering results in an empty string
    if not search_query:
        search_query = normalized_query

    # Log the user's query
    user = update.effective_user
    log_text = f"🔍 User: {user.full_name} | @{user.username} | ID: {user.id}\nQuery: {raw_query}"
    try:
        await context.bot.send_message(LOG_CHANNEL, text=log_text)
    except Exception as e:
        logger.error(f"Failed to log query to channel: {e}")


    # --- REVISED SEARCH LOGIC (Broad Filtering + Fuzzy Ranking) ---

    # Split the query into words and escape them for a forgiving regex. Ignore short words.
    words = [re.escape(word) for word in search_query.split() if len(word) > 1]

    if not words:
        await send_and_delete_message(context, update.effective_chat.id, "🤔 Query too short or invalid. Please try a longer search term. 🤔")
        return

    # Create an AND condition using positive lookaheads.
    # This ensures that ALL words must be in the filename to be considered for fuzzy ranking.
    regex_pattern = re.compile("".join([f"(?=.*{word})" for word in words]), re.IGNORECASE)
    query_filter = {"file_name": {"$regex": regex_pattern}}

    preliminary_results = []

    # Iterate over ALL URIs for search
    total_dbs = len(MONGO_URIS)
    for idx, uri in enumerate(MONGO_URIS):
        try:
            await context.bot.edit_message_text(
                chat_id=status_message.chat.id,
                message_id=status_message.message_id,
                text=f"⏳ Searching... ({idx + 1}/{total_dbs} databases searched) ⏳"
            )
        except TelegramError:  # Ignore if message can't be edited
            pass

        client = mongo_clients.get(uri)
        if not client:
            continue
        try:
            # Use the existing client from the pool
            temp_db = client["telegram_files"]
            temp_files_col = temp_db["files"]

            # Query the database with the broad filter
            results = list(temp_files_col.find(query_filter))
            preliminary_results.extend(results)

        except Exception as e:
            logger.error(f"MongoDB search query failed on URI #{idx + 1}: {e}")

    # --- Fuzzy Ranking (to ensure the best match is first) ---

    if not preliminary_results:
        try:
            google_search_url = f"https://www.google.com/search?q={raw_query.replace(' ', '+')}"
            keyboard = InlineKeyboardMarkup(
                [[InlineKeyboardButton("Check Spelling on Google", url=google_search_url)]]
            )
            no_results_text = (
                f"🤷‍♀️ No files found for your query: <b>{html.escape(raw_query)}</b> 🤷‍♂️\n\n"
                "This might be due to a spelling mistake. You can use the button below to double-check on Google. 👇"
            )
            edited_message = await context.bot.edit_message_text(
                chat_id=status_message.chat.id,
                message_id=status_message.message_id,
                text=no_results_text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            # Schedule the "no results" message for deletion
            asyncio.create_task(delete_message_after_delay(context, edited_message.chat.id, edited_message.message_id, 5 * 60))
        except TelegramError:
            pass # Ignore if message was deleted
        return

    results_with_score = []
    # Use a set to track file_id + channel_id tuples to ensure no duplicates from different DBs
    unique_files = set()

    for file in preliminary_results:
        file_key = (file.get('file_id'), file.get('channel_id'))
        if file_key in unique_files:
            continue

        # Check for an exact match first to prioritize it
        if search_query.lower() == file['file_name'].lower():
            score = 101 # Give a score higher than any possible fuzzy score
        else:
            # Use WRatio for a more robust score that handles partial strings and other variations well.
            score = fuzz.WRatio(search_query, file['file_name'])

        if score > 45:
            results_with_score.append((file, score))
            unique_files.add(file_key)

    # Sort the results by score in descending order
    sorted_results = sorted(results_with_score, key=lambda x: x[1], reverse=True)

    # Extract the file documents from the sorted list
    final_results = [result[0] for result in sorted_results]

    if not final_results:
        try:
            await context.bot.edit_message_text(
                chat_id=status_message.chat.id,
                message_id=status_message.message_id,
                text="🤷‍♀️ No relevant files found after filtering by relevance. For your query contact @kaustavhibot 🤷‍♂️"
            )
        except TelegramError:
            pass # Ignore if message was deleted
        return

    # Pass the full result list to the pagination function for consistency
    context.user_data['search_results'] = final_results
    context.user_data['search_query'] = raw_query

    # Edit the status message to show the results
    await send_results_page(
        chat_id=status_message.chat.id,
        results=final_results,
        page=0,
        context=context,
        query=raw_query,
        user_mention=user.mention_html(),
        message_id=status_message.message_id
    )


async def send_results_page(chat_id, results, page, context: ContextTypes.DEFAULT_TYPE, query: str, user_mention: str, message_id: int = None, reply_to_message_id: int = None):
    """Sends or edits a message to show a paginated list of search results."""
    start, end = page * 6, (page + 1) * 6
    page_results = results[start:end]
    bot_username = context.bot.username

    # Escape the query string for HTML
    escaped_query = html.escape(query)
    text = (
        f"Hey {user_mention}, here are the top {len(results)} results for: <b>{escaped_query}</b> 🎉\n"
        f"(Page {page + 1} / {math.ceil(len(results) / 6)}) (Sorted by Relevance)"
    )
    buttons = []

    # Add files for the current page
    for idx, file in enumerate(page_results, start=start + 1):
        file_size = format_size(file.get("file_size"))
        file_obj_id = str(file['_id'])

        # Escape filename for HTML
        file_name_escaped = html.escape(file['file_name'][:40])
        button_text = f"[{file_size}] {file_name_escaped}"

        # Create the deep link URL
        deep_link_url = f"https://t.me/{bot_username}?start=files_{file_obj_id}"

        buttons.append(
            [InlineKeyboardButton(button_text, url=deep_link_url)]
        )

    # Add the promotional text at the end
    text += "\n\nKaustav Ray                                                                                                      Join here: @filestore4u     @freemovie5u"

    # Add navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"page_{page-1}_{query}"))
    if end < len(results):
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"page_{page+1}_{query}"))

    if nav_buttons:
        buttons.append(nav_buttons)

    # Send All button
    buttons.append([InlineKeyboardButton("📨 Send All Files (Current Page)", callback_data=f"sendall_{page}_{query}")])

    reply_markup = InlineKeyboardMarkup(buttons)

    sent_message = None
    try:
        if message_id:
            sent_message = await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode="HTML"
            )
        else:
            # Send as a new message, replying if the ID is provided
            sent_message = await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode="HTML",
                reply_to_message_id=reply_to_message_id
            )

        if sent_message:
            # Schedule the search results for deletion
            asyncio.create_task(delete_message_after_delay(context, chat_id, sent_message.message_id, 5 * 60))

    except TelegramError as e:
        logger.error(f"Error sending or editing search results page: {e}")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button clicks."""
    asyncio.create_task(react_to_message_task(update))
    query = update.callback_query
    await query.answer()

    if await is_banned(update.effective_user.id):
        await send_and_delete_message(context, query.message.chat.id, "🚫 You are banned from using this bot. 🚫")
        return

    await save_user_info(update.effective_user)
    if not await check_member_status(update.effective_user.id, context):
        buttons = [[InlineKeyboardButton(f"Join {ch['name']}", url=ch['link'])] for ch in PROMO_CHANNELS]
        keyboard = InlineKeyboardMarkup(buttons)
        await send_and_delete_message(context, query.message.chat.id, "❗️ You must join ALL our channels to use this bot! ❗️", reply_markup=keyboard)
        return

    data = query.data
    user_id = query.from_user.id

    # --- Send All Files (Batch) ---
    if data.startswith("sendall_"):
        _, page_str, search_query = data.split("_", 2)
        page = int(page_str)
        final_results = context.user_data.get('search_results')
        if not final_results:
            await send_and_delete_message(context, user_id, "😥 Search session expired. Please search again. 🙏")
            return
        files_to_send = final_results[page * 6:(page + 1) * 6]
        if not files_to_send:
            await send_and_delete_message(context, user_id, "🤷‍♀️ No files found on this page to send. 🤷‍♂️")
            return

        asyncio.create_task(send_all_files_task(user_id, query.message.chat.id, context, files_to_send, query.from_user.mention_html()))

    # --- Other Button Logic (Pagination, Start Menu, etc.) ---
    elif data.startswith("page_"):
        _, page_str, search_query = data.split("_", 2)
        page = int(page_str)

        final_results = context.user_data.get('search_results')
        if not final_results:
            await query.answer("⚠️ Search results have expired. Please search again. ⚠️", show_alert=True)
            return

        await send_results_page(
            chat_id=query.message.chat.id,
            results=final_results,
            page=page,
            context=context,
            query=search_query,
            message_id=query.message.message_id,
            user_mention=query.from_user.mention_html()
        )

    elif data == "start_about":
        await query.message.delete()
        await info_command(update, context)

    elif data == "start_help":
        await query.message.delete()
        await help_command(update, context)

    elif data == "start_close":
        await query.message.delete()

    elif data == "no_owner":
        await query.answer("🤷‍♀️ Owner not configured. 🤷‍♂️", show_alert=True)


async def handle_admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles when an admin replies to a user's forwarded message."""
    user = update.effective_user
    message = update.effective_message

    # Condition 1: Is the message from an admin in a private chat?
    if user.id not in ADMINS or message.chat.type != 'private':
        return

    # Condition 2: Is it a reply to another message?
    replied_message = message.reply_to_message
    if not replied_message:
        return

    # Condition 3: Does the replied-to message contain the special "User ID for Reply:" text?
    if "User ID for Reply:" in replied_message.text:
        try:
            # Extract the user ID from the message text, accounting for the <code> tag
            user_id_match = re.search(r"User ID for Reply:\s*<code>(\d+)</code>", replied_message.text)
            if not user_id_match:
                return  # Could not find user ID in the expected format

            user_id_to_reply = int(user_id_match.group(1))
            admin_reply_text = message.text

            # Format the message to be sent to the user
            reply_to_user = (
                f"📨 **A Message from the Admin** 📨\n\n"
                f"Hello there! 👋 The admin has sent you a reply:\n\n"
                f"<blockquote>{html.escape(admin_reply_text)}</blockquote>\n\n"
                f"Thank you for reaching out! 🙏"
            )

            await context.bot.send_message(
                chat_id=user_id_to_reply,
                text=reply_to_user,
                parse_mode="HTML"
            )

            # Confirm to the admin that the reply was sent
            await send_and_delete_message(
                context,
                message.chat.id,
                "✅ Your reply has been sent to the user successfully. 👍",
                reply_to_message_id=message.message_id,
                auto_delete=False
            )

        except (IndexError, ValueError, TypeError) as e:
            logger.error(f"Error parsing user ID from admin reply: {e}")
        except TelegramError as e:
            logger.error(f"Failed to send admin reply to user: {e}")
            await send_and_delete_message(
                context,
                message.chat.id,
                f"😥 Failed to send reply. Error: <code>{html.escape(str(e))}</code>",
                reply_to_message_id=message.message_id,
                auto_delete=False,
                parse_mode="HTML"
            )


# ========================
# MAIN
# ========================

class ServerThread(Thread):
    def __init__(self, app):
        Thread.__init__(self)
        port = int(os.environ.get("PORT", 10000))
        self.srv = make_server('0.0.0.0', port, app)
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        logger.info('starting server')
        self.srv.serve_forever()

    def shutdown(self):
        self.srv.shutdown()

import argparse

async def main_async():
    """The main asynchronous entry point for the bot."""
    parser = argparse.ArgumentParser(description="Telegram Filter Bot")
    parser.add_argument("--token", help="The Telegram bot token to use.")
    args = parser.parse_args()

    # Use the token from the command line if provided, otherwise use the one from config
    token = args.token if args.token else BOT_TOKEN

    if not connect_to_mongo():
        logger.critical("Failed to connect to the initial MongoDB URI. Exiting.")
        return

    # Create the application instance with persistence
    persistence = PicklePersistence(filepath="bot_persistence.pickle")
    ptb_app = Application.builder().token(token).persistence(persistence).build()

    # Start the Flask web server in a background thread
    server = ServerThread(app)
    server.start()
    logger.info("Web server started in a background thread.")
    ptb_app.bot_data["server"] = server

    # Create TTL index for premium users (expires at the time specified in the 'premium_until' field)
    if referrals_col is not None:
        try:
            referrals_col.create_index("premium_until", expireAfterSeconds=0)
            logger.info("TTL index on 'referrals' collection for premium users ensured.")
        except PyMongoError as e:
            if e.code == 85: # IndexOptionsConflict
                logger.warning("TTL index for 'referrals' collection already exists with different options. Skipping.")
            else:
                logger.error(f"Could not create TTL index for referrals: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during TTL index creation for referrals: {e}")


    # Command Handlers
    ptb_app.add_handler(CommandHandler("start", start))
    ptb_app.add_handler(CommandHandler("help", help_command))
    ptb_app.add_handler(CommandHandler("info", info_command))
    ptb_app.add_handler(CommandHandler("rand", rand_command))
    ptb_app.add_handler(CommandHandler("refer", refer_command))
    ptb_app.add_handler(CommandHandler("dl", dl_command))
    ptb_app.add_handler(CommandHandler("connect_to_admin", connect_to_admin_command))
    ptb_app.add_handler(CommandHandler("request", request_command))
    ptb_app.add_handler(CommandHandler("request_index", request_index_command))
    ptb_app.add_handler(CommandHandler("done", done_command))
    ptb_app.add_handler(CommandHandler("cancel", cancel_command))
    ptb_app.add_handler(CommandHandler("log", log_command))
    ptb_app.add_handler(CommandHandler("total_users", total_users_command))
    ptb_app.add_handler(CommandHandler("total_files", total_files_command))
    ptb_app.add_handler(CommandHandler("stats", stats_command))
    ptb_app.add_handler(CommandHandler("deletefile", delete_file_command))
    ptb_app.add_handler(CommandHandler("findfile", find_file_command))
    ptb_app.add_handler(CommandHandler("recent", recent_command))
    ptb_app.add_handler(CommandHandler("deleteall", delete_all_command))
    ptb_app.add_handler(CommandHandler("ban", ban_user_command))
    ptb_app.add_handler(CommandHandler("unban", unban_user_command))
    ptb_app.add_handler(CommandHandler("freeforall", freeforall_command))
    ptb_app.add_handler(CommandHandler("broadcast", broadcast_message))
    ptb_app.add_handler(CommandHandler("grp_broadcast", grp_broadcast_command))
    ptb_app.add_handler(CommandHandler("restart", restart_command))
    ptb_app.add_handler(CommandHandler("usm", usm_command))
    ptb_app.add_handler(CommandHandler("index_channel", index_channel_command))
    ptb_app.add_handler(CommandHandler("pm_on", pm_on_command))
    ptb_app.add_handler(CommandHandler("pm_off", pm_off_command))

    # File and Message Handlers
    # Admin file upload via PM
    ptb_app.add_handler(MessageHandler(
        (filters.Document.ALL | filters.VIDEO | filters.AUDIO) & filters.ChatType.PRIVATE,
        save_file_from_pm
    ))

    # Admin file indexing via DB Channel
    ptb_app.add_handler(MessageHandler(
        (filters.Document.ALL | filters.VIDEO | filters.AUDIO) & filters.Chat(chat_id=DB_CHANNEL),
        save_file_from_channel
    ))

    # Text Search Handler (REVISED LOGIC)
    ptb_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_files))

    # Admin Reply Handler
    ptb_app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & filters.REPLY & ~filters.COMMAND,
        handle_admin_reply
    ))

    # Callback Query Handler (for buttons)
    ptb_app.add_handler(CallbackQueryHandler(button_handler))

    # Group tracking handler
    ptb_app.add_handler(ChatMemberHandler(on_chat_member_update))

    logger.info("Bot starting...")

    # Initialize and start the application
    await ptb_app.initialize()
    await ptb_app.start()
    await ptb_app.updater.start_polling(poll_interval=1, timeout=10, drop_pending_updates=True)

    # Keep the bot running indefinitely until a signal is received
    logger.info("Bot is running. Press Ctrl-C to stop.")


    # Broadcast restart message if the bot was restarted via the command
    if "--restarted" in sys.argv:
        logger.info("Bot was restarted, sending notification to all users.")
        if users_col is not None:
            users_cursor = users_col.find({}, {"_id": 1})
            user_ids = [user["_id"] for user in users_cursor]
            for user_id in user_ids:
                try:
                    await ptb_app.bot.send_message(chat_id=user_id, text="Bot has been restarted.")
                    await asyncio.sleep(0.1)  # Avoid rate limiting
                except Exception as e:
                    logger.warning(f"Could not send restart message to user {user_id}: {e}")
            logger.info("Finished sending restart notifications.")

    await asyncio.Future()  # This will run forever


if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot shut down initiated.")
    finally:
        # Gracefully close all MongoDB connections
        logger.info("Closing all database connections...")
        for client in mongo_clients.values():
            if client:
                client.close()
        logger.info("All database connections closed. Exiting.")
