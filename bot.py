"""
╔══════════════════════════════════════════════════════════════════════╗
║            ⚡ 𝐇𝐘𝐏𝐄𝐑-𝐗 𝐌𝐮𝐥𝐭𝐢-𝐁𝐨𝐭 𝐂𝐨𝐧𝐭𝐫𝐨𝐥𝐥𝐞𝐫 ⚡                  ║
╠══════════════════════════════════════════════════════════════════════╣
║  Multi-User | Each User Has Own Bot Space | HYPER-X v3.0 Powered     ║
╠══════════════════════════════════════════════════════════════════════╣
║  Credit: Dev 🎀👑😛 (@god_olds)                                       ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import os
import time
import random
import logging
import json
from typing import Dict, List, Set, Optional
from datetime import datetime
from pathlib import Path

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, 
    CallbackQueryHandler, ConversationHandler, filters, ContextTypes
)
from telegram.constants import ChatType, ParseMode
from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest, Forbidden

logging.basicConfig(
    format="%(asctime)s - [HYPER-X] - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("HyperX")

DATA_FILE = "bot_data.json"
CONTROLLER_TOKEN = "8492426300:AAEasavi51hrI8OqrbUQzkDmdf9OViSDS6c"

WAITING_TOKEN = 1
WAITING_OWNER_ID = 2
WAITING_NC_TEMPLATE = 3
WAITING_SPAM_TEMPLATE = 4
WAITING_BROADCAST = 5

HEART_EMOJIS = ['❤️', '🧡', '💛', '💚', '💙', '💜', '🤎', '🖤', '🤍', '💘', '💝', '💖', '💗', '💓', '💞', '💌', '💕', '💟', '♥️', '❣️', '💔']

DEFAULT_NC_MESSAGES = [
    "DEV TERA BAAP  🔥⃤⃟⃝🐦‍🔥『🚩』",
    "{target} TERI BHEN KA BHOSADA 🔥⃤⃟⃝🐦‍🔥『🚩』",
    "{target} TERI MAA DEV KE LUND PR 🔥⃤⃟⃝🐦‍🔥『🚩』",
    "{target} TERI MAA KA BHOSADA CHUDA 🔥⃤⃟⃝🐦‍🔥『🚩』",
    "{target} TERI CHUDAYI BY DEV PAPA 🔥⃤⃟⃝🐦‍🔥『🚩』",
    "{target} CVR LE RANDI KE BACCHE 🔥⃤⃟⃝🐦‍🔥『🚩』",
    "{target} TERI MAA RANDI 🔥⃤⃟⃝🐦‍🔥 『🚩』",
    "{target} TERI BHEN KAALI CHUT 🔥⃤⃟⃝🐦‍🔥『🚩』",
]

DEFAULT_REPLY_MESSAGES = [
    "{target} ---RDI🐣",
    "{target} चुद गया -!",
    "Aʟᴏᴏ Kʜᴀᴋᴇ {target} Kɪ Mᴀ Cʜᴏᴅ Dᴜɴɢᴀ!",
    "{target} Cʜᴜᴅᴀ🦖🪽",
    "{target} Bᴏʟᴇ ᴅᴇᴠ ᴘᴀᴘᴀ पिताश्री Mᴇʀɪ Mᴀ Cʜᴏᴅ Dᴏ",
    "{target} Kɪ Mᴀ Bᴏʟᴇ ᴅᴇᴠ ᴘᴀᴘᴀ Sᴇ Cʜᴜᴅᴜɴɢɪ",
    "{target} Kɪ Bᴇʜɴ Kɪ Cʜᴜᴛ Kᴀʟɪ Kᴀʟɪ",
    "{target} Kɪ Mᴀ Rᴀɴᴅɪ",
    "{target} ɢᴀʀᴇᴇʙ ᴋᴀ ʙᴀᴄʜʜᴀ",
    "{target} ᴄʜᴜᴅ ᴋᴇ ᴘᴀɢᴀʟ ʜᴏɢᴀʏᴀ",
]

DEFAULT_SPAM_MESSAGES = [
    "✝ 𝐀ɴᴛᴀ𝐑 𝐌ᴀɴ𝐓ᴀʀ 𝐒ʜᴀɪ𝐓ᴀɴ𝐈 𝐊ʜᴏ𝐏ᴀᴅ𝐀 {target} 𝐆ᴀ𝐑ɪ𝐁 𝐊ɪ 𝐀ᴍᴍ𝐈 𝐊ᴀ 𝐊ᴀʟ𝐀 𝐁ʜᴏs𝐃ᴀ",
    "{target} 𝙏𝙀𝙍𝙄 𝙈𝘼𝘼 𝘽𝘼𝙃𝘼𝙉 𝘿𝙊𝙉𝙊 𝙆𝙊 𝙍𝘼𝙉𝘿𝙄 𝙆𝙊 𝘾𝙃𝙊𝘿𝙐 🤣",
    "{target} 𝐓𝐄𝐑𝐈 𝐌𝐀𝐀_𝐁𝐀𝐇𝐀𝐍 𝐊𝐎 𝐂𝐇𝐎𝐃𝐔 𝐁𝐈𝐍𝐀 𝐂𝐎𝐍𝐃𝐎𝐌 𝐊𝐄 😝",
]

UNAUTHORIZED_MSG = "𝐃𝐄𝐕 𝐏𝐀𝐏𝐀 𝐒𝐄 𝐁𝐈𝐊𝐇 𝐌𝐀𝐍𝐆 🤣🎀😻"


def init_database():
    if not Path(DATA_FILE).exists():
        data = {
            "users": {},
            "bots": {},
            "bot_users": {},
            "nc_templates": {},
            "spam_templates": {},
            "reply_templates": {},
            "permissions": {},
            "next_bot_id": 1
        }
        save_data(data)
    logger.info("Data storage initialized!")


def load_data() -> dict:
    if Path(DATA_FILE).exists():
        with open(DATA_FILE, 'r') as f:
            return json.load(f)
    return {
        "users": {},
        "bots": {},
        "bot_users": {},
        "nc_templates": {},
        "spam_templates": {},
        "reply_templates": {},
        "permissions": {},
        "next_bot_id": 1
    }


def save_data(data: dict):
    with open(DATA_FILE, 'w') as f:
        json.dump(data, f, indent=2)


class Database:
    @staticmethod
    def register_user(user_id: int, username: str = None):
        data = load_data()
        user_id_str = str(user_id)
        if user_id_str not in data["users"]:
            data["users"][user_id_str] = {
                "user_id": user_id,
                "username": username,
                "is_sudo": False,
                "created_at": datetime.now().isoformat()
            }
        else:
            data["users"][user_id_str]["username"] = username
        save_data(data)

    @staticmethod
    def is_sudo(user_id: int) -> bool:
        data = load_data()
        user = data["users"].get(str(user_id))
        return user and user.get("is_sudo", False)

    @staticmethod
    def add_sudo(user_id: int) -> bool:
        data = load_data()
        user_id_str = str(user_id)
        if user_id_str not in data["users"]:
            data["users"][user_id_str] = {
                "user_id": user_id,
                "username": None,
                "is_sudo": True,
                "created_at": datetime.now().isoformat()
            }
        else:
            data["users"][user_id_str]["is_sudo"] = True
        save_data(data)
        return True

    @staticmethod
    def remove_sudo(user_id: int) -> bool:
        data = load_data()
        user_id_str = str(user_id)
        if user_id_str in data["users"]:
            data["users"][user_id_str]["is_sudo"] = False
            save_data(data)
            return True
        return False

    @staticmethod
    def get_sudos() -> List[int]:
        data = load_data()
        return [int(uid) for uid, user in data["users"].items() if user.get("is_sudo", False)]

    @staticmethod
    def add_bot(user_id: int, token: str, username: str = None, name: str = None) -> Optional[int]:
        data = load_data()
        token_exists = any(b.get("token") == token for b in data["bots"].values())
        if token_exists:
            return None
        
        bot_id = data["next_bot_id"]
        data["bots"][str(bot_id)] = {
            "id": bot_id,
            "user_id": user_id,
            "token": token,
            "bot_username": username,
            "bot_name": name,
            "is_running": False,
            "created_at": datetime.now().isoformat()
        }
        data["next_bot_id"] += 1
        save_data(data)
        return bot_id

    @staticmethod
    def remove_bot(user_id: int, bot_id: int) -> bool:
        data = load_data()
        bot_id_str = str(bot_id)
        if bot_id_str in data["bots"]:
            bot = data["bots"][bot_id_str]
            if bot["user_id"] == user_id:
                del data["bots"][bot_id_str]
                if bot_id_str in data["bot_users"]:
                    del data["bot_users"][bot_id_str]
                save_data(data)
                return True
        return False

    @staticmethod
    def get_user_bots(user_id: int) -> List[dict]:
        data = load_data()
        bots = [b for b in data["bots"].values() if b["user_id"] == user_id]
        return sorted(bots, key=lambda x: x["id"])

    @staticmethod
    def get_bot(bot_id: int) -> Optional[dict]:
        data = load_data()
        bot_id_str = str(bot_id)
        return data["bots"].get(bot_id_str)

    @staticmethod
    def get_bot_by_token(token: str) -> Optional[dict]:
        data = load_data()
        for bot in data["bots"].values():
            if bot.get("token") == token:
                return bot
        return None

    @staticmethod
    def update_bot_status(bot_id: int, is_running: bool):
        data = load_data()
        bot_id_str = str(bot_id)
        if bot_id_str in data["bots"]:
            data["bots"][bot_id_str]["is_running"] = is_running
            save_data(data)

    @staticmethod
    def update_bot_info(token: str, username: str, name: str):
        data = load_data()
        for bot in data["bots"].values():
            if bot.get("token") == token:
                bot["bot_username"] = username
                bot["bot_name"] = name
                save_data(data)
                break

    @staticmethod
    def add_bot_user(bot_id: int, user_id: int, added_by: int) -> bool:
        data = load_data()
        bot_id_str = str(bot_id)
        if bot_id_str not in data["bot_users"]:
            data["bot_users"][bot_id_str] = []
        if user_id not in data["bot_users"][bot_id_str]:
            data["bot_users"][bot_id_str].append(user_id)
            save_data(data)
            return True
        return False

    @staticmethod
    def remove_bot_user(bot_id: int, user_id: int) -> bool:
        data = load_data()
        bot_id_str = str(bot_id)
        if bot_id_str in data["bot_users"]:
            if user_id in data["bot_users"][bot_id_str]:
                data["bot_users"][bot_id_str].remove(user_id)
                save_data(data)
                return True
        return False

    @staticmethod
    def get_bot_users(bot_id: int) -> List[int]:
        data = load_data()
        bot_id_str = str(bot_id)
        return data["bot_users"].get(bot_id_str, [])

    @staticmethod
    def add_nc_template(user_id: int, template: str) -> int:
        data = load_data()
        user_id_str = str(user_id)
        if user_id_str not in data["nc_templates"]:
            data["nc_templates"][user_id_str] = {"next_id": 1, "templates": {}}
        
        tpl_id = data["nc_templates"][user_id_str]["next_id"]
        data["nc_templates"][user_id_str]["templates"][str(tpl_id)] = {
            "id": tpl_id,
            "template": template,
            "created_at": datetime.now().isoformat()
        }
        data["nc_templates"][user_id_str]["next_id"] += 1
        save_data(data)
        return tpl_id

    @staticmethod
    def get_nc_templates(user_id: int) -> List[dict]:
        data = load_data()
        user_id_str = str(user_id)
        if user_id_str in data["nc_templates"]:
            templates = list(data["nc_templates"][user_id_str]["templates"].values())
            return sorted(templates, key=lambda x: x["id"])
        return []

    @staticmethod
    def remove_nc_template(user_id: int, template_id: int = None) -> int:
        data = load_data()
        user_id_str = str(user_id)
        count = 0
        if user_id_str in data["nc_templates"]:
            if template_id:
                if str(template_id) in data["nc_templates"][user_id_str]["templates"]:
                    del data["nc_templates"][user_id_str]["templates"][str(template_id)]
                    count = 1
            else:
                count = len(data["nc_templates"][user_id_str]["templates"])
                data["nc_templates"][user_id_str]["templates"] = {}
            save_data(data)
        return count

    @staticmethod
    def add_spam_template(user_id: int, template: str) -> int:
        data = load_data()
        user_id_str = str(user_id)
        if user_id_str not in data["spam_templates"]:
            data["spam_templates"][user_id_str] = {"next_id": 1, "templates": {}}
        
        tpl_id = data["spam_templates"][user_id_str]["next_id"]
        data["spam_templates"][user_id_str]["templates"][str(tpl_id)] = {
            "id": tpl_id,
            "template": template,
            "created_at": datetime.now().isoformat()
        }
        data["spam_templates"][user_id_str]["next_id"] += 1
        save_data(data)
        return tpl_id

    @staticmethod
    def get_spam_templates(user_id: int) -> List[dict]:
        data = load_data()
        user_id_str = str(user_id)
        if user_id_str in data["spam_templates"]:
            templates = list(data["spam_templates"][user_id_str]["templates"].values())
            return sorted(templates, key=lambda x: x["id"])
        return []

    @staticmethod
    def remove_spam_template(user_id: int, template_id: int = None) -> int:
        data = load_data()
        user_id_str = str(user_id)
        count = 0
        if user_id_str in data["spam_templates"]:
            if template_id:
                if str(template_id) in data["spam_templates"][user_id_str]["templates"]:
                    del data["spam_templates"][user_id_str]["templates"][str(template_id)]
                    count = 1
            else:
                count = len(data["spam_templates"][user_id_str]["templates"])
                data["spam_templates"][user_id_str]["templates"] = {}
            save_data(data)
        return count

    @staticmethod
    def add_reply_template(user_id: int, template: str) -> int:
        data = load_data()
        user_id_str = str(user_id)
        if user_id_str not in data["reply_templates"]:
            data["reply_templates"][user_id_str] = {"next_id": 1, "templates": {}}
        
        tpl_id = data["reply_templates"][user_id_str]["next_id"]
        data["reply_templates"][user_id_str]["templates"][str(tpl_id)] = {
            "id": tpl_id,
            "template": template,
            "created_at": datetime.now().isoformat()
        }
        data["reply_templates"][user_id_str]["next_id"] += 1
        save_data(data)
        return tpl_id

    @staticmethod
    def get_reply_templates(user_id: int) -> List[dict]:
        data = load_data()
        user_id_str = str(user_id)
        if user_id_str in data["reply_templates"]:
            templates = list(data["reply_templates"][user_id_str]["templates"].values())
            return sorted(templates, key=lambda x: x["id"])
        return []

    @staticmethod
    def remove_reply_template(user_id: int, template_id: int = None) -> int:
        data = load_data()
        user_id_str = str(user_id)
        count = 0
        if user_id_str in data["reply_templates"]:
            if template_id:
                if str(template_id) in data["reply_templates"][user_id_str]["templates"]:
                    del data["reply_templates"][user_id_str]["templates"][str(template_id)]
                    count = 1
            else:
                count = len(data["reply_templates"][user_id_str]["templates"])
                data["reply_templates"][user_id_str]["templates"] = {}
            save_data(data)
        return count

    @staticmethod
    def grant_permission(user_id: int, permission: str, granted_by: int) -> bool:
        data = load_data()
        user_id_str = str(user_id)
        if user_id_str not in data["permissions"]:
            data["permissions"][user_id_str] = []
        if permission not in data["permissions"][user_id_str]:
            data["permissions"][user_id_str].append(permission)
            save_data(data)
            return True
        return False

    @staticmethod
    def revoke_permission(user_id: int, permission: str = None) -> bool:
        data = load_data()
        user_id_str = str(user_id)
        if user_id_str in data["permissions"]:
            if permission:
                if permission in data["permissions"][user_id_str]:
                    data["permissions"][user_id_str].remove(permission)
                    save_data(data)
                    return True
            else:
                data["permissions"][user_id_str] = []
                save_data(data)
                return True
        return False

    @staticmethod
    def get_permissions(user_id: int) -> List[str]:
        data = load_data()
        user_id_str = str(user_id)
        return data["permissions"].get(user_id_str, [])

    @staticmethod
    def has_permission(user_id: int, permission: str) -> bool:
        perms = Database.get_permissions(user_id)
        return 'all' in perms or permission in perms


COMMAND_LOCKS: Dict[int, asyncio.Lock] = {}

def get_lock(chat_id: int) -> asyncio.Lock:
    if chat_id not in COMMAND_LOCKS:
        COMMAND_LOCKS[chat_id] = asyncio.Lock()
    return COMMAND_LOCKS[chat_id]


class ChildBot:
    def __init__(self, bot_id: int, token: str, owner_id: int):
        self.bot_id = bot_id
        self.token = token
        self.owner_id = owner_id
        self.active_spam: Dict[int, List[asyncio.Task]] = {}
        self.active_nc: Dict[int, List[asyncio.Task]] = {}
        self.active_custom_nc: Dict[int, List[asyncio.Task]] = {}
        self.active_reply: Dict[int, asyncio.Task] = {}
        self.reply_targets: Dict[int, str] = {}
        self.pending_replies: Dict[int, List[int]] = {}
        self.delays: Dict[int, float] = {}
        self.threads: Dict[int, int] = {}
        self.locks: Dict[int, asyncio.Lock] = {}
        self.stats = {"sent": 0, "errors": 0, "start": time.time()}
        self.running = True
        self.app = None
        self.stop_event = asyncio.Event()

    def get_lock(self, chat_id):
        if chat_id not in self.locks:
            self.locks[chat_id] = asyncio.Lock()
        return self.locks[chat_id]

    def is_authorized(self, user_id: int) -> bool:
        if user_id == self.owner_id:
            return True
        return user_id in Database.get_bot_users(self.bot_id)

    async def check_auth(self, update) -> bool:
        if not self.is_authorized(update.effective_user.id):
            try:
                await update.message.reply_text(UNAUTHORIZED_MSG)
            except:
                pass
            return False
        return True

    async def cancel_tasks(self, tasks: List[asyncio.Task]):
        for t in tasks:
            if not t.done():
                t.cancel()
        for t in tasks:
            try:
                await asyncio.wait_for(asyncio.shield(t), timeout=2.0)
            except:
                pass

    async def stop_all(self):
        all_tasks = []
        for tasks in list(self.active_spam.values()):
            all_tasks.extend(tasks)
        self.active_spam.clear()
        for tasks in list(self.active_nc.values()):
            all_tasks.extend(tasks)
        self.active_nc.clear()
        for tasks in list(self.active_custom_nc.values()):
            all_tasks.extend(tasks)
        self.active_custom_nc.clear()
        for task in list(self.active_reply.values()):
            all_tasks.append(task)
        self.active_reply.clear()
        self.reply_targets.clear()
        self.pending_replies.clear()
        await self.cancel_tasks(all_tasks)
        return len(all_tasks)

    def get_nc_messages(self):
        bot = Database.get_bot(self.bot_id)
        if bot:
            templates = Database.get_nc_templates(bot['user_id'])
            if templates:
                return [t['template'] for t in templates]
        return DEFAULT_NC_MESSAGES

    def get_spam_messages(self):
        bot = Database.get_bot(self.bot_id)
        if bot:
            templates = Database.get_spam_templates(bot['user_id'])
            if templates:
                return [t['template'] for t in templates]
        return DEFAULT_SPAM_MESSAGES

    def get_reply_messages(self):
        bot = Database.get_bot(self.bot_id)
        if bot:
            templates = Database.get_reply_templates(bot['user_id'])
            if templates:
                return [t['template'] for t in templates]
        return DEFAULT_REPLY_MESSAGES

    async def nc_loop(self, chat_id, target, context, worker_id=1):
        idx = 0
        msgs = self.get_nc_messages()
        count = 0
        while self.running:
            try:
                delay = self.delays.get(chat_id, 0)
                msg = msgs[idx % len(msgs)].format(target=target)
                await context.bot.set_chat_title(chat_id=chat_id, title=msg)
                idx += 1
                count += 1
                self.stats["sent"] += 1
                if delay > 0:
                    await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break
            except RetryAfter as e:
                await asyncio.sleep(int(e.retry_after) + 0.1)
            except (TimedOut, NetworkError):
                pass
            except (BadRequest, Forbidden):
                await asyncio.sleep(0.1)
                idx += 1
            except:
                self.stats["errors"] += 1
                idx += 1

    async def custom_nc_loop(self, chat_id, custom, context, worker_id=1):
        count = 0
        while self.running:
            try:
                delay = self.delays.get(chat_id, 0)
                heart = random.choice(HEART_EMOJIS)
                await context.bot.set_chat_title(chat_id=chat_id, title=f"{custom} {heart}")
                count += 1
                self.stats["sent"] += 1
                if delay > 0:
                    await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break
            except RetryAfter as e:
                await asyncio.sleep(int(e.retry_after) + 0.1)
            except (TimedOut, NetworkError):
                pass
            except (BadRequest, Forbidden):
                await asyncio.sleep(0.1)
            except:
                self.stats["errors"] += 1

    async def spam_loop(self, chat_id, target, context, worker_id):
        count = 0
        while self.running:
            try:
                delay = self.delays.get(chat_id, 0)
                msgs = self.get_spam_messages()
                msg = random.choice(msgs).format(target=target)
                await context.bot.send_message(chat_id=chat_id, text=msg)
                count += 1
                self.stats["sent"] += 1
                if delay > 0:
                    await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break
            except RetryAfter as e:
                await asyncio.sleep(int(e.retry_after) + 0.1)
            except (TimedOut, NetworkError):
                pass
            except (BadRequest, Forbidden):
                await asyncio.sleep(0.1)
            except:
                self.stats["errors"] += 1

    async def reply_loop(self, chat_id, context):
        target = self.reply_targets.get(chat_id, "User")
        idx = 0
        msgs = self.get_reply_messages()
        while self.running:
            try:
                msg = msgs[idx % len(msgs)].format(target=target)
                await context.bot.send_message(chat_id=chat_id, text=msg)
                idx += 1
                self.stats["sent"] += 1
                await asyncio.sleep(random.uniform(2, 5))
            except asyncio.CancelledError:
                break
            except:
                self.stats["errors"] += 1

    async def run(self):
        try:
            self.app = Application.builder().token(self.token).build()
            await self.app.initialize()

            bot_info = await self.app.bot.get_me()
            Database.update_bot_info(self.token, bot_info.username, bot_info.first_name)

            self.app.add_handler(CommandHandler("nc", self.cmd_nc))
            self.app.add_handler(CommandHandler("spam", self.cmd_spam))
            self.app.add_handler(CommandHandler("ctmnc", self.cmd_ctmnc))
            self.app.add_handler(CommandHandler("reply", self.cmd_reply))
            self.app.add_handler(CommandHandler("target", self.cmd_target))
            self.app.add_handler(CommandHandler("delay", self.cmd_delay))
            self.app.add_handler(CommandHandler("threads", self.cmd_threads))
            self.app.add_handler(CommandHandler("stopall", self.cmd_stopall))
            self.app.add_handler(CommandHandler("stats", self.cmd_stats))
            self.app.add_handler(CommandHandler("adduser", self.cmd_adduser))
            self.app.add_handler(CommandHandler("removeuser", self.cmd_removeuser))
            self.app.add_handler(CommandHandler("addnc", self.cmd_addnc))
            self.app.add_handler(CommandHandler("removenc", self.cmd_removenc))
            self.app.add_handler(CommandHandler("addspam", self.cmd_addspam))
            self.app.add_handler(CommandHandler("removespam", self.cmd_removespam))
            self.app.add_handler(CommandHandler("addreply", self.cmd_addreply))
            self.app.add_handler(CommandHandler("removereply", self.cmd_removereply))

            await self.app.start()
            logger.info(f"Child bot {self.bot_id} running: @{bot_info.username}")

            await self.stop_event.wait()

        except Exception as e:
            logger.error(f"Error running bot {self.bot_id}: {e}")
        finally:
            await self.stop_all()
            if self.app:
                await self.app.stop()
                await self.app.shutdown()
            self.running = False
            logger.info(f"Child bot {self.bot_id} stopped")

    async def cmd_nc(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /nc <target_name>")
            return

        chat_id = update.effective_chat.id
        target = " ".join(context.args)
        lock = self.get_lock(chat_id)
        async with lock:
            if chat_id in self.active_nc:
                await update.message.reply_text("✅ NC already running in this chat")
                return

            threads = self.threads.get(chat_id, 1)
            self.active_nc[chat_id] = []
            for i in range(threads):
                task = asyncio.create_task(self.nc_loop(chat_id, target, context, i+1))
                self.active_nc[chat_id].append(task)
            await update.message.reply_text(f"🚀 **NC Started** for {target}\n\n{threads} thread(s) | Delay: {self.delays.get(chat_id, 0)}s")

    async def cmd_spam(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /spam <target_name>")
            return

        chat_id = update.effective_chat.id
        target = " ".join(context.args)
        lock = self.get_lock(chat_id)
        async with lock:
            if chat_id in self.active_spam:
                await update.message.reply_text("✅ Spam already running in this chat")
                return

            threads = self.threads.get(chat_id, 1)
            self.active_spam[chat_id] = []
            for i in range(threads):
                task = asyncio.create_task(self.spam_loop(chat_id, target, context, i+1))
                self.active_spam[chat_id].append(task)
            await update.message.reply_text(f"🚀 **Spam Started** for {target}\n\n{threads} thread(s) | Delay: {self.delays.get(chat_id, 0)}s")

    async def cmd_ctmnc(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /ctmnc <custom_text>")
            return

        chat_id = update.effective_chat.id
        custom = " ".join(context.args)
        lock = self.get_lock(chat_id)
        async with lock:
            if chat_id in self.active_custom_nc:
                await update.message.reply_text("✅ Custom NC already running")
                return

            self.active_custom_nc[chat_id] = []
            task = asyncio.create_task(self.custom_nc_loop(chat_id, custom, context))
            self.active_custom_nc[chat_id].append(task)
            await update.message.reply_text(f"🚀 **Custom NC Started**: {custom}")

    async def cmd_reply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /reply <target_name>")
            return

        chat_id = update.effective_chat.id
        target = " ".join(context.args)
        lock = self.get_lock(chat_id)
        async with lock:
            if chat_id in self.active_reply:
                await update.message.reply_text("✅ Reply already active in this chat")
                return

            self.reply_targets[chat_id] = target
            task = asyncio.create_task(self.reply_loop(chat_id, context))
            self.active_reply[chat_id] = task
            await update.message.reply_text(f"🚀 **Reply Mode Active** for {target}")

    async def cmd_target(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /target <target_name>")
            return

        chat_id = update.effective_chat.id
        target = " ".join(context.args)
        await self.cmd_nc(update, context)
        await self.cmd_spam(update, context)
        await update.message.reply_text(f"🚀 **NC + Spam Started** for {target}")

    async def cmd_delay(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /delay <seconds>")
            return

        try:
            delay = float(context.args[0])
            chat_id = update.effective_chat.id
            self.delays[chat_id] = delay
            await update.message.reply_text(f"⏱ Delay set to {delay}s")
        except ValueError:
            await update.message.reply_text("Invalid delay value!")

    async def cmd_threads(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /threads <1-50>")
            return

        try:
            threads = int(context.args[0])
            if threads < 1 or threads > 50:
                await update.message.reply_text("Threads must be between 1 and 50!")
                return
            chat_id = update.effective_chat.id
            self.threads[chat_id] = threads
            await update.message.reply_text(f"🔄 Threads set to {threads}")
        except ValueError:
            await update.message.reply_text("Invalid thread count!")

    async def cmd_stopall(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return

        chat_id = update.effective_chat.id
        stopped = 0

        if chat_id in self.active_spam:
            await self.cancel_tasks(self.active_spam[chat_id])
            del self.active_spam[chat_id]
            stopped += 1

        if chat_id in self.active_nc:
            await self.cancel_tasks(self.active_nc[chat_id])
            del self.active_nc[chat_id]
            stopped += 1

        if chat_id in self.active_custom_nc:
            await self.cancel_tasks(self.active_custom_nc[chat_id])
            del self.active_custom_nc[chat_id]
            stopped += 1

        if chat_id in self.active_reply:
            self.active_reply[chat_id].cancel()
            del self.active_reply[chat_id]
            stopped += 1

        await update.message.reply_text(f"⏹ Stopped {stopped} operation(s)")

    async def cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return

        uptime = time.time() - self.stats["start"]
        uptime_str = f"{int(uptime // 3600)}h {int((uptime % 3600) // 60)}m"
        text = f"""
📊 **Bot Stats**

📤 Sent: {self.stats['sent']}
❌ Errors: {self.stats['errors']}
⏱ Uptime: {uptime_str}
"""
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    async def cmd_adduser(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /adduser <bot_id> <user_id>")
            return

        try:
            bot_id = int(context.args[0])
            user_id = int(context.args[1])
            if Database.add_bot_user(bot_id, user_id, update.effective_user.id):
                await update.message.reply_text(f"✅ User {user_id} added to bot {bot_id}")
            else:
                await update.message.reply_text("User already added or bot not found!")
        except ValueError:
            await update.message.reply_text("Invalid IDs!")

    async def cmd_removeuser(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /removeuser <bot_id> <user_id>")
            return

        try:
            bot_id = int(context.args[0])
            user_id = int(context.args[1])
            if Database.remove_bot_user(bot_id, user_id):
                await update.message.reply_text(f"✅ User {user_id} removed from bot {bot_id}")
            else:
                await update.message.reply_text("User not found!")
        except ValueError:
            await update.message.reply_text("Invalid IDs!")

    async def cmd_addnc(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /addnc <template_text>")
            return

        template = " ".join(context.args)
        bot = Database.get_bot(self.bot_id)
        tpl_id = Database.add_nc_template(bot['user_id'], template)
        await update.message.reply_text(f"✅ NC Template #{tpl_id} added!")

    async def cmd_removenc(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /removenc <id or 'all'>")
            return

        bot = Database.get_bot(self.bot_id)
        if context.args[0].lower() == 'all':
            count = Database.remove_nc_template(bot['user_id'])
            await update.message.reply_text(f"✅ Removed {count} NC templates!")
        else:
            try:
                tpl_id = int(context.args[0])
                count = Database.remove_nc_template(bot['user_id'], tpl_id)
                await update.message.reply_text(f"✅ Removed NC template #{tpl_id}!" if count > 0 else "Template not found!")
            except ValueError:
                await update.message.reply_text("Invalid template ID!")

    async def cmd_addspam(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /addspam <template_text>")
            return

        template = " ".join(context.args)
        bot = Database.get_bot(self.bot_id)
        tpl_id = Database.add_spam_template(bot['user_id'], template)
        await update.message.reply_text(f"✅ Spam Template #{tpl_id} added!")

    async def cmd_removespam(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /removespam <id or 'all'>")
            return

        bot = Database.get_bot(self.bot_id)
        if context.args[0].lower() == 'all':
            count = Database.remove_spam_template(bot['user_id'])
            await update.message.reply_text(f"✅ Removed {count} spam templates!")
        else:
            try:
                tpl_id = int(context.args[0])
                count = Database.remove_spam_template(bot['user_id'], tpl_id)
                await update.message.reply_text(f"✅ Removed spam template #{tpl_id}!" if count > 0 else "Template not found!")
            except ValueError:
                await update.message.reply_text("Invalid template ID!")

    async def cmd_addreply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /addreply <template_text>")
            return

        template = " ".join(context.args)
        bot = Database.get_bot(self.bot_id)
        tpl_id = Database.add_reply_template(bot['user_id'], template)
        await update.message.reply_text(f"✅ Reply Template #{tpl_id} added!")

    async def cmd_removereply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_auth(update):
            return
        if not context.args:
            await update.message.reply_text("Usage: /removereply <id or 'all'>")
            return

        bot = Database.get_bot(self.bot_id)
        if context.args[0].lower() == 'all':
            count = Database.remove_reply_template(bot['user_id'])
            await update.message.reply_text(f"✅ Removed {count} reply templates!")
        else:
            try:
                tpl_id = int(context.args[0])
                count = Database.remove_reply_template(bot['user_id'], tpl_id)
                await update.message.reply_text(f"✅ Removed reply template #{tpl_id}!" if count > 0 else "Template not found!")
            except ValueError:
                await update.message.reply_text("Invalid template ID!")


RUNNING_BOTS: Dict[int, ChildBot] = {}


class ControllerBot:
    def __init__(self):
        self.app = None

    def get_main_menu(self):
        keyboard = [
            [InlineKeyboardButton("➕ Add Bot", callback_data="menu_addbot"),
             InlineKeyboardButton("📋 My Bots", callback_data="menu_listbots")],
            [InlineKeyboardButton("▶️ Start All", callback_data="menu_startall"),
             InlineKeyboardButton("⏹ Stop All", callback_data="menu_stopall")],
            [InlineKeyboardButton("📝 Templates", callback_data="menu_templates"),
             InlineKeyboardButton("📊 Status", callback_data="menu_status")],
            [InlineKeyboardButton("❓ Help", callback_data="menu_help")]
        ]
        return InlineKeyboardMarkup(keyboard)

    def get_templates_menu(self):
        keyboard = [
            [InlineKeyboardButton("📝 NC Templates", callback_data="tpl_nc"),
             InlineKeyboardButton("💬 Spam Templates", callback_data="tpl_spam")],
            [InlineKeyboardButton("💭 Reply Templates", callback_data="tpl_reply")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")]
        ]
        return InlineKeyboardMarkup(keyboard)

    def get_bot_actions(self, bot_id: int):
        keyboard = [
            [InlineKeyboardButton("▶️ Start", callback_data=f"bot_start_{bot_id}"),
             InlineKeyboardButton("⏹ Stop", callback_data=f"bot_stop_{bot_id}")],
            [InlineKeyboardButton("ℹ️ Info", callback_data=f"bot_info_{bot_id}"),
             InlineKeyboardButton("🗑 Remove", callback_data=f"bot_remove_{bot_id}")],
            [InlineKeyboardButton("👥 Users", callback_data=f"bot_users_{bot_id}")],
            [InlineKeyboardButton("🔙 Back to Bots", callback_data="menu_listbots")]
        ]
        return InlineKeyboardMarkup(keyboard)

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        Database.register_user(user.id, user.username)

        welcome_text = f"""
╔══════════════════════════════════════════╗
║   ⚡ 𝐇𝐘𝐏𝐄𝐑-𝐗 𝐌𝐮𝐥𝐭𝐢-𝐁𝐨𝐭 𝐂𝐨𝐧𝐭𝐫𝐨𝐥𝐥𝐞𝐫 ⚡   ║
╚══════════════════════════════════════════╝

Welcome, **{user.first_name}**!

Your ID: `{user.id}`

Use the buttons below to manage your bots:
"""
        await update.message.reply_text(welcome_text, reply_markup=self.get_main_menu(), parse_mode=ParseMode.MARKDOWN)

    async def callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        user_id = query.from_user.id

        if data == "menu_back" or data == "menu_main":
            await query.edit_message_text(
                f"╔══════════════════════════════════════════╗\n"
                f"║   ⚡ 𝐇𝐘𝐏𝐄𝐑-𝐗 𝐌𝐮𝐥𝐭𝐢-𝐁𝐨𝐭 𝐂𝐨𝐧𝐭𝐫𝐨𝐥𝐥𝐞𝐫 ⚡   ║\n"
                f"╚══════════════════════════════════════════╝\n\n"
                f"Your ID: `{user_id}`\n\n"
                f"Use the buttons below:",
                reply_markup=self.get_main_menu(),
                parse_mode=ParseMode.MARKDOWN
            )

        elif data == "menu_addbot":
            context.user_data['adding_bot'] = True
            context.user_data['bot_step'] = 'token'
            await query.edit_message_text(
                "🤖 **Add New Bot**\n\n"
                "Please send me the bot token from @BotFather:\n\n"
                "Example: `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`\n\n"
                "Send /cancel to cancel.",
                parse_mode=ParseMode.MARKDOWN
            )

        elif data == "menu_listbots":
            bots = Database.get_user_bots(user_id)
            if not bots:
                keyboard = [[InlineKeyboardButton("➕ Add Bot", callback_data="menu_addbot")],
                           [InlineKeyboardButton("🔙 Back", callback_data="menu_back")]]
                await query.edit_message_text(
                    "📭 You have no bots yet!\n\nClick below to add one:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return

            text = "🤖 **Your Bots:**\n\n"
            keyboard = []
            for b in bots:
                status = "🟢" if b['id'] in RUNNING_BOTS else "🔴"
                text += f"{status} **#{b['id']}** - @{b['bot_username'] or 'Unknown'}\n"
                keyboard.append([InlineKeyboardButton(
                    f"{status} #{b['id']} - @{b['bot_username'] or 'Bot'}",
                    callback_data=f"bot_select_{b['id']}"
                )])
            keyboard.append([InlineKeyboardButton("➕ Add New Bot", callback_data="menu_addbot")])
            keyboard.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")])
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data.startswith("bot_select_"):
            bot_id = int(data.split("_")[2])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await query.edit_message_text("❌ Bot not found!")
                return
            status = "🟢 Running" if bot_id in RUNNING_BOTS else "🔴 Stopped"
            users = Database.get_bot_users(bot_id)
            text = f"""
📊 **Bot #{bot_id}**

👤 Username: @{bot['bot_username'] or 'Unknown'}
📛 Name: {bot['bot_name'] or 'N/A'}
📍 Status: {status}
👥 Authorized: {len(users)} users
"""
            await query.edit_message_text(text, reply_markup=self.get_bot_actions(bot_id), parse_mode=ParseMode.MARKDOWN)

        elif data.startswith("bot_start_"):
            bot_id = int(data.split("_")[2])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await query.edit_message_text("❌ Bot not found!")
                return
            if bot_id in RUNNING_BOTS:
                await query.answer("⚠️ Bot is already running!", show_alert=True)
                return
            
            await query.answer()
            await query.edit_message_text(
                f"⏳ **Starting Bot #{bot_id}...**\n\n@{bot['bot_username']}\n\n_Please wait while we initialize the bot..._",
                reply_markup=self.get_bot_actions(bot_id),
                parse_mode=ParseMode.MARKDOWN
            )
            
            try:
                child = ChildBot(bot_id, bot['token'], bot['user_id'])
                RUNNING_BOTS[bot_id] = child
                asyncio.create_task(child.run())
                
                await query.edit_message_text(
                    f"✅ **Bot #{bot_id} Started Successfully!**\n\n"
                    f"👤 Username: @{bot['bot_username']}\n"
                    f"📛 Name: {bot['bot_name'] or 'N/A'}\n"
                    f"🟢 Status: Running\n\n"
                    f"_The bot is now active and listening for commands._",
                    reply_markup=self.get_bot_actions(bot_id),
                    parse_mode=ParseMode.MARKDOWN
                )
                logger.info(f"Bot #{bot_id} started successfully")
            except Exception as e:
                await query.edit_message_text(
                    f"❌ **Failed to Start Bot #{bot_id}**\n\n"
                    f"Error: {str(e)}",
                    reply_markup=self.get_bot_actions(bot_id),
                    parse_mode=ParseMode.MARKDOWN
                )
                logger.error(f"Failed to start bot {bot_id}: {e}")

        elif data.startswith("bot_stop_"):
            bot_id = int(data.split("_")[2])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await query.edit_message_text("❌ Bot not found!")
                return
            if bot_id not in RUNNING_BOTS:
                await query.answer("⚠️ Bot is not running!", show_alert=True)
                return
            
            await query.answer()
            await query.edit_message_text(
                f"⏳ **Stopping Bot #{bot_id}...**\n\n@{bot['bot_username']}\n\n_Please wait while we shut down the bot..._",
                reply_markup=self.get_bot_actions(bot_id),
                parse_mode=ParseMode.MARKDOWN
            )
            
            try:
                if bot_id in RUNNING_BOTS:
                    RUNNING_BOTS[bot_id].stop_event.set()
                    del RUNNING_BOTS[bot_id]
                
                await query.edit_message_text(
                    f"⏹ **Bot #{bot_id} Stopped Successfully!**\n\n"
                    f"👤 Username: @{bot['bot_username']}\n"
                    f"📛 Name: {bot['bot_name'] or 'N/A'}\n"
                    f"🔴 Status: Offline\n\n"
                    f"_The bot is now stopped. Click Start to activate it again._",
                    reply_markup=self.get_bot_actions(bot_id),
                    parse_mode=ParseMode.MARKDOWN
                )
                logger.info(f"Bot #{bot_id} stopped successfully")
            except Exception as e:
                await query.edit_message_text(
                    f"❌ **Failed to Stop Bot #{bot_id}**\n\n"
                    f"Error: {str(e)}",
                    reply_markup=self.get_bot_actions(bot_id),
                    parse_mode=ParseMode.MARKDOWN
                )
                logger.error(f"Failed to stop bot {bot_id}: {e}")

        elif data.startswith("bot_info_"):
            bot_id = int(data.split("_")[2])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await query.edit_message_text("❌ Bot not found!")
                return
            status = "🟢 Running" if bot_id in RUNNING_BOTS else "🔴 Stopped"
            users = Database.get_bot_users(bot_id)
            text = f"""
📊 **Bot #{bot_id} Details**

👤 Username: @{bot['bot_username'] or 'Unknown'}
📛 Name: {bot['bot_name'] or 'N/A'}
👑 Owner ID: `{bot['user_id']}`
📅 Added: {bot['created_at']}
📍 Status: {status}
👥 Authorized Users: {len(users)}
"""
            await query.edit_message_text(text, reply_markup=self.get_bot_actions(bot_id), parse_mode=ParseMode.MARKDOWN)

        elif data.startswith("bot_remove_"):
            bot_id = int(data.split("_")[2])
            keyboard = [
                [InlineKeyboardButton("✅ Yes, Remove", callback_data=f"bot_confirm_remove_{bot_id}"),
                 InlineKeyboardButton("❌ Cancel", callback_data=f"bot_select_{bot_id}")]
            ]
            await query.edit_message_text(
                f"⚠️ **Remove Bot #{bot_id}?**\n\nThis action cannot be undone!",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )

        elif data.startswith("bot_confirm_remove_"):
            bot_id = int(data.split("_")[3])
            if bot_id in RUNNING_BOTS:
                RUNNING_BOTS[bot_id].stop_event.set()
                del RUNNING_BOTS[bot_id]
            if Database.remove_bot(user_id, bot_id):
                await query.answer("Bot removed!", show_alert=True)
                keyboard = [[InlineKeyboardButton("🔙 Back to Bots", callback_data="menu_listbots")]]
                await query.edit_message_text("✅ Bot removed successfully!", reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await query.edit_message_text("❌ Failed to remove bot!")

        elif data.startswith("bot_users_"):
            bot_id = int(data.split("_")[2])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await query.edit_message_text("❌ Bot not found!")
                return
            users = Database.get_bot_users(bot_id)
            text = f"👥 **Authorized Users for Bot #{bot_id}:**\n\n"
            text += f"👑 Owner: `{bot['user_id']}`\n\n"
            if users:
                for u in users:
                    text += f"• `{u}`\n"
            else:
                text += "_No additional users authorized_\n"
            text += f"\nTo add: `/adduser {bot_id} <user_id>`\nTo remove: `/removeuser {bot_id} <user_id>`"
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data=f"bot_select_{bot_id}")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data == "menu_startall":
            bots = Database.get_user_bots(user_id)
            started = 0
            for bot in bots:
                if bot['id'] not in RUNNING_BOTS:
                    child = ChildBot(bot['id'], bot['token'], bot['user_id'])
                    RUNNING_BOTS[bot['id']] = child
                    asyncio.create_task(child.run())
                    started += 1
            
            if started > 0:
                text = f"✅ **All Bots Started!**\n\n🟢 Started {started} bot(s)"
            else:
                text = "⚠️ **All bots are already running!**"
            
            keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data == "menu_stopall":
            bots = Database.get_user_bots(user_id)
            stopped = 0
            for bot in bots:
                if bot['id'] in RUNNING_BOTS:
                    RUNNING_BOTS[bot['id']].stop_event.set()
                    del RUNNING_BOTS[bot['id']]
                    stopped += 1
            await query.answer(f"Stopped {stopped} bots!", show_alert=True)

        elif data == "menu_templates":
            nc_count = len(Database.get_nc_templates(user_id))
            spam_count = len(Database.get_spam_templates(user_id))
            reply_count = len(Database.get_reply_templates(user_id))
            text = f"📝 **Your Templates**\n\nNC Templates: {nc_count}\nSpam Templates: {spam_count}\nReply Templates: {reply_count}"
            await query.edit_message_text(text, reply_markup=self.get_templates_menu(), parse_mode=ParseMode.MARKDOWN)

        elif data == "tpl_nc":
            templates = Database.get_nc_templates(user_id)
            text = "📝 **NC Templates:**\n\n"
            if templates:
                for t in templates:
                    text += f"**#{t['id']}:** {t['template'][:40]}...\n"
            else:
                text += "_No templates. Using defaults._\n"
            text += "\nTo add: `/addnc <template>`\nTo remove: `/removenc <id/all>`"
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="menu_templates")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data == "tpl_spam":
            templates = Database.get_spam_templates(user_id)
            text = "💬 **Spam Templates:**\n\n"
            if templates:
                for t in templates:
                    text += f"**#{t['id']}:** {t['template'][:40]}...\n"
            else:
                text += "_No templates. Using defaults._\n"
            text += "\nTo add: `/addspam <template>`\nTo remove: `/removespam <id/all>`"
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="menu_templates")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data == "tpl_reply":
            templates = Database.get_reply_templates(user_id)
            text = "💭 **Reply Templates:**\n\n"
            if templates:
                for t in templates:
                    text += f"**#{t['id']}:** {t['template'][:40]}...\n"
            else:
                text += "_No templates. Using defaults._\n"
            text += "\nTo add: `/addreply <template>`\nTo remove: `/removereply <id/all>`"
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="menu_templates")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data == "menu_status":
            bots = Database.get_user_bots(user_id)
            running = sum(1 for b in bots if b['id'] in RUNNING_BOTS)
            nc_count = len(Database.get_nc_templates(user_id))
            spam_count = len(Database.get_spam_templates(user_id))
            reply_count = len(Database.get_reply_templates(user_id))
            text = f"""
📊 **Your Status**

👤 Your ID: `{user_id}`
🤖 Total Bots: {len(bots)}
🟢 Running: {running}
🔴 Stopped: {len(bots) - running}

📝 NC Templates: {nc_count}
💬 Spam Templates: {spam_count}
💭 Reply Templates: {reply_count}
"""
            keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")]]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

        elif data == "menu_help":
            help_text = """
❓ **Help & Commands**

━━ 🤖 Bot Management ━━
• Add bots via the menu
• Each bot needs an owner ID
• Only the owner controls the bot

━━ 📝 Templates ━━
`/addnc <text>` - Add NC template
`/addspam <text>` - Add spam template
Use `{target}` as placeholder

━━ 👥 User Management ━━
`/adduser <bot_id> <user_id>`
`/removeuser <bot_id> <user_id>`

━━ ⚡ Child Bot Commands ━━
`/nc <target>` - Start NC loop
`/spam <target>` - Start spam
`/ctmnc <name>` - Custom NC
`/reply <target>` - Auto reply
`/target <name>` - NC + Spam
`/delay <sec>` - Set delay
`/threads <1-50>` - Set threads
`/stopall` - Stop all tasks
"""
            keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")]]
            await query.edit_message_text(help_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)

    async def cmd_addbot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        Database.register_user(user_id, update.effective_user.username)
        await update.message.reply_text(
            "🤖 **Add New Bot**\n\n"
            "Please send me the bot token from @BotFather:\n\n"
            "Example: `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`\n\n"
            "Send /cancel to cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return WAITING_TOKEN

    async def receive_token(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        token = update.message.text.strip()

        if Database.get_bot_by_token(token):
            await update.message.reply_text("❌ This bot is already registered!")
            if 'adding_bot' in context.user_data:
                context.user_data.pop('adding_bot', None)
                context.user_data.pop('bot_step', None)
            return ConversationHandler.END

        try:
            test_app = Application.builder().token(token).build()
            await test_app.initialize()
            bot_info = await test_app.bot.get_me()
            await test_app.shutdown()

            context.user_data['pending_token'] = token
            context.user_data['pending_bot_username'] = bot_info.username
            context.user_data['pending_bot_name'] = bot_info.first_name

            if 'adding_bot' in context.user_data:
                context.user_data['bot_step'] = 'owner_id'

            await update.message.reply_text(
                f"✅ **Token Valid!**\n\n"
                f"Bot: @{bot_info.username} ({bot_info.first_name})\n\n"
                f"Now send the **Owner's Telegram User ID** who will control this bot:\n\n"
                f"(Get your ID from @userinfobot on Telegram)\n\n"
                f"Send /cancel to cancel.",
                parse_mode=ParseMode.MARKDOWN
            )
            return WAITING_OWNER_ID

        except Exception as e:
            await update.message.reply_text(f"❌ Invalid token or error: {e}")
            if 'adding_bot' in context.user_data:
                context.user_data.pop('adding_bot', None)
                context.user_data.pop('bot_step', None)
            return ConversationHandler.END

    async def receive_owner_id(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            owner_id = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID! Please send a numeric Telegram user ID.")
            return WAITING_OWNER_ID

        token = context.user_data.get('pending_token')
        bot_username = context.user_data.get('pending_bot_username')
        bot_name = context.user_data.get('pending_bot_name')

        if not token:
            await update.message.reply_text("❌ Session expired. Please start again with /addbot")
            return ConversationHandler.END

        user_bots = Database.get_user_bots(owner_id)
        if len(user_bots) >= 30:
            await update.message.reply_text("❌ **Bot Limit Reached!**\n\nYou can only add maximum **30 bots** per account.\n\nPlease remove a bot first using /removebot <bot_id>")
            return ConversationHandler.END

        bot_id = Database.add_bot(owner_id, token, bot_username, bot_name)
        if bot_id:
            await update.message.reply_text(
                f"✅ **Bot Added Successfully!**\n\n"
                f"🆔 Bot ID: `{bot_id}`\n"
                f"👤 Username: @{bot_username}\n"
                f"📛 Name: {bot_name}\n"
                f"👑 Owner ID: `{owner_id}`\n\n"
                f"Only user `{owner_id}` can control this bot!\n\n"
                f"Use `/startbot {bot_id}` to start it!",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text("❌ Failed to add bot. Token might already exist.")

        context.user_data.clear()
        return ConversationHandler.END

    async def cmd_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("❌ Operation cancelled.")
        context.user_data.clear()
        return ConversationHandler.END

    async def text_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if 'adding_bot' in context.user_data:
            if context.user_data.get('bot_step') == 'token':
                return await self.receive_token(update, context)
            elif context.user_data.get('bot_step') == 'owner_id':
                return await self.receive_owner_id(update, context)

    async def cmd_removebot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /removebot <bot_id>")
            return
        try:
            bot_id = int(context.args[0])
            if bot_id in RUNNING_BOTS:
                RUNNING_BOTS[bot_id].stop_event.set()
                del RUNNING_BOTS[bot_id]
            if Database.remove_bot(user_id, bot_id):
                await update.message.reply_text(f"✅ Bot #{bot_id} removed!")
            else:
                await update.message.reply_text("❌ Bot not found or not yours!")
        except ValueError:
            await update.message.reply_text("Invalid bot ID!")

    async def cmd_listbots(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        bots = Database.get_user_bots(user_id)
        if not bots:
            await update.message.reply_text("📭 You have no bots yet. Use /addbot to add one!")
            return

        text = "🤖 **Your Bots:**\n\n"
        for b in bots:
            status = "🟢 Running" if b['id'] in RUNNING_BOTS else "🔴 Stopped"
            text += f"**#{b['id']}** - @{b['bot_username'] or 'Unknown'}\n"
            text += f"   📛 {b['bot_name'] or 'N/A'} | {status}\n\n"

        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    async def cmd_botinfo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /botinfo <bot_id>")
            return
        try:
            bot_id = int(context.args[0])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await update.message.reply_text("❌ Bot not found or not yours!")
                return

            status = "🟢 Running" if bot_id in RUNNING_BOTS else "🔴 Stopped"
            users = Database.get_bot_users(bot_id)

            text = f"""
📊 **Bot #{bot_id} Info**

👤 Username: @{bot['bot_username'] or 'Unknown'}
📛 Name: {bot['bot_name'] or 'N/A'}
📅 Added: {bot['created_at']}
📍 Status: {status}
👥 Authorized Users: {len(users)}
"""
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        except ValueError:
            await update.message.reply_text("Invalid bot ID!")

    async def cmd_startbot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /startbot <bot_id>")
            return
        try:
            bot_id = int(context.args[0])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await update.message.reply_text("❌ Bot not found or not yours!")
                return

            if bot_id in RUNNING_BOTS:
                await update.message.reply_text("⚠️ Bot is already running!")
                return

            child = ChildBot(bot_id, bot['token'], bot['user_id'])
            RUNNING_BOTS[bot_id] = child
            asyncio.create_task(child.run())
            await update.message.reply_text(f"✅ Bot #{bot_id} started!")
        except ValueError:
            await update.message.reply_text("Invalid bot ID!")

    async def cmd_stopbot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not context.args:
            await update.message.reply_text("Usage: /stopbot <bot_id>")
            return
        try:
            bot_id = int(context.args[0])
            bot = Database.get_bot(bot_id)
            if not bot or bot['user_id'] != user_id:
                await update.message.reply_text("❌ Bot not found or not yours!")
                return

            if bot_id not in RUNNING_BOTS:
                await update.message.reply_text("⚠️ Bot is not running!")
                return

            RUNNING_BOTS[bot_id].stop_event.set()
            del RUNNING_BOTS[bot_id]

            await update.message.reply_text(f"✅ Bot #{bot_id} stopped!")
        except ValueError:
            await update.message.reply_text("Invalid bot ID!")

    async def run(self):
        self.app = Application.builder().token(CONTROLLER_TOKEN).build()

        conv_handler = ConversationHandler(
            entry_points=[CommandHandler("addbot", self.cmd_addbot)],
            states={
                WAITING_TOKEN: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_token),
                ],
                WAITING_OWNER_ID: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_owner_id),
                ],
            },
            fallbacks=[CommandHandler("cancel", self.cmd_cancel)],
            persistent=False,
            name="add_bot_conv"
        )
        self.app.add_handler(conv_handler)
        
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.text_handler))
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("removebot", self.cmd_removebot))
        self.app.add_handler(CommandHandler("listbots", self.cmd_listbots))
        self.app.add_handler(CommandHandler("botinfo", self.cmd_botinfo))
        self.app.add_handler(CommandHandler("startbot", self.cmd_startbot))
        self.app.add_handler(CommandHandler("stopbot", self.cmd_stopbot))
        self.app.add_handler(CallbackQueryHandler(self.callback_handler))

        await self.app.initialize()
        await self.app.start()
        logger.info("Controller bot started!")
        await self.app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        logger.info("Polling started!")
        await asyncio.Event().wait()


async def main():
    init_database()
    controller = ControllerBot()
    await controller.run()


if __name__ == "__main__":
    asyncio.run(main())
