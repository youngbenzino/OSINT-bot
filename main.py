import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
import re
import hashlib
import time
from enum import Enum

from telethon import TelegramClient, events
from telethon.tl.types import (
    Channel, Message, User, PeerChannel,
    InputPeerChannel, MessageReplies, PeerUser,
    ChannelForbidden, Chat, ChatForbidden,
    UserEmpty, UserFull, Document, Photo,
    PeerChat, ChannelParticipantsRecent,
    UserProfilePhoto, ChatPhoto, PhotoEmpty,
    DocumentEmpty, MessageMediaPhoto, MessageMediaDocument,
    InputMessagesFilterPhotoVideo,
    UserStatusOnline, UserStatusOffline, UserStatusRecently,
    UserStatusLastWeek, UserStatusLastMonth
)
from telethon.tl.custom import Button
from telethon.tl.functions.messages import GetDiscussionMessageRequest, GetRepliesRequest
from telethon.tl.functions.channels import GetFullChannelRequest, GetMessagesRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.errors import (
    ChannelInvalidError, ChannelPrivateError,
    FloodWaitError, UsernameNotOccupiedError,
    SessionPasswordNeededError, ChatAdminRequiredError,
    MessageIdInvalidError, MessageNotModifiedError,
    UserNotParticipantError, UsernameInvalidError
)

# ===== НАСТРОЙКИ =====
BOT_TOKEN = ''
API_ID = ''
API_HASH = ''
PHONE_NUMBER = (''
                '')

BOT_SESSION_NAME = 'channel_analyzer_bot'
USER_SESSION_NAME = 'channel_analyzer_user'

# УБРАНЫ ВСЕ ОГРАНИЧЕНИЯ
MAX_POSTS_ANALYZE = 0  # 0 = без ограничений
COMMENTS_LIMIT = 0  # 0 = без ограничений
REQUEST_DELAY = 0.5  # Оптимальная задержка
MAX_BUTTONS_PER_PAGE = 8
COMMENTS_PER_PAGE = 10
# =====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AnalysisStatus(Enum):
    PENDING = "pending"
    ANALYZING = "analyzing"
    COMPLETED = "completed"
    FAILED = "failed"


class AccountDataType(Enum):
    BASIC_INFO = "basic_info"
    FULL_INFO = "full_info"
    ALL_DATA = "all_data"


class UserMode(Enum):
    IDLE = "idle"
    WAITING_ACCOUNT_INPUT = "waiting_account_input"
    WAITING_CHANNEL_LINK = "waiting_channel_link"
    WAITING_POSTS_COUNT = "waiting_posts_count"
    ANALYZING_CHANNEL = "analyzing_channel"
    ANALYZING_ACCOUNT = "analyzing_account"


@dataclass
class UserData:
    # Общие поля
    user_mode: UserMode = UserMode.IDLE
    analysis_start_time: Optional[datetime] = None
    status: AnalysisStatus = AnalysisStatus.PENDING

    # Поля для анализа каналов
    channel_entity: Optional[Channel] = None
    posts_to_analyze: int = 0
    found_users: Dict[str, Dict[str, Any]] = None
    current_channel_link: str = None
    progress_message: Optional[Message] = None
    current_authors_page: int = 0
    total_comments_collected: int = 0
    total_posts_processed: int = 0
    last_processed_post_id: int = 0

    # Поля для анализа аккаунтов
    target_account: Optional[str] = None
    account_data_type: Optional[AccountDataType] = None

    def __post_init__(self):
        if self.found_users is None:
            self.found_users = {}


class PerfectTelegramAnalyzer:
    def __init__(self):
        self.bot_client = TelegramClient(BOT_SESSION_NAME, API_ID, API_HASH)
        self.user_client = TelegramClient(USER_SESSION_NAME, API_ID, API_HASH)
        self.user_sessions: Dict[int, UserData] = {}
        self.rate_limit_semaphore = asyncio.Semaphore(3)
        self.request_count = 0
        self.last_request_time = time.time()

    async def initialize(self):
        """Инициализация клиентов"""
        try:
            await self.bot_client.start(bot_token=BOT_TOKEN)
            logger.info("Бот успешно запущен")

            await self.start_user_client()
            logger.info("Пользовательский клиент успешно запущен")

        except Exception as e:
            logger.error(f"Ошибка инициализации: {e}")
            raise

    async def start_user_client(self):
        """Запуск и авторизация пользовательского клиента"""
        try:
            await self.user_client.start(phone=lambda: PHONE_NUMBER)

            if not await self.user_client.is_user_authorized():
                try:
                    code = input("Введите код подтверждения из Telegram: ")
                    await self.user_client.sign_in(phone=PHONE_NUMBER, code=code)
                except SessionPasswordNeededError:
                    password = input("Введите пароль двухфакторной аутентификации: ")
                    await self.user_client.sign_in(password=password)

            me = await self.user_client.get_me()
            logger.info(f"Пользовательский клиент авторизован как: {me.first_name} ({me.username})")

        except Exception as e:
            logger.error(f"Ошибка авторизации пользовательского клиента: {e}")
            raise

    async def safe_telegram_request(self, coroutine, max_retries=5):
        """Безопасный запрос к Telegram API с повторными попытками"""
        for attempt in range(max_retries):
            try:
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                if time_since_last < REQUEST_DELAY:
                    await asyncio.sleep(REQUEST_DELAY - time_since_last)

                async with self.rate_limit_semaphore:
                    self.request_count += 1
                    self.last_request_time = time.time()

                    result = await coroutine
                    await asyncio.sleep(REQUEST_DELAY)
                    return result

            except FloodWaitError as e:
                wait_time = e.seconds
                logger.warning(f"Flood wait, sleeping for {wait_time} seconds")
                await asyncio.sleep(wait_time)
                await asyncio.sleep(2.0)
                continue
            except (MessageIdInvalidError, ChannelPrivateError) as e:
                logger.warning(f"Невозможно получить данные: {e}")
                return None
            except Exception as e:
                logger.warning(f"Попытка {attempt + 1} не удалась: {e}")
                if attempt == max_retries - 1:
                    raise e
                await asyncio.sleep(2 ** attempt)
        return None

    # ===== ИНТЕРФЕЙС И УПРАВЛЕНИЕ =====

    async def send_welcome_message(self, event):
        """Отправка приветственного сообщения"""
        user_id = event.sender_id
        self.user_sessions[user_id] = UserData(user_mode=UserMode.IDLE)

        welcome_text = """
🚀 PROFESSIONAL TELEGRAM ANALYZER - БЕЗ ОГРАНИЧЕНИЙ

⚡ Полное решение для анализа Telegram с максимальной производительностью

🔥 ВОЗМОЖНОСТИ АНАЛИЗА КАНАЛОВ:
• Анализ ВСЕХ постов канала (без ограничений)
• Сбор ВСЕХ комментариев (тысячи+)
• Поиск ВСЕХ авторов (с username и без)
• Полная статистика и аналитика
• Прямые ссылки на каждый комментарий
• Продолжение анализа после перезапуска

🌟 ВОЗМОЖНОСТИ АНАЛИЗА АККАУНТОВ:
• Полная информация о пользователях и каналах
• Базовая и расширенная аналитика
• Все общедоступные данные

💡 Выберите тип анализа ниже
"""
        buttons = [
            [Button.inline("📺 Анализ канала", b"channel_analysis"),
             Button.inline("👤 Анализ аккаунта", b"account_analysis")],
            [Button.inline("🆘 Помощь", b"help")]
        ]

        await event.respond(welcome_text, buttons=buttons)

    async def handle_channel_analysis_start(self, event):
        """Начало анализа канала"""
        user_id = event.sender_id
        self.user_sessions[user_id] = UserData(
            user_mode=UserMode.WAITING_CHANNEL_LINK,
            analysis_start_time=datetime.now()
        )

        await event.respond(
            "📺 **АНАЛИЗ КАНАЛА TELEGRAM - БЕЗ ОГРАНИЧЕНИЙ**\n\n"
            "Отправьте ссылку на канал для ПОЛНОГО анализа:\n\n"
            "📝 Форматы ссылок:\n"
            "• `https://t.me/channel_name`\n"
            "• `@channel_name`\n"
            "• `t.me/channel_name`\n\n"
            "⚡ *Анализ будет продолжаться до сбора ВСЕХ данных*"
        )

    async def handle_account_analysis_start(self, event):
        """Начало анализа аккаунта"""
        user_id = event.sender_id
        self.user_sessions[user_id] = UserData(
            user_mode=UserMode.WAITING_ACCOUNT_INPUT,
            analysis_start_time=datetime.now()
        )

        await event.respond(
            "👤 **АНАЛИЗ АККАУНТА TELEGRAM**\n\n"
            "Введите username пользователя для анализа:\n\n"
            "📝 Примеры:\n"
            "- `@username`\n"
            "- `username`\n\n"
            "💡 *Собираются только общедоступные данные*"
        )

    # ===== МЕТОДЫ АНАЛИЗА АККАУНТОВ =====

    async def handle_account_input(self, event, account_identifier: str):
        """Обработка ввода идентификатора аккаунта"""
        user_id = event.sender_id

        user_data = self.user_sessions[user_id]
        user_data.target_account = account_identifier
        user_data.user_mode = UserMode.IDLE

        await event.respond(
            f"✅ **Аккаунт получен:** `{account_identifier}`\n\n"
            "Выберите тип данных для анализа:",
            buttons=[
                [Button.inline("📋 Базовая информация", b"account_basic"),
                 Button.inline("📊 Расширенная информация", b"account_full")],
                [Button.inline("🔍 Все данные", b"account_all")],
                [Button.inline("⬅️ Назад", b"main_menu")]
            ]
        )

    async def handle_account_analysis(self, event, data_type: AccountDataType):
        """Обработка анализа аккаунта"""
        user_id = event.sender_id
        user_data = self.user_sessions.get(user_id)

        if not user_data or not user_data.target_account:
            await event.respond("❌ Сначала введите идентификатор аккаунта")
            return

        user_data.user_mode = UserMode.ANALYZING_ACCOUNT
        user_data.account_data_type = data_type

        progress_msg = await event.respond("🔄 Начинаю анализ аккаунта...")

        try:
            entity = await self.get_account_entity(user_data.target_account)
            if not entity:
                await event.respond("❌ Не удалось найти указанный аккаунт")
                user_data.user_mode = UserMode.IDLE
                return

            analysis_results = {}

            if data_type == AccountDataType.BASIC_INFO:
                analysis_results = await self.analyze_account_basic_info(entity)
            elif data_type == AccountDataType.FULL_INFO:
                analysis_results = await self.analyze_account_full_info(entity)
            elif data_type == AccountDataType.ALL_DATA:
                analysis_results = await self.perform_full_account_analysis(entity)

            formatted_results = await self.format_account_analysis_results(analysis_results)

            if len(formatted_results) > 4000:
                parts = [formatted_results[i:i + 4000] for i in range(0, len(formatted_results), 4000)]
                for part in parts:
                    await event.respond(part)
            else:
                await event.respond(formatted_results)

            user_data.user_mode = UserMode.IDLE

        except Exception as e:
            logger.error(f"Ошибка анализа аккаунта: {e}")
            await event.respond(f"❌ Ошибка при анализе аккаунта: {str(e)}")
            user_data.user_mode = UserMode.IDLE
        finally:
            await progress_msg.delete()

    async def get_account_entity(self, account_identifier: str) -> Optional[Any]:
        """Получение entity аккаунта по username"""
        try:
            if account_identifier.startswith('@'):
                account_identifier = account_identifier[1:]

            entity = await self.safe_telegram_request(
                self.user_client.get_entity(account_identifier)
            )

            return entity

        except (ValueError, UserNotParticipantError, UsernameInvalidError, UsernameNotOccupiedError) as e:
            logger.error(f"Ошибка получения аккаунта {account_identifier}: {e}")
            return None
        except Exception as e:
            logger.error(f"Неизвестная ошибка получения аккаунта: {e}")
            return None

    async def analyze_account_basic_info(self, entity) -> Dict[str, Any]:
        """Анализ базовой информации аккаунта"""
        basic_info = {
            "Тип объекта": self.get_entity_type(entity),
            "ID": getattr(entity, 'id', 'Недоступно'),
            "Доступ": "Публичный" if isinstance(entity, (User, Channel)) else "Ограниченный"
        }

        if isinstance(entity, User):
            basic_info.update({
                "Основная информация": {
                    "Имя": getattr(entity, 'first_name', ''),
                    "Фамилия": getattr(entity, 'last_name', '') or "Не указана",
                    "Username": f"@{entity.username}" if getattr(entity, 'username', None) else "Не установлен",
                    "Бот": "Да" if getattr(entity, 'bot', False) else "Нет",
                    "Ограничен": "Да" if getattr(entity, 'restricted', False) else "Нет",
                    "Мошенник": "Да" if getattr(entity, 'scam', False) else "Нет",
                    "Флаг языка": getattr(entity, 'lang_code', 'Не установлен')
                },
                "Статус": self.parse_user_status(getattr(entity, 'status', None)),
                "Фото профиля": "Присутствует" if getattr(entity, 'photo', None) else "Отсутствует"
            })

        elif isinstance(entity, Channel):
            basic_info.update({
                "Основная информация": {
                    "Название": getattr(entity, 'title', ''),
                    "Username": f"@{entity.username}" if getattr(entity, 'username', None) else "Не установлен",
                    "Участников": getattr(entity, 'participants_count', 'Неизвестно'),
                    "Тип": "Супергруппа" if getattr(entity, 'megagroup', False) else "Канал",
                    "Верифицирован": "Да" if getattr(entity, 'verified', False) else "Нет",
                    "Ограничен": "Да" if getattr(entity, 'restricted', False) else "Нет",
                    "Мошенник": "Да" if getattr(entity, 'scam', False) else "Нет"
                }
            })

        return basic_info

    async def analyze_account_full_info(self, entity) -> Dict[str, Any]:
        """Анализ расширенной информации аккаунта"""
        full_info = {}

        try:
            if isinstance(entity, User):
                user_full = await self.safe_telegram_request(
                    self.user_client(GetFullUserRequest(entity))
                )

                if user_full:
                    full_info.update({
                        "Биография": getattr(user_full.full_user, 'about', 'Не указана') or "Не указана",
                        "Ссылка на профиль": f"https://t.me/{entity.username}" if getattr(entity, 'username',
                                                                                          None) else "Недоступна",
                        "Информация о фото": await self.analyze_profile_photo(user_full),
                        "Флаги и настройки": {
                            "Поддержка премиум": "Да" if getattr(user_full.full_user, 'premium', False) else "Нет",
                            "Может быть вызван": "Да" if getattr(user_full.full_user, 'phone_calls_available',
                                                                 False) else "Нет",
                            "Приватные звонки": "Да" if getattr(user_full.full_user, 'phone_calls_private',
                                                                False) else "Нет",
                            "Видео-аватар": "Да" if getattr(user_full.full_user, 'has_video_avatar', False) else "Нет"
                        }
                    })

            elif isinstance(entity, Channel):
                channel_full = await self.safe_telegram_request(
                    self.user_client(GetFullChannelRequest(entity))
                )

                if channel_full:
                    full_info.update({
                        "Описание": getattr(channel_full.full_chat, 'about', 'Не указано') or "Не указано",
                        "Статистика": {
                            "Участников": getattr(channel_full.full_chat, 'participants_count', 0),
                            "Онлайн": getattr(channel_full.full_chat, 'online_count', 0),
                            "Просмотров": getattr(channel_full.full_chat, 'views', 0)
                        },
                        "Ссылка приглашение": str(getattr(channel_full.full_chat, 'exported_invite', 'Недоступна')),
                        "Настройки": {
                            "Тип истории": "Доступна" if getattr(channel_full.full_chat, 'read_inbox_max_id',
                                                                 0) > 0 else "Недоступна",
                            "Скрытые участники": "Да" if getattr(channel_full.full_chat, 'hidden_prehistory',
                                                                 False) else "Нет"
                        }
                    })

        except Exception as e:
            logger.error(f"Ошибка получения полной информации: {e}")
            full_info["Ошибка"] = f"Не удалось получить полную информацию: {str(e)}"

        return full_info

    async def perform_full_account_analysis(self, entity) -> Dict[str, Any]:
        """Полный анализ всех общедоступных данных аккаунта"""
        full_analysis = {}

        analysis_methods = [
            ("Базовая информация", self.analyze_account_basic_info),
            ("Расширенная информация", self.analyze_account_full_info)
        ]

        for section_name, method in analysis_methods:
            try:
                full_analysis[section_name] = await method(entity)
                await asyncio.sleep(1.0)
            except Exception as e:
                full_analysis[section_name] = {"Ошибка": f"Не удалось получить данные: {str(e)}"}

        return full_analysis

    # Вспомогательные методы для анализа аккаунтов
    def get_entity_type(self, entity) -> str:
        if isinstance(entity, User):
            return "Пользователь"
        elif isinstance(entity, Channel):
            return "Канал" if not getattr(entity, 'megagroup', False) else "Супергруппа"
        elif isinstance(entity, Chat):
            return "Группа"
        else:
            return "Неизвестный тип"

    def parse_user_status(self, status) -> str:
        if not status:
            return "Неизвестно"
        if isinstance(status, UserStatusOnline):
            return "Онлайн"
        elif isinstance(status, UserStatusOffline):
            return f"Был в сети {status.was_online.strftime('%Y-%m-%d %H:%M')}"
        elif isinstance(status, UserStatusRecently):
            return "Был недавно"
        elif isinstance(status, UserStatusLastWeek):
            return "Был на прошлой неделе"
        elif isinstance(status, UserStatusLastMonth):
            return "Был в прошлом месяце"
        else:
            return "Неизвестно"

    async def analyze_profile_photo(self, user_full) -> str:
        if hasattr(user_full, 'profile_photo') and user_full.profile_photo:
            return "Присутствует (публичное)"
        return "Отсутствует"

    async def format_account_analysis_results(self, analysis_data: Dict[str, Any]) -> str:
        """Форматирование результатов анализа аккаунта"""
        lines = ["📊 **РЕЗУЛЬТАТЫ АНАЛИЗА АККАУНТА**", ""]

        for section, data in analysis_data.items():
            lines.append(f"🔹 **{section.upper()}**")
            lines.append("─" * 40)

            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, list):
                        lines.append(f"  📁 {key}: {len(value)} записей")
                        for item in value[:3]:
                            lines.append(f"    • {item}")
                        if len(value) > 3:
                            lines.append(f"    ... и еще {len(value) - 3} записей")
                    elif isinstance(value, dict):
                        lines.append(f"  📂 {key}:")
                        for subkey, subvalue in value.items():
                            lines.append(f"    • {subkey}: {subvalue}")
                    else:
                        lines.append(f"  • {key}: {value}")
            else:
                lines.append(str(data))

            lines.append("")

        lines.append("💡 *Собраны только общедоступные данные*")
        return "\n".join(lines)

    # ===== МЕТОДЫ АНАЛИЗА КАНАЛОВ =====

    async def handle_channel_link_input(self, event, channel_link: str):
        """Обработка ввода ссылки на канал"""
        user_id = event.sender_id

        if not self.is_valid_channel_link(channel_link):
            await event.respond(
                "❌ **Некорректная ссылка на канал!**\n\n"
                "📝 **Используйте один из форматов:**\n"
                "• `https://t.me/channel_name`\n"
                "• `@channel_name`\n"
                "• `t.me/channel_name`"
            )
            return

        user_data = self.user_sessions[user_id]
        user_data.current_channel_link = channel_link
        user_data.user_mode = UserMode.WAITING_POSTS_COUNT

        await event.respond(
            f"✅ **Канал получен:** `{channel_link}`\n\n"
            "⚡ **Выберите режим анализа:**\n\n"
            "• **Полный анализ** - все посты канала\n"
            "• **Выборочный анализ** - определенное количество\n"
            "• **Продолжить анализ** - с последней позиции",
            buttons=[
                [Button.inline("🚀 Полный анализ (ВСЕ посты)", b"full_analysis")],
                [Button.inline("📊 Выборочный анализ", b"custom_analysis")],
                [Button.inline("🔄 Продолжить анализ", b"continue_analysis")],
                [Button.inline("⬅️ Назад", b"main_menu")]
            ]
        )

    async def handle_posts_count(self, event, posts_count: int):
        """Обработка выбора количества постов"""
        user_id = event.sender_id
        user_data = self.user_sessions.get(user_id)

        if not user_data or user_data.user_mode != UserMode.WAITING_POSTS_COUNT:
            await event.respond("❌ Сессия устарела. Начните заново.")
            return

        user_data.posts_to_analyze = posts_count
        user_data.user_mode = UserMode.ANALYZING_CHANNEL

        progress_msg = await event.respond(
            f"🚀 **Запуск анализа {posts_count} постов...**\n\n"
            f"📺 Канал: `{user_data.current_channel_link}`\n"
            f"📊 Постов для анализа: `{posts_count}`\n"
            f"⏱️ Начато: `{datetime.now().strftime('%H:%M:%S')}`\n\n"
            f"⚡ *Инициализация ускоренного анализатора...*"
        )
        user_data.progress_message = progress_msg

        try:
            channel_entity = await self.get_channel_entity(user_data.current_channel_link)
            if not channel_entity:
                await event.respond("❌ Не удалось получить доступ к каналу.")
                user_data.user_mode = UserMode.IDLE
                return

            user_data.channel_entity = channel_entity

            asyncio.create_task(
                self.perform_optimized_channel_analysis(event, user_data, channel_entity, posts_count)
            )

        except Exception as e:
            logger.error(f"Ошибка при запуске анализа: {e}")
            if user_data.progress_message:
                try:
                    await user_data.progress_message.delete()
                except:
                    pass
            await event.respond(f"❌ Ошибка при запуске анализа: {str(e)}")
            user_data.user_mode = UserMode.IDLE

    async def handle_full_analysis(self, event):
        """Обработка полного анализа"""
        user_id = event.sender_id
        user_data = self.user_sessions.get(user_id)

        if not user_data:
            await event.respond("❌ Сессия устарела. Начните заново.")
            return

        user_data.posts_to_analyze = 0  # 0 = без ограничений
        user_data.user_mode = UserMode.ANALYZING_CHANNEL

        progress_msg = await event.respond(
            f"🚀 **ЗАПУСК ПОЛНОГО АНАЛИЗА КАНАЛА**\n\n"
            f"📺 Канал: `{user_data.current_channel_link}`\n"
            f"📊 Режим: `ВСЕ ПОСТЫ (без ограничений)`\n"
            f"⏱️ Начато: `{datetime.now().strftime('%H:%M:%S')}`\n\n"
            f"⚡ *Анализ будет продолжаться до полного сбора данных...*"
        )
        user_data.progress_message = progress_msg

        try:
            channel_entity = await self.get_channel_entity(user_data.current_channel_link)
            if not channel_entity:
                await event.respond("❌ Не удалось получить доступ к каналу.")
                user_data.user_mode = UserMode.IDLE
                return

            user_data.channel_entity = channel_entity

            asyncio.create_task(
                self.perform_unlimited_analysis(event, user_data, channel_entity)
            )

        except Exception as e:
            logger.error(f"Ошибка при запуске полного анализа: {e}")
            if user_data.progress_message:
                try:
                    await user_data.progress_message.delete()
                except:
                    pass
            await event.respond(f"❌ Ошибка при запуске полного анализа: {str(e)}")
            user_data.user_mode = UserMode.IDLE

    def is_valid_channel_link(self, link: str) -> bool:
        """Проверка валидности ссылки на канал"""
        patterns = [
            r'^https?://t\.me/[\w@]+',
            r'^https?://telegram\.me/[\w@]+',
            r'^t\.me/[\w@]+',
            r'^@[\w]+',
            r'^[\w]+$'
        ]
        return any(re.match(pattern, link) for pattern in patterns)

    async def get_channel_entity(self, channel_link: str) -> Optional[Channel]:
        """Получение entity канала по ссылке"""
        try:
            channel_link = self.normalize_channel_link(channel_link)
            if not channel_link:
                return None

            entity = await self.safe_telegram_request(
                self.user_client.get_entity(channel_link)
            )

            if isinstance(entity, Channel):
                logger.info(f"✅ Получен канал: {entity.title} (ID: {entity.id})")
                return entity
            else:
                logger.warning(f"⚠️ Ссылка ведет не в канал: {channel_link}")
                return None

        except (ChannelInvalidError, ChannelPrivateError, ValueError, UsernameNotOccupiedError) as e:
            logger.error(f"❌ Ошибка получения канала {channel_link}: {e}")
            return None

    def normalize_channel_link(self, channel_link: str) -> str:
        """Нормализация ссылки на канал"""
        if not channel_link:
            return ""

        channel_link = re.sub(r'^https?://(t\.me/|telegram\.me/)', '', channel_link)

        if channel_link.startswith('@'):
            channel_link = channel_link[1:]

        channel_link = channel_link.split('?')[0].split('#')[0]
        channel_link = channel_link.rstrip('/')

        return channel_link.strip()

    async def perform_unlimited_analysis(self, event, user_data, channel_entity):
        """Выполнение анализа без ограничений"""
        try:
            user_data.status = AnalysisStatus.ANALYZING
            user_data.total_comments_collected = 0
            user_data.total_posts_processed = 0

            # Получение ВСЕХ постов канала
            await self.update_progress_message(user_data, "📥 Получение ВСЕХ постов канала...", 0)

            all_posts = []
            async for message in self.user_client.iter_messages(channel_entity, limit=None):  # None = без ограничений
                all_posts.append(message)

            if not all_posts:
                await event.respond("❌ Не удалось получить посты из канала.")
                user_data.user_mode = UserMode.IDLE
                return

            total_posts = len(all_posts)
            successful_posts = 0

            await self.update_progress_message(user_data, f"🔍 Начало анализа {total_posts} постов...", 0)

            # Анализ комментариев для каждого поста
            for i, post in enumerate(all_posts):
                if user_data.user_mode != UserMode.ANALYZING_CHANNEL:
                    break

                try:
                    comments_found = await self.analyze_post_comments_optimized(user_data, post, channel_entity)
                    if comments_found:
                        successful_posts += 1

                    user_data.total_posts_processed = i + 1

                    # Обновляем прогресс каждые 10 постов или каждые 10%
                    if i % 10 == 0 or i == total_posts - 1:
                        progress_percent = (i + 1) / total_posts * 100
                        await self.update_progress_message(
                            user_data,
                            f"📊 Анализ поста {i + 1}/{total_posts}",
                            progress_percent
                        )

                    # Адаптивная задержка для избежания флуда
                    current_delay = max(0.1, min(1.0, (i + 1) / 1000))
                    await asyncio.sleep(current_delay)

                except Exception as e:
                    logger.error(f"Ошибка при анализе поста {post.id}: {e}")
                    continue

            # Отправка результатов
            await self.send_channel_analysis_results(event, user_data, successful_posts, total_posts)

        except Exception as e:
            logger.error(f"Ошибка анализа канала: {e}")
            await event.respond(f"❌ Ошибка анализа канала: {str(e)}")
        finally:
            user_data.status = AnalysisStatus.COMPLETED
            user_data.user_mode = UserMode.IDLE
            if user_data.progress_message:
                try:
                    await user_data.progress_message.delete()
                except:
                    pass

    async def perform_optimized_channel_analysis(self, event, user_data, channel_entity, posts_count):
        """Оптимизированный анализ с заданным количеством постов"""
        try:
            user_data.status = AnalysisStatus.ANALYZING
            user_data.total_comments_collected = 0
            user_data.total_posts_processed = 0

            # Получение постов канала
            await self.update_progress_message(user_data, "📥 Получение постов канала...", 0)

            posts = []
            async for message in self.user_client.iter_messages(channel_entity, limit=posts_count):
                posts.append(message)
                if len(posts) >= posts_count:
                    break

            if not posts:
                await event.respond("❌ Не удалось получить посты из канала.")
                user_data.user_mode = UserMode.IDLE
                return

            total_posts = len(posts)
            successful_posts = 0

            # Анализ комментариев для каждого поста
            for i, post in enumerate(posts):
                if user_data.user_mode != UserMode.ANALYZING_CHANNEL:
                    break

                try:
                    # Пропускаем посты без комментариев
                    if not hasattr(post, 'replies') or not post.replies or post.replies.replies == 0:
                        user_data.total_posts_processed = i + 1
                        continue

                    comments_found = await self.analyze_post_comments_optimized(user_data, post, channel_entity)
                    if comments_found:
                        successful_posts += 1

                    user_data.total_posts_processed = i + 1

                    # Обновляем прогресс
                    if i % 5 == 0 or i == total_posts - 1:
                        progress_percent = (i + 1) / total_posts * 100
                        await self.update_progress_message(
                            user_data,
                            f"📊 Анализ поста {i + 1}/{total_posts}",
                            progress_percent
                        )

                    await asyncio.sleep(0.5)

                except Exception as e:
                    logger.error(f"Ошибка при анализе поста {post.id}: {e}")
                    continue

            # Отправка результатов
            await self.send_channel_analysis_results(event, user_data, successful_posts, total_posts)

        except Exception as e:
            logger.error(f"Ошибка анализа канала: {e}")
            await event.respond(f"❌ Ошибка анализа канала: {str(e)}")
        finally:
            user_data.status = AnalysisStatus.COMPLETED
            user_data.user_mode = UserMode.IDLE
            if user_data.progress_message:
                try:
                    await user_data.progress_message.delete()
                except:
                    pass

    async def analyze_post_comments_optimized(self, user_data, post, channel_entity) -> bool:
        """Оптимизированный анализ комментариев к посту"""
        try:
            # Получаем комментарии через reply_to (самый эффективный метод)
            comments = await self.safe_telegram_request(
                self.user_client.get_messages(
                    channel_entity,
                    reply_to=post.id,
                    limit=None  # Без ограничений
                )
            )

            if not comments:
                return False

            user_data.total_comments_collected += len(comments)

            # Быстрая обработка авторов комментариев
            for comment in comments:
                await self.process_comment_author_fast(user_data, comment, post.id)

            logger.info(f"✅ Найдено {len(comments)} комментариев к посту {post.id}")
            return True

        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить комментарии для поста {post.id}: {e}")
            return False

    async def process_comment_author_fast(self, user_data, comment, post_id):
        """Быстрая обработка автора комментария"""
        try:
            if not hasattr(comment, 'sender_id'):
                return

            sender_id = comment.sender_id
            if not sender_id:
                return

            # Быстрое получение информации об авторе
            try:
                user_entity = await self.safe_telegram_request(
                    self.user_client.get_entity(sender_id)
                )
            except:
                return

            if not user_entity:
                return

            user_id = str(user_entity.id)

            # Генерация ссылки на комментарий
            comment_link = self.generate_comment_link(user_data.channel_entity, post_id, comment.id)

            if user_id not in user_data.found_users:
                user_data.found_users[user_id] = {
                    'entity': user_entity,
                    'comments_count': 0,
                    'first_seen': comment.date,
                    'last_seen': comment.date,
                    'comments': []
                }

            user_info = user_data.found_users[user_id]
            user_info['comments_count'] += 1
            user_info['last_seen'] = comment.date

            # Сохраняем только основные данные для скорости
            user_info['comments'].append({
                'post_id': post_id,
                'comment_id': comment.id,
                'date': comment.date,
                'text': getattr(comment, 'text', '')[:150] + '...' if getattr(comment, 'text', '') and len(
                    getattr(comment, 'text', '')) > 150 else getattr(comment, 'text', ''),
                'link': comment_link
            })

        except Exception as e:
            logger.error(f"❌ Ошибка обработки автора комментария: {e}")

    def generate_comment_link(self, channel_entity: Channel, post_id: int, comment_id: int) -> str:
        """Генерация прямой ссылки на комментарий"""
        try:
            if hasattr(channel_entity, 'username') and channel_entity.username:
                return f"https://t.me/{channel_entity.username}/{post_id}?comment={comment_id}"
            else:
                channel_id = abs(channel_entity.id) - 1000000000000
                return f"https://t.me/c/{channel_id}/{post_id}?comment={comment_id}"
        except Exception as e:
            logger.error(f"❌ Ошибка генерации ссылки: {e}")
            return f"https://t.me/c/{post_id}?comment={comment_id}"

    async def update_progress_message(self, user_data, status: str, progress: float):
        """Обновление сообщения о прогрессе"""
        try:
            if not user_data.progress_message:
                return

            progress_bar = self.create_progress_bar(progress)
            elapsed_time = (datetime.now() - user_data.analysis_start_time).total_seconds()

            message_text = (
                f"🚀 **Анализ канала в реальном времени**\n\n"
                f"📺 Канал: `{user_data.current_channel_link}`\n"
                f"📊 Статус: {status}\n"
                f"⏱️ Прошло времени: {elapsed_time:.0f} сек.\n"
                f"🔢 Запросов к API: {self.request_count}\n\n"
                f"{progress_bar} {progress:.1f}%\n\n"
                f"📈 **Прогресс:**\n"
                f"• Обработано постов: {user_data.total_posts_processed}\n"
                f"• Собрано комментариев: {user_data.total_comments_collected}\n"
                f"• Найдено авторов: {len(user_data.found_users)}\n"
                f"• Скорость: {user_data.total_comments_collected / max(elapsed_time, 1):.1f} комм/сек\n"
            )

            await user_data.progress_message.edit(message_text)

        except Exception as e:
            logger.error(f"❌ Ошибка обновления прогресса: {e}")

    def create_progress_bar(self, progress: float, length: int = 20) -> str:
        """Создание строки прогресс-бара"""
        filled = int(length * progress / 100)
        empty = length - filled
        return f"[{'█' * filled}{'░' * empty}]"

    async def send_channel_analysis_results(self, event, user_data, successful_posts: int, total_posts: int):
        """Отправка результатов анализа канала"""
        try:
            # Сортируем пользователей по количеству комментариев
            sorted_users = sorted(
                user_data.found_users.items(),
                key=lambda x: x[1]['comments_count'],
                reverse=True
            )

            total_analysis_time = (datetime.now() - user_data.analysis_start_time).total_seconds()

            # Основная статистика
            stats_text = (
                f"🎉 **ПОЛНЫЙ АНАЛИЗ КАНАЛА ЗАВЕРШЕН!**\n\n"
                f"📈 **Общая статистика:**\n"
                f"• 📺 Канал: `{user_data.current_channel_link}`\n"
                f"• 📄 Обработано постов: {total_posts}\n"
                f"• ✅ Успешных постов: {successful_posts}\n"
                f"• 💬 Собрано комментариев: {user_data.total_comments_collected}\n"
                f"• 👥 Найдено авторов: {len(user_data.found_users)}\n"
                f"• ⏱️ Время анализа: {total_analysis_time:.1f} сек.\n"
                f"• 🔢 Запросов к API: {self.request_count}\n"
                f"• 🚀 Скорость: {user_data.total_comments_collected / max(total_analysis_time, 1):.1f} комм/сек\n\n"
            )

            if sorted_users:
                stats_text += f"🏆 **Топ-15 комментаторов:**\n"
                # Добавляем топ-15 комментаторов
                for i, (user_id, user_info) in enumerate(sorted_users[:15], 1):
                    user = user_info['entity']
                    username = f"@{user.username}" if hasattr(user, 'username') and user.username else "без username"
                    name = getattr(user, 'first_name', '') or getattr(user, 'title', '') or f"User {user_id}"
                    stats_text += f"{i}. {name} ({username}) - {user_info['comments_count']} комм.\n"
            else:
                stats_text += "❌ Комментарии не найдены в обработанных постах.\n"

            await event.respond(stats_text)

            # Отправляем кнопки для просмотра детальной информации
            if user_data.found_users:
                await self.show_authors_list(event, user_data, 0)

        except Exception as e:
            logger.error(f"❌ Ошибка отправки результатов: {e}")
            await event.respond(f"❌ Ошибка формирования результатов: {str(e)}")

    async def show_authors_list(self, event, user_data, page: int):
        """Показ списка авторов с пагинацией"""
        try:
            sorted_users = sorted(
                user_data.found_users.items(),
                key=lambda x: x[1]['comments_count'],
                reverse=True
            )

            total_pages = (len(sorted_users) + MAX_BUTTONS_PER_PAGE - 1) // MAX_BUTTONS_PER_PAGE
            start_idx = page * MAX_BUTTONS_PER_PAGE
            end_idx = start_idx + MAX_BUTTONS_PER_PAGE
            users_page = sorted_users[start_idx:end_idx]

            buttons = []
            for user_id, user_info in users_page:
                user = user_info['entity']
                name = getattr(user, 'first_name', '') or getattr(user, 'title', '')
                if not name:
                    name = f"User {user_id}"

                button_text = f"{name} ({user_info['comments_count']})"
                if len(button_text) > 30:
                    button_text = button_text[:27] + "..."

                buttons.append([Button.inline(button_text, f"user_{user_id}_{page}")])

            # Кнопки навигации
            navigation_buttons = []
            if page > 0:
                navigation_buttons.append(Button.inline("⬅️ Назад", f"authors_{page - 1}"))
            if page < total_pages - 1:
                navigation_buttons.append(Button.inline("Вперед ➡️", f"authors_{page + 1}"))

            if navigation_buttons:
                buttons.append(navigation_buttons)

            buttons.append([Button.inline("🔙 Главное меню", "main_menu")])

            await event.respond(
                f"👥 **Список авторов комментариев (стр. {page + 1}/{total_pages}):**\n"
                f"Нажмите на пользователя для подробной информации",
                buttons=buttons
            )

        except Exception as e:
            logger.error(f"❌ Ошибка показа списка авторов: {e}")

    async def show_user_details(self, event, user_id: str, page: int):
        """Показ детальной информации о пользователе со ссылками на комментарии"""
        try:
            user_data = self.user_sessions.get(event.sender_id)
            if not user_data or user_id not in user_data.found_users:
                await event.respond("❌ Информация о пользователе не найдена.")
                return

            user_info = user_data.found_users[user_id]
            user_entity = user_info['entity']

            # Формируем информацию о пользователе
            user_details = self.format_user_info(user_entity, user_info)

            # Добавляем кнопку для просмотра комментариев со ссылками
            buttons = [
                [Button.inline("📝 Просмотреть комментарии со ссылками", f"user_comments_{user_id}_{page}")],
                [Button.inline("⬅️ Назад к списку", f"authors_{page}")],
                [Button.inline("🔙 Главное меню", "main_menu")]
            ]

            await event.respond(user_details, buttons=buttons)

        except Exception as e:
            logger.error(f"❌ Ошибка показа деталей пользователя: {e}")
            await event.respond("❌ Ошибка при получении информации о пользователе.")

    async def show_user_comments_page(self, event, user_identifier: str, page: int = 0):
        """Показ страницы с комментариями пользователя со ссылками"""
        try:
            user_data = self.user_sessions.get(event.sender_id)
            if not user_data or user_identifier not in user_data.found_users:
                await event.respond("❌ Данные устарели или автор не найден.")
                return

            user_info = user_data.found_users[user_identifier]
            all_comments = user_info['comments']

            if not all_comments:
                await event.respond("❌ Нет комментариев для отображения.")
                return

            # Сортируем комментарии по дате (новые сначала)
            all_comments.sort(key=lambda x: x['date'], reverse=True)

            # Пагинация
            total_pages = (len(all_comments) + COMMENTS_PER_PAGE - 1) // COMMENTS_PER_PAGE
            if page >= total_pages:
                page = total_pages - 1
            if page < 0:
                page = 0

            start_idx = page * COMMENTS_PER_PAGE
            end_idx = start_idx + COMMENTS_PER_PAGE
            page_comments = all_comments[start_idx:end_idx]

            # Формируем сообщение со ссылками
            message_text = f"💬 **Комментарии пользователя:** {user_info['entity'].first_name or user_info['entity'].title}\n\n"
            message_text += f"📄 Страница {page + 1} из {total_pages}\n\n"

            for i, comment in enumerate(page_comments, start=start_idx + 1):
                timestamp = comment['date'].strftime('%d.%m.%Y %H:%M')
                message_text += f"{i}. [{timestamp}]({comment['link']})\n"
                message_text += f"   {comment['text']}\n\n"

            # Создаем кнопки пагинации
            buttons = []
            nav_buttons = []
            if page > 0:
                nav_buttons.append(Button.inline("⬅️ Назад", f"user_comments_{user_identifier}_{page - 1}"))
            if page < total_pages - 1:
                nav_buttons.append(Button.inline("Вперед ➡️", f"user_comments_{user_identifier}_{page + 1}"))

            if nav_buttons:
                buttons.append(nav_buttons)

            buttons.append([Button.inline("⬅️ Назад к пользователю", f"user_{user_identifier}_0")])

            await event.edit(message_text, buttons=buttons, link_preview=True)

        except Exception as e:
            logger.error(f"❌ Ошибка показа комментариев: {e}")
            await event.respond("❌ Ошибка при загрузке комментариев.")

    def format_user_info(self, user_entity, user_info: Dict) -> str:
        """Форматирование информации о пользователе"""
        lines = []

        if isinstance(user_entity, User):
            lines.append(f"👤 **Информация о пользователе**")
            lines.append(f"🆔 ID: `{user_entity.id}`")
            lines.append(f"👤 Имя: {getattr(user_entity, 'first_name', '')}")
            if getattr(user_entity, 'last_name', ''):
                lines.append(f"👥 Фамилия: {user_entity.last_name}")
            if getattr(user_entity, 'username', ''):
                lines.append(f"🔗 Username: @{user_entity.username}")
            lines.append(f"🤖 Бот: {'✅ Да' if getattr(user_entity, 'bot', False) else '❌ Нет'}")

        elif isinstance(user_entity, Channel):
            lines.append(f"📺 **Информация о канале**")
            lines.append(f"🆔 ID: `{user_entity.id}`")
            lines.append(f"🏷️ Название: {getattr(user_entity, 'title', '')}")
            if getattr(user_entity, 'username', ''):
                lines.append(f"🔗 Username: @{user_entity.username}")
            lines.append(f"👥 Участников: {getattr(user_entity, 'participants_count', 'Неизвестно')}")

        lines.append("")
        lines.append("📊 **Статистика комментариев:**")
        lines.append(f"• 💬 Всего комментариев: {user_info['comments_count']}")
        lines.append(f"• 📅 Первый комментарий: {user_info['first_seen'].strftime('%Y-%m-%d %H:%M')}")
        lines.append(f"• 🕒 Последний комментарий: {user_info['last_seen'].strftime('%Y-%m-%d %H:%M')}")

        return "\n".join(lines)

    # ===== ОБРАБОТЧИКИ СОБЫТИЙ =====

    async def setup_handlers(self):
        """Настройка обработчиков событий"""

        @self.bot_client.on(events.NewMessage(pattern='/start'))
        async def start_handler(event):
            await self.send_welcome_message(event)

        @self.bot_client.on(events.NewMessage(pattern='/help'))
        async def help_handler(event):
            await self.send_welcome_message(event)

        @self.bot_client.on(events.NewMessage(pattern='/analyze_account'))
        async def analyze_account_handler(event):
            await self.handle_account_analysis_start(event)

        @self.bot_client.on(events.NewMessage(pattern='/analyze_channel'))
        async def analyze_channel_handler(event):
            await self.handle_channel_analysis_start(event)

        @self.bot_client.on(events.CallbackQuery)
        async def button_handler(event):
            user_id = event.sender_id
            data = event.data.decode('utf-8')

            try:
                if data == 'main_menu':
                    await self.send_welcome_message(event)

                elif data == 'channel_analysis':
                    await self.handle_channel_analysis_start(event)

                elif data == 'account_analysis':
                    await self.handle_account_analysis_start(event)

                elif data == 'help':
                    await self.send_welcome_message(event)

                elif data == 'full_analysis':
                    await self.handle_full_analysis(event)

                elif data == 'custom_analysis':
                    await event.respond(
                        "🔢 **Введите количество постов для анализа:**\n\n"
                        "💡 *Рекомендуем начинать с 100-200 постов для тестирования*\n"
                        "🚀 *Максимум: 5000 постов*",
                        buttons=[Button.inline("⬅️ Назад", b"main_menu")]
                    )

                elif data.startswith('posts_'):
                    if data == 'posts_custom':
                        await event.respond("🔢 Введите количество постов для анализа (числом):")
                        return

                    posts_count = int(data.split('_')[1])
                    await self.handle_posts_count(event, posts_count)

                elif data.startswith('account_'):
                    data_type_map = {
                        'account_basic': AccountDataType.BASIC_INFO,
                        'account_full': AccountDataType.FULL_INFO,
                        'account_all': AccountDataType.ALL_DATA
                    }

                    if data in data_type_map:
                        await self.handle_account_analysis(event, data_type_map[data])
                    else:
                        await event.respond("❌ Неизвестный тип анализа")

                elif data.startswith('authors_'):
                    page = int(data.split('_')[1])
                    user_data = self.user_sessions.get(user_id)
                    if user_data:
                        await self.show_authors_list(event, user_data, page)

                elif data.startswith('user_') and not data.startswith('user_comments_'):
                    parts = data.split('_')
                    user_id_str = parts[1]
                    page = int(parts[2])
                    await self.show_user_details(event, user_id_str, page)

                elif data.startswith('user_comments_'):
                    parts = data.split('_')
                    user_identifier = parts[2]
                    comment_page = int(parts[3])
                    await self.show_user_comments_page(event, user_identifier, comment_page)

                await event.answer()

            except Exception as e:
                logger.error(f"❌ Ошибка обработки кнопки: {e}")
                await event.respond("❌ Произошла ошибка. Попробуйте еще раз.")
                await event.answer()

        @self.bot_client.on(events.NewMessage)
        async def message_handler(event):
            user_id = event.sender_id
            user_data = self.user_sessions.get(user_id)

            if event.text.startswith('/'):
                return

            if not user_data:
                await self.send_welcome_message(event)
                return

            if user_data.user_mode == UserMode.WAITING_ACCOUNT_INPUT:
                await self.handle_account_input(event, event.text.strip())

            elif user_data.user_mode == UserMode.WAITING_CHANNEL_LINK:
                await self.handle_channel_link_input(event, event.text.strip())

            elif user_data.user_mode == UserMode.WAITING_POSTS_COUNT:
                try:
                    posts_count = int(event.text.strip())
                    if posts_count > 5000:
                        await event.respond("❌ Максимальное количество постов: 5000")
                        return
                    await self.handle_posts_count(event, posts_count)
                except ValueError:
                    await event.respond("❌ Пожалуйста, введите число для количества постов.")

            else:
                if self.is_valid_channel_link(event.text):
                    await self.handle_channel_analysis_start(event)
                else:
                    await event.respond(
                        "📝 Отправьте ссылку на канал для анализа или используйте /analyze_account для анализа аккаунта."
                    )

    async def run(self):
        """Запуск бота"""
        await self.initialize()
        await self.setup_handlers()

        logger.info("🚀 Бот полностью готов к работе!")
        logger.info("⏰ Запущен в: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        await self.bot_client.run_until_disconnected()


def main():
    """Основная функция запуска"""
    bot = PerfectTelegramAnalyzer()

    try:
        print("🚀 Запуск Professional Telegram Analyzer (БЕЗ ОГРАНИЧЕНИЙ)...")
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("🛑 Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
        print(f"💥 Произошла критическая ошибка: {e}")
    finally:
        print("👋 Работа бота завершена")


if __name__ == "__main__":
    main()