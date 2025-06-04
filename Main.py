"""
Telegram-бот для парсинга вакансий с hh.ru
Автор: Ваше имя VaSeBa
Версия: 1.0
"""

# Импорт стандартных библиотек
import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from api_token import TOKEN

# Сторонние библиотеки
import pandas as pd
import aiohttp  # Для асинхронных HTTP-запросов
from dateutil import parser  # Парсинг дат
from aiogram import Bot, Dispatcher, types, F  # Фреймворк для Telegram API
from aiogram.filters import Command  # Обработка команд
from aiogram.fsm.context import FSMContext  # Контекст состояний
from aiogram.fsm.state import StatesGroup, State  # Система состояний
from aiogram.types import FSInputFile  # Отправка файлов
from openpyxl import load_workbook  # Работа с Excel
from openpyxl.worksheet.table import Table, TableStyleInfo  # Форматирование таблиц

# Настройка логирования для отслеживания ошибок
logging.basicConfig(level=logging.INFO)

# Инициализация бота и диспетчера
bot = Bot(token=TOKEN)  # Замените на реальный токен
dp = Dispatcher()

# Конфигурация парсера
API_URL = "https://api.hh.ru/vacancies"  # Основной URL API
AREA = 113  # Код России в API hh.ru
MAX_RETRIES = 3  # Максимальное количество повторов при ошибках
REQUEST_DELAY = 0.25  # Задержка между запросами (в секундах)
DATE_RANGE_DAYS = 30  # Период поиска (дней)


# Определение состояний бота с помощью Finite State Machine (FSM)
class ParseStates(StatesGroup):
    waiting_for_profession = State()  # Состояние ожидания ввода профессии


"""
Класс HHruParser реализует основную логику парсинга.
Использует асинхронные методы для эффективной работы с сетью.
"""


class HHruParser:
    def __init__(self, profession: str, chat_id: int):
        self.profession = profession  # Искомая профессия
        self.chat_id = chat_id  # ID чата для отправки результатов
        self.running = True  # Флаг для остановки парсинга
        self.total_vacancies = 0  # Счетчик найденных вакансий

    async def parse(self):
        """Основной метод запуска парсинга"""
        try:
            # Рассчитываем диапазон дат
            end_date = datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            start_date = end_date - timedelta(days=DATE_RANGE_DAYS)

            # Генерируем интервалы для поиска
            date_intervals = list(self.date_range(start_date, end_date))
            total_intervals = len(date_intervals)

            all_vacancies = []  # Список для сбора вакансий

            # Основной цикл обработки интервалов
            for i, (date_from, date_to) in enumerate(date_intervals):
                if not self.running:
                    return  # Выход при остановке

                # Отправка прогресса и обработка данных
                progress = int((i + 1) / total_intervals * 100)
                await self.send_progress(progress)

                vacancies = await self.get_vacancies(
                    date_from=date_from.isoformat(),
                    date_to=date_to.isoformat()
                )

                all_vacancies.extend(vacancies)
                await self.send_status_update(date_from, date_to, len(vacancies))
                await asyncio.sleep(0.5)  # Задержка между запросами

            # Финализация и отправка результатов
            await self.finalize_parsing(all_vacancies)

        except Exception as e:
            await self.handle_error(e)

    async def get_vacancies(self, date_from: str, date_to: str) -> list:
        """Асинхронное получение вакансий через API"""
        params = {
            "text": self.profession,
            "area": AREA,
            "date_from": date_from,
            "date_to": date_to,
            "per_page": 100,  # Максимум на страницу
            "page": 0
        }

        vacancies = []
        retries = MAX_RETRIES  # Счетчик попыток

        # Используем контекстный менеджер для сессии
        async with aiohttp.ClientSession() as session:
            while self.running and retries > 0:
                try:
                    async with session.get(
                            API_URL,
                            params=params,
                            timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        # Обработка статусных кодов
                        if response.status == 403:
                            await self.handle_rate_limit()
                            continue

                        response.raise_for_status()  # Проверка ошибок HTTP
                        data = await response.json()

                        # Обработка пагинации
                        vacancies.extend(data.get("items", []))
                        if params["page"] >= data.get("pages", 1) - 1:
                            break

                        params["page"] += 1
                        await asyncio.sleep(REQUEST_DELAY)
                        retries = MAX_RETRIES  # Сброс счетчика

                # Обработка сетевых ошибок
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    retries -= 1
                    await self.handle_network_error(e, retries)
                except Exception as e:
                    await self.handle_general_error(e)
                    break

        return vacancies

    @staticmethod
    def date_range(start_date: datetime, end_date: datetime,
                   delta: timedelta = timedelta(days=7)):
        """Генератор интервалов дат с заданным шагом"""
        current_date = start_date
        while current_date < end_date:
            next_date = min(current_date + delta, end_date)
            yield (current_date, next_date)
            current_date = next_date

    async def save_to_excel(self, vacancies: list) -> str:
        """Сохранение результатов в Excel с форматированием"""
        # Подготовка данных
        data = []
        for item in vacancies:
            # Безопасное извлечение данных
            salary = item.get("salary") or {}
            employer = item.get("employer") or {}
            area = item.get("area") or {}

            # Парсинг даты публикации
            try:
                pub_date = parser.parse(item.get("published_at", "")).strftime("%d.%m.%Y %H:%M")
            except (ValueError, TypeError):
                pub_date = "N/A"

            data.append({
                "Название": item.get("name"),
                "Компания": employer.get("name"),
                "Зарплата от": salary.get("from"),
                "Зарплата до": salary.get("to"),
                "Валюта": salary.get("currency"),
                "Регион": area.get("name"),
                "Дата публикации": pub_date,
                "Ссылка": item.get("alternate_url")
            })

        # Создание безопасного имени файла
        safe_name = "".join(
            [c if c.isalnum() or c in ('_', '-') else '_'
             for c in self.profession]).rstrip('_')
        file_name = f"{safe_name}_vacancies.xlsx"

        # Сохранение через pandas
        df = pd.DataFrame(data)
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Вакансии')

        # Форматирование таблицы
        wb = load_workbook(file_name)
        ws = wb.active
        tab = Table(displayName="VacanciesTable", ref=f"A1:H{len(data) + 1}")
        tab.tableStyleInfo = TableStyleInfo(
            name="TableStyleMedium9",
            showRowStripes=True
        )
        ws.add_table(tab)
        wb.save(file_name)

        return file_name

    async def send_progress(self, progress: int):
        """Отправка обновлений о прогрессе"""
        try:
            await bot.send_message(
                self.chat_id,
                f"📊 Прогресс: {progress}%"
            )
        except Exception as e:
            logging.error(f"Ошибка отправки прогресса: {str(e)}")

    async def send_status_update(self, date_from, date_to, count):
        """Отправка информации о количестве найденных вакансий"""
        await bot.send_message(
            self.chat_id,
            f"📅 Период: {date_from.date()} - {date_to.date()}\n"
            f"🔍 Найдено вакансий: {count}"
        )

    async def finalize_parsing(self, vacancies):
        """Финализация процесса парсинга"""
        if vacancies:
            file_path = await self.save_to_excel(vacancies)
            await self.send_results(file_path, len(vacancies))
            os.remove(file_path)  # Удаление временного файла
        else:
            await bot.send_message(
                self.chat_id,
                "😞 Не найдено подходящих вакансий"
            )

    async def send_results(self, file_path, count):
        """Отправка результатов пользователю"""
        await bot.send_document(
            self.chat_id,
            FSInputFile(file_path),
            caption=f"✅ Готово! Найдено вакансий: {count}"
        )

    async def handle_error(self, error):
        """Обработка критических ошибок"""
        await bot.send_message(
            self.chat_id,
            f"❌ Критическая ошибка: {str(error)}"
        )

    async def handle_rate_limit(self):
        """Обработка превышения лимита запросов"""
        await bot.send_message(
            self.chat_id,
            "⚠️ Превышен лимит запросов! Ждем 10 секунд..."
        )
        await asyncio.sleep(10)

    async def handle_network_error(self, error, retries):
        """Обработка сетевых ошибок"""
        await bot.send_message(
            self.chat_id,
            f"⚠️ Ошибка соединения: {str(error)}\n"
            f"Попыток осталось: {retries}"
        )
        await asyncio.sleep(5)

    async def handle_general_error(self, error):
        """Обработка общих ошибок"""
        await bot.send_message(
            self.chat_id,
            f"⚠️ Ошибка: {str(error)}"
        )

    def stop(self):
        """Остановка парсинга"""
        self.running = False


# ==============================================
# Обработчики команд Telegram
# ==============================================

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Обработчик команды /start"""
    await message.answer(
        "🔍 Добро пожаловать в бот для поиска вакансий с hh.ru!\n"
        "📌 Используйте команду /parse для начала поиска"
    )


@dp.message(Command("parse"))
async def cmd_parse(message: types.Message, state: FSMContext):
    """Обработчик команды /parse"""
    await message.answer(
        "📝 Введите название профессии для поиска:"
    )
    await state.set_state(ParseStates.waiting_for_profession)


@dp.message(ParseStates.waiting_for_profession)
async def process_profession(message: types.Message, state: FSMContext):
    """Обработка введенной профессии"""
    profession = message.text.strip()
    if not profession:
        await message.answer("⚠️ Пожалуйста, введите корректное название профессии!")
        return

    await state.clear()

    # Создание и запуск задачи парсинга
    msg = await message.answer("⏳ Идет поиск вакансий...")
    parser = HHruParser(profession, message.chat.id)
    task = asyncio.create_task(parser.parse())

    # Ожидание завершения и удаление сообщения
    await task
    await msg.delete()


@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    """Обработчик команды /cancel"""
    current_state = await state.get_state()
    if current_state is None:
        return

    await state.clear()
    await message.answer("❌ Операция отменена")


# ==============================================
# Запуск бота
# ==============================================

async def main():
    """Основная функция запуска бота"""
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
