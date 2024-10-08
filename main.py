import asyncio
import aioconsole
import aiohttp
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import pandas as pd
from datetime import datetime
import os

# Определяем модель базы данных
DATABASE_URL = "sqlite+aiosqlite:///weather_data.db"
engine = create_async_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

# Определяем модель WeatherData
class WeatherData(sa.orm.declarative_base()):
    __tablename__ = "weather"

    id = sa.Column(sa.Integer, primary_key=True, index=True)
    temperature = sa.Column(sa.Float)
    wind_speed = sa.Column(sa.Float)
    wind_direction = sa.Column(sa.String)
    pressure = sa.Column(sa.Float)
    precipitation_rain= sa.Column(sa.Float)
    precipitation_snow = sa.Column(sa.Float)
    timestamp = sa.Column(sa.DateTime, default=datetime.utcnow)


# Функция для создания таблиц
async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(WeatherData.metadata.create_all)


# Асинхронная функция для запроса данных о погоде
async def fetch_weather():
    async with aiohttp.ClientSession() as session:
        while True:
            # Создаем запрос к сервису api.open-meteo и сохраняем в переменную ответа - response
            async with session.get("https://api.open-meteo.com/v1/forecast?latitude=55.69901105&longitude=37.359583750315124&current=temperature_2m,precipitation,rain,snowfall,weather_code,surface_pressure,wind_speed_10m,wind_direction_10m&timezone=Europe%2FMoscow") as response:
                # Преобразование ответа сервера в формат JSON и получение конкретных данных по ключу 'current'
                data = await response.json()
                current_weather = data['current']

                # Вызов и ожидание выполнения функции сохранения данных в БД
                await save_weather_to_db(current_weather)
            # Добавляем задержку выполнения (например на 3 минуты)
            await asyncio.sleep(180)


# Функция получения стороны света по переменной - градусам
def get_direction_by_degree(degree):
    directions = [
        (0, 'C'),
        (22.5, 'CВ'),
        (45, 'В'),
        (67.5, 'ЮВ'),
        (90, 'Ю'),
        (112.5, 'ЮЗ'),
        (135, 'З'),
        (157.5, 'СЗ'),
        (180, 'С')
    ]
    degrees = degree % 360
    for i in range(len(directions)):
        if degrees < directions[i][0]:
            return directions[i - 1][1] if i > 0 else directions[0][1]

    # Если угол больше 337.5, возвращаем север
    return directions[-1][1]

# Функция для сохранения данных в БД
async def save_weather_to_db(weather):
    async with SessionLocal() as session:
        async with session.begin():
            wind_direction = get_direction_by_degree(weather['wind_direction_10m'])

            weather_entry = WeatherData(
                    temperature=weather['temperature_2m'],
                    wind_speed=weather['wind_speed_10m'],
                    wind_direction=wind_direction,
                    pressure = weather['surface_pressure'],
                    precipitation_rain=weather['rain'],
                    precipitation_snow=weather['snowfall'],
                )
            session.add(weather_entry)


# Функция для экспорта данных в Excel
async def export_to_excel():
    async with SessionLocal() as session:
        async with session.begin():
            query = await session.execute(sa.select(WeatherData).order_by(WeatherData.timestamp.desc()))
            data = query.scalars().all()
        # Создание pandas фрейма
        df = pd.DataFrame([{
            "Temperature": wd.temperature,
            "Wind Speed": wd.wind_speed,
            "Wind Direction": wd.wind_direction,
            "Pressure": wd.pressure,
            "Precipitation Rain": wd.precipitation_rain,
            "Precipitation Snow": wd.precipitation_snow,
            "Timestamp": wd.timestamp
        } for wd in data])
        # Экспорт фрейма в файл xlsx
        df.to_excel(os.path.join(os.getcwd(), 'weather_data.xlsx'), index=False)


# Асинхронная функция ожидания ввода от пользователя
async def get_input():
    while True:
        user_input = await aioconsole.ainput("Введите команду: ")
        if user_input.lower() == "export":
            print("Экспорт данных в таблицу...")
            await export_to_excel()
            break


# Основная функция
async def main():
    await create_tables()  # Создаем таблицы перед началом работы
    fetch_task = asyncio.create_task(fetch_weather())
    get_user_input = asyncio.create_task(get_input())

    while True:
        await fetch_task
        await get_user_input()


if __name__ == "__main__":
    asyncio.run(main())
