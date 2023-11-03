import os
import json
import asyncio
import environ
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message
from dotenv import load_dotenv
from aggregation import aggregate_data


environ.Env.read_env()
load_dotenv()
BOT_TOKEN = os.environ.get('BOT_TOKEN')
dp = Dispatcher()


@dp.message(CommandStart())
async def on_start(message: Message):
  await message.answer(f"Добрый день, {message.from_user.full_name}! Введите данные для агрегации")
  

@dp.message()
async def on_aggregate(message: Message):
  try:
    data = json.loads(message.text.replace("'", '"'))

    if 'dt_from' in data and 'dt_upto' in data and 'group_type' in data:
      result = await aggregate_data(data['dt_from'], data['dt_upto'], data['group_type'])
      await message.answer(str(result)) if result else message.answer("Нету данных")
    else:
      await message.answer("Отсутствуют необходимые данные")
      
  except json.JSONDecodeError:
    await message.answer("Вы передали неверный формат данных, повторите попытку")
  except Exception:
    await message.answer("Произошла ошибка")


async def main():
  bot = Bot(BOT_TOKEN)
  await dp.start_polling(bot)


if __name__ == "__main__":
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main())