import json
from aiogram import Bot, Dispatcher, executor, types

import config
from app import aggregate_salary_data

bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher(bot)


# Обработчик команды /start
@dp.message_handler(commands="start")
async def start_message(message: types.Message):
    """
    После команды "/start" бот приветствует пользователя
    """
    text = f'Привет, {message.from_user.full_name}!'
    await message.answer(text)


@dp.message_handler()
async def handle_text(message: types.Message):
    """
    Обработка текстовых сообщений и возврат данных
    """
    try:
        data = json.loads(message.text)
        dt_from = data['dt_from']
        dt_upto = data['dt_upto']
        group_type = data['group_type']
        result = aggregate_salary_data(dt_from, dt_upto, group_type)
        await message.answer(f'{result}')
    except Exception as e:
        await message.answer(f'Ошибка обработки данных: {e}')


# Запуск бота
if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)