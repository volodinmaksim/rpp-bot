from aiogram.fsm.state import StatesGroup, State


class StoryState(StatesGroup):
    waiting_for_subscription = State()
    waiting_15min_pause = State()
    waiting_for_extra_materials = State()
    choosing_experience = State()

    novice_path = State()  # Тексты №1, №2, №3
    pro_path = State()  # Тексты №7, №8, №9

    # --- Опрос "Не заспамили?"  ---
    waiting_for_survey_response = State()  # Да / Нет
    waiting_for_wishes = State()  # Ожидание текста с пожеланиями (после "Да")

    final_stage = State()  # Тексты №4-№7
    waiting_for_feedback_query = State()
