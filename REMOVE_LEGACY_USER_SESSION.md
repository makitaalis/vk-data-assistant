Удаление старой сессии `user_session`:

- Удалён каталог `data/sessions/user_session`.
- Очищен флаг `bot:session:enabled:user_session` в Redis, чтобы сессия не отображалась в админ-меню.
