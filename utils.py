from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def chunked(data, size):
    for i in range(0, len(data), size):
        yield data[i : i + size]


def get_yesterday_bounds_msk(ts: str):
    """
    Принимает строку с UTC-временем (ISO8601),
    возвращает (date_from, date_to) — календарные даты вчерашнего дня по МСК.
    Оба значения одинаковые → закрытый интервал [вчера, вчера].
    """
    # парсим входное UTC-время
    dt_utc = datetime.fromisoformat(ts)

    # часовой пояс МСК
    msk_zone = ZoneInfo("Europe/Moscow")

    # конвертируем в МСК
    dt_msk = dt_utc.astimezone(msk_zone)

    # вчерашняя дата по МСК
    yesterday = dt_msk.date() - timedelta(days=1)

    return yesterday, yesterday


def get_yesterday_bounds_utc(ts: str):
    """
    Принимает строку с UTC-временем (ISO8601),
    возвращает (date_from, date_to) — UTC-границы вчерашнего дня по МСК.
    """
    # парсим входное UTC-время
    dt_utc = datetime.fromisoformat(ts)

    # часовой пояс МСК
    msk_zone = ZoneInfo("Europe/Moscow")

    # конвертируем в МСК
    dt_msk = dt_utc.astimezone(msk_zone)

    # вычисляем "вчера" по МСК
    yesterday = dt_msk.date() - timedelta(days=1)

    # границы дня (по МСК)
    start_msk = datetime.combine(yesterday, datetime.min.time(), tzinfo=msk_zone)
    end_msk = datetime.combine(yesterday, datetime.max.time(), tzinfo=msk_zone)

    # переводим обратно в UTC
    date_from = start_msk.astimezone(timezone.utc)
    date_to = end_msk.astimezone(timezone.utc)

    return date_from, date_to
