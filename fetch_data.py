import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp

from utils import get_yesterday_moscow_from_utc

GET_FBS_ORDERS = "https://marketplace-api.wildberries.ru/api/v3/orders"


async def fetch_data(api_token: str, ts: str) -> list:
    headers = {
        "Authorization": api_token
    }
    limit = 1000

    next_val = 0
    all_orders = []

    yesterday = get_yesterday_moscow_from_utc(ts)

    date_from = datetime.strptime(yesterday, "%Y-%m-%d")
    date_to = date_from + timedelta(days=1)

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            params = {
                "limit": limit,
                "next": next_val,
                "dateFrom": int(date_from.timestamp()),
                "dateTo": int(date_to.timestamp()),
            }

            data = await fetch_page_with_retry(session, GET_FBS_ORDERS, params)

            orders = data.get("orders", [])
            all_orders.extend(orders)

            next_val = data.get("next", 0)
            logging.info("Next value: %s", next_val)
            if not orders or next_val == 0:
                break

    return all_orders


async def fetch_page_with_retry(session, url, params):
    while True:
        async with session.get(url, params=params) as response:
            if response.status == 429:
                retry_after = int(response.headers.get('X-Ratelimit-Retry', 10))
                logging.warning(f"Rate limited (429). Retrying after {retry_after} seconds...")
                await asyncio.sleep(retry_after)
                continue

            response.raise_for_status()
            return await response.json()
