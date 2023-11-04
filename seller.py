import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон.

    Args:
        last_id (str): Последний ID товара.
        client_id (str): ID клиента
        seller_token (str): Персональный токен продавца

    Returns: Возвращает словарь
        dict:
        {
        "result":
        {
            "items":
            [
            {
                "product_id": 223681945,
                "offer_id": "136748"
            }
        ],
        "total": 1,
        "last_id": "bnVсbA=="
        }

        }
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон.

    Args:
        client_id (str): ID клиента
        seller_token (str): Персональный токен продавца

    Returns:
        list: Возвращает список ID предложений
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id: str, seller_token: str):
    """Обновить цены товаров.

    Функция обновляет цены товара на Озон

    Args:
        prices (list): Список цен
        client_id (str): Персональный ID клиента
        seller_token (str)Ж Персональный токен продавца

    Returns:
        json: Возвращает json с ценами.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки.

    Функция делает запрос через апи озона и обновляет остатки

    Args:
        stocks (list): список запасов.
        client_id (str): Персональный id клиента
        seller_token (str): Персональный токен продавца.

    Returns:
        json: Возвращает json-строку.

    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Функция скачивает файл ostatki с сайта casio."""
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants: list, offer_ids: list) -> list[dict]:
    """Функция создает список запасов.

    Args:
        watch_remnants (list):
        offer_ids (list): Список ИД товаров

    Returns:
        stocks (list[dict]): Список остатков.
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants: dict, offer_ids: list) -> list[dict[str, str]]:
    """Функция создает список цен.

    Args:
        watch_remnants (dict): словарь с остатками часов
        offer_ids (list): список с id товарами.

    Returns:
        list(dict[str, str]): Список цен.
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовывает цену.

    Args:
        price (str): 5'990.00 руб

    Returns:
        str: 5990

    Raises:
        AttributeError: Должна быть строка.
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список на части.

    Разделить список lst на части по n элементов

    Args:
        lst (list): [1, 2, 3, 4, 5]
        n (int): 4

    Returns:
        <generator object divide at>:

    Raises:
        AttributeError: Должна быть список, и int.

    Examples:
        Этот пример показывает как использовать функцию.

        >>> list1 = list(range(10))
        >>> n = 2
        >>> a = divide(list1, n)
        >>> a
        >>> <generator object divide at 0x7f149f439e00>
        >>> list(a)
        >>> [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Функция асинхронно создает список цен.

    Args:
        watch_remnants (dict): словарь с остатками часов
        client_id (str): Строка с ID клиенто
        seller_token (str): Персональный токен продавца

    Returns:
        list[dict[str, str]]: список содержащий словари.

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Функция асинхронно создает список запасов.

    Args:
        watch_remnants (dict): словарь с остатками часов
        client_id (str): Строка с ID клиенто
        seller_token (str): Персональный токен продавца

    Returns:
        list: Не пустой список
        list: список содержащий словари.

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Ключевая функция запуска скрипта."""
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
