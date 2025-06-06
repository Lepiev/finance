import requests, pandas as pd
from pathlib import Path
from datetime import date


def api_request(stock, data_from, data_to, board="TQBR"):
    """
    Возвращает свечи акции stock с data_from до data_to
    """

    # адрес по которому делается запрос
    base = (
        f"https://iss.moex.com/iss/engines/stock/markets/shares/"
        f"boards/{board}/securities/{stock}/candles.json"
        f"?from={data_from}&till={data_to}&interval=24"
    )

    # так как запрос возвращает только 500 строк за один запрос,
    # будем хранить каждый запрос, и склеивать их

    piece, start = [], 0
    while True:
        url = f"{base}&start={start}"  # start - начало следующего куска
        j = requests.get(url, timeout=10).json()
        """
        в j храниться словарь информации о 
        {
        "candles": {
            "metadata": { … },              # Описание полей, типы и т. п.
            "columns": [                    # Список имён колонок
            "begin", "end", "open", "high",
            "low", "close", "volume", "value", …
            ],
            "data": [                       # Сами данные — список «строк»
            [
                "2020-01-02T10:00:00",      # begin
                "2020-01-03T10:00:00",      # end
                274.1,                      # open
                276.5,                      # high
                273.2,                      # low
                275.8,                      # close
                1234567,                    # volume
                340000000,                  # value
                …
            ],
            …                             # ещё до 500 таких вложенных списков
            ]
        }
        }

        """
        cols, data = j["candles"]["columns"], j["candles"]["data"]
        if not data:
            break
        df = pd.DataFrame([{k: r[i] for i, k in enumerate(cols)} for r in data])
        piece.append(df)
        if len(data) < 500:  # дошли до конца
            break
        start += 500  # следующий

    # объеденяем все dataframe которые мы по 500 строк собирали
    full = pd.concat(piece, ignore_index=True)

    # Превращаем колонку begin в datetime + делаем её индексом
    full["begin"] = pd.to_datetime(full["begin"])
    full = full.set_index("begin").sort_index()
    return full


def get_number(ticker: str) -> int:
    """
    Возвращает количество акций в обращении для тикера ticker.
    """
    url = (
        f"https://iss.moex.com/iss/engines/stock/markets/shares/"
        f"securities/{ticker}.json?iss.only=securities"
    )
    j = requests.get(url, timeout=10).json()
    cols, row = j["securities"]["columns"], j["securities"]["data"][0]
    info = dict(zip(cols, row))
    for k in ("issue_size", "issuesize", "ISSUESIZE"):
        if k in info and info[k]:
            return int(info[k])
    raise KeyError(f"{ticker}: поле issue_size не найдено")
