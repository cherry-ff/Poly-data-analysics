import requests
import json
import time
from decimal import Decimal

URL = "https://data.chain.link/api/query-timescale"

FEED_ID = "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8"

params = {
    "query": "LIVE_STREAM_REPORTS_QUERY",
    "variables": json.dumps({"feedId": FEED_ID}),
}

SCALE = Decimal("1e18")


def parse_price(raw):
    return float(Decimal(raw) / SCALE)


while True:
    try:
        r = requests.get(URL, params=params, timeout=3)
        r.raise_for_status()

        data = r.json()

        nodes = data.get("data", {}).get("liveStreamReports", {}).get("nodes", [])

        results = []

        for node in nodes:
            item = {
                "timestamp": node["validFromTimestamp"],
                "price": parse_price(node["price"]),
                "bid": parse_price(node["bid"]),
                "ask": parse_price(node["ask"]),
            }
            results.append(item)

        print(json.dumps(results, ensure_ascii=False, indent=2))

        time.sleep(1)

    except Exception as e:
        print({"error": str(e)})
        time.sleep(1)