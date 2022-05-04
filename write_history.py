import asyncio
from datetime import datetime

import pandas as pd
from tinkoff.invest import (
    AsyncClient,
    TradeInstrument,
    MarketDataRequest,
    SubscribeTradesRequest,
    SubscriptionAction
)

import settings
from utils.convert import Convert

pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.width = None

INSTRUMENTS = [
    {'name': 'USD000UTSTOM', 'figi': 'BBG0013HGFT4'},
    {'name': 'SBER', 'figi': 'BBG004730N88'},
    {'name': 'GAZP', 'figi': 'BBG004730RP0'},
]


async def main():
    df = pd.DataFrame(columns=['figi', 'direction', 'price', 'quantity', 'time'])
    df.time = pd.to_datetime(df.time, unit='ms')
    df.price = pd.to_numeric(df.price)
    df.quantity = pd.to_numeric(df.quantity)

    instrument_files = {}
    for instrument in INSTRUMENTS:
        file_name = f'./data/{instrument["name"]}-{datetime.now().strftime("%Y%m%d")}.csv'
        instrument_files[instrument['figi']] = open(file_name, 'a', newline='')

    with open(f'./data/all-{datetime.now().strftime("%Y%m%d")}.csv', 'a', newline='') as all_instrument_file:
        async def request_iterator():
            yield MarketDataRequest(
                subscribe_trades_request=SubscribeTradesRequest(
                    subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                    instruments=[
                        TradeInstrument(
                            # USD000UTSTOM
                            figi="BBG0013HGFT4"
                        ),
                        TradeInstrument(
                            # SBER: Акции обыкновенные ПАО Сбербанк
                            figi="BBG004730N88"
                        ),
                        TradeInstrument(
                            # GAZP: Акции обыкновенные ПАО "Газпром"
                            figi="BBG004730RP0"
                        ),
                    ],
                )
            )
            while True:
                await asyncio.sleep(1)

        try:
            async with AsyncClient(settings.TOKEN) as client:
                async for marketdata in client.market_data_stream.market_data_stream(
                        request_iterator()
                ):
                    if marketdata is None:
                        continue
                    trade = marketdata.trade
                    if trade is None:
                        continue

                    print(trade, flush=True)

                    price = Convert.quotation_to_price(trade.price)
                    data = pd.DataFrame.from_records([
                        {
                            'figi': trade.figi,
                            'direction': trade.direction,
                            'price': price,
                            'quantity': trade.quantity,
                            'time': trade.time,
                        }
                    ])
                    data.to_csv(all_instrument_file, header=all_instrument_file.tell() == 0, index=False)
                    data.to_csv(instrument_files[trade.figi], header=instrument_files[trade.figi].tell() == 0,
                                index=False)
                    # df = pd.concat([df, data])
        except Exception as ex:
            print(ex, flush=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as ex:
        print(ex, flush=True)
