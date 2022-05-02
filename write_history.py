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


async def main():
    df = pd.DataFrame(columns=['figi', 'direction', 'price', 'quantity', 'time'])
    df.time = pd.to_datetime(df.time, unit='ms')
    df.price = pd.to_numeric(df.price)
    df.quantity = pd.to_numeric(df.quantity)

    with open(f'./data/statistics-{datetime.now().strftime("%Y%m%d")}.csv', 'a', newline='') as file:
        async def request_iterator():
            yield MarketDataRequest(
                subscribe_trades_request=SubscribeTradesRequest(
                    subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                    instruments=[
                        TradeInstrument(
                            figi="BBG0013HGFT4"
                        )
                    ],
                )
            )
            while True:
                await asyncio.sleep(1)

        async with AsyncClient(settings.TOKEN) as client:
            async for marketdata in client.market_data_stream.market_data_stream(
                    request_iterator()
            ):
                if marketdata is None:
                    continue
                trade = marketdata.trade
                if trade is None:
                    continue

                print(trade)

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
                data.to_csv(file, header=file.tell() == 0, index=False)
                df = pd.concat([df, data])


if __name__ == "__main__":
    asyncio.run(main())
