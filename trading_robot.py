import asyncio
import logging
from datetime import datetime
from datetime import timedelta

import pandas as pd
from tinkoff.invest import (
    AsyncClient,
    TradeInstrument,
    MarketDataRequest,
    SubscribeTradesRequest,
    SubscriptionAction
)
from tinkoff.invest.utils import now

from settings import INSTRUMENTS, CAN_OPEN_ORDERS, TOKEN
from strategies.profile_touch_strategy import ProfileTouchStrategy
from services.order_service import OrderService
from utils.utils import Utils

pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.width = None

date = datetime.now().strftime('%Y-%m-%d')
format = '%(asctime)s %(levelname)s --- (%(filename)s).%(funcName)s(%(lineno)d):\t %(message)s'
logging.basicConfig(format=format,
                    level=logging.INFO,
                    handlers=[
                        logging.FileHandler(f'./logs/log-{date}.log', encoding='utf-8'),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

TIME_STEP = 60


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


def get_file_path_by_instrument(instrument):
    return f'./data/{instrument["name"]}-{datetime.now().strftime("%Y%m%d")}.csv'


def create_empty_df():
    df = pd.DataFrame(columns=['figi', 'direction', 'price', 'quantity', 'time'])
    df.time = pd.to_datetime(df.time, unit='ms')
    df.price = pd.to_numeric(df.price)
    df.quantity = pd.to_numeric(df.quantity)
    return df


# перенести в utils?
def processed_data(trade):
    try:
        if trade is None:
            return

        price = Utils.quotation_to_float(trade.price)
        data = pd.DataFrame.from_records([
            {
                'figi': trade.figi,
                'direction': trade.direction,
                'price': price,
                'quantity': trade.quantity,
                'time': pd.to_datetime(str(trade.time), utc=True)
            }
        ])

        return data
    except Exception as ex:
        logger.error(ex)


def is_open_exchange():
    # условно биржа работает до 18мск
    close_time = now().replace(hour=16, minute=0, second=0, microsecond=0)
    return now() < close_time


class TradingRobot:
    def __init__(self):
        self.order_service = OrderService(is_notification=True, can_open_orders=CAN_OPEN_ORDERS)
        self.order_service.start()

        self.is_history_processed = True

        self.df_by_instrument = {}
        self.strategy = {}
        for instrument in INSTRUMENTS:
            figi = instrument['figi']
            df_by_instrument = create_empty_df()
            self.df_by_instrument[figi] = df_by_instrument

            file_path = get_file_path_by_instrument(instrument)
            instrument_file = open(file_path, 'a', newline='')
            df_by_instrument.to_csv(instrument_file, mode='a', header=instrument_file.tell() == 0, index=False)

            profile_touch_strategy = ProfileTouchStrategy(instrument['name'])
            profile_touch_strategy.start()
            self.strategy[figi] = profile_touch_strategy

    async def sync_df(self, client):
        self.is_history_processed = True
        for instrument in INSTRUMENTS:
            try:
                figi = instrument['figi']

                file_path = get_file_path_by_instrument(instrument)
                self.df_by_instrument[figi] = pd.read_csv(file_path, sep=',')
                self.df_by_instrument[figi]['time'] = pd.to_datetime(self.df_by_instrument[figi]['time'], utc=True)

                history_df = await self.get_history_trades(client, instrument)
                self.df_by_instrument[figi] = Utils.merge_two_frames(self.df_by_instrument[figi], history_df)
            except Exception as ex:
                logger.error(ex)
        self.is_history_processed = False

    # загрузка последних доступных обезличенных сделок
    async def get_history_trades(self, client, instrument):
        history_df = create_empty_df()
        figi = instrument['figi']
        current_date = now()
        time = 0

        while True:
            try:
                interval_from = current_date - timedelta(minutes=time + TIME_STEP)
                interval_to = current_date - timedelta(minutes=time)

                logger.info(instrument)
                logger.info(f'from {interval_from}')
                logger.info(f'to {interval_to}')

                response = await client.market_data.get_last_trades(
                    figi=figi,
                    from_=interval_from,
                    to=interval_to,
                )
                logger.info(f'{instrument} size = {len(response.trades)}')
                if response is None or len(response.trades) == 0:
                    break

                for trade in response.trades:
                    processed_trade_df = processed_data(trade)
                    if processed_trade_df is not None:
                        history_df = pd.concat([history_df, processed_trade_df])
                history_df = history_df.sort_values('time')
                time += TIME_STEP
            except Exception as ex:
                logger.error(ex)
                break

        return history_df

    async def trades_stream(self, client):
        temp_df = {}
        for instrument in INSTRUMENTS:
            temp_df[instrument['figi']] = create_empty_df()

        try:
            async for marketdata in client.market_data_stream.market_data_stream(
                    request_iterator()
            ):
                if not is_open_exchange():
                    logger.info('торговый день завершен, сохранение статистики')
                    self.order_service.write_statistics()
                    # добавить выход из приложения

                logger.info(marketdata)
                if marketdata is None:
                    continue
                trade = marketdata.trade
                if trade is None:
                    continue

                figi = trade.figi
                instrument = next(item for item in INSTRUMENTS if item["figi"] == figi)

                processed_trade_df = processed_data(trade)
                if processed_trade_df is not None:
                    if self.is_history_processed is True:
                        # пока происходит обработка истории - новые данные складываю во временную переменную
                        next_df = [temp_df[figi], processed_trade_df]
                        temp_df[figi] = pd.concat(next_df, ignore_index=True)
                    else:
                        # есть проблема, когда исторические данные загрузились, но в real-time они не приходят
                        # тогда исторические данные не окажутся в файле
                        if len(temp_df[figi]) > 0:
                            # если после обработки истории успели накопить real-time данные,
                            # то подмерживаю их и очищаю временную переменную
                            next_df = [self.df_by_instrument[figi], temp_df[figi], processed_trade_df]
                            self.df_by_instrument[figi] = pd.concat(next_df, ignore_index=True)
                            temp_df[figi].drop(temp_df[figi].index, inplace=True)

                            # отправляю обезличенные сделки на анализ
                            self.strategy[figi].set_df(self.df_by_instrument[figi])

                            file_path = get_file_path_by_instrument(instrument)
                            self.df_by_instrument[figi].to_csv(file_path, mode='w', header=True, index=False)
                        else:
                            # отправляю обезличенную сделку на анализ
                            # (алгоритм анализа можно заменить на любой)
                            # если ТВ подтвердится, то возвращается структура сделки
                            orders = self.strategy[figi].analyze(processed_trade_df)
                            if orders is not None:
                                [self.order_service.create_order(order) for order in orders]

                            next_df = [self.df_by_instrument[figi], processed_trade_df]
                            self.df_by_instrument[figi] = pd.concat(next_df, ignore_index=True)

                        file_path = get_file_path_by_instrument(instrument)
                        processed_trade_df.to_csv(file_path, mode='a', header=False, index=False)
        except Exception as ex:
            logger.error(ex)

    async def main(self):
        async with AsyncClient(TOKEN) as client:
            tasks = [asyncio.ensure_future(self.trades_stream(client)),
                     asyncio.ensure_future(self.sync_df(client))]
            await asyncio.wait(tasks)


if __name__ == "__main__":
    try:
        robot = TradingRobot()
        asyncio.run(robot.main())
    except Exception as ex:
        logger.error(ex)
