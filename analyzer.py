import datetime
import logging
import threading

import numpy
import pandas as pd
from tinkoff.invest import TradeDirection

from finplot_graph import FinplotGraph
from settings import CURRENT_TIMEFRAME, FIRST_TOUCH_VOLUME_LEVEL, SECOND_TOUCH_VOLUME_LEVEL, TAKE_PROFIT, \
    PERCENTAGE_STOP_LOSS, SIGNAL_CLUSTER_PERIOD, IS_SHOW_CHART, NOTIFICATION
from telegram_service import TelegramService
from utils.utils import Utils

pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.width = None

logger = logging.getLogger(__name__)

TIMEFRAME = {
    '1min': 1,
    '5min': 5,
    '30min': 30,
    '1h': 60
}


def apply_frame_type(df):
    return df.astype({
        'figi': 'object',
        'direction': 'int64',
        'price': 'float64',
        'quantity': 'int64',
        # 'time': 'datetime64[ms]',
    })


def is_open_orders(time):
    # доступно открытие позиций до 18мск
    available_time = time.replace(hour=15, minute=0, second=0, microsecond=0)
    return time < available_time


def is_premarket_time(time):
    # пропускаю анализ премаркета
    available_time = time.replace(hour=7, minute=30, second=0, microsecond=0)
    return time < available_time


class Analyzer(threading.Thread):
    def __init__(self, instrument_name):
        super().__init__()

        self.instrument_name = instrument_name

        self.df = pd.DataFrame(columns=['figi', 'direction', 'price', 'quantity', 'time'])
        self.df = apply_frame_type(self.df)

        self.first_tick_time = None
        self.fix_date = {}
        self.clusters = None
        self.processed_volume_levels = {}
        self.level_response = []

        self.orders = []

        self.telegram_service = TelegramService(NOTIFICATION['chat_id'])

        if IS_SHOW_CHART:
            self.finplot_graph = FinplotGraph()
            self.finplot_graph.start()

    def set_df(self, df):
        self.df = df
        logger.info('загружен новый data frame')

    def analyze(self, trade_df):
        trade_data = trade_df.iloc[0]
        current_price = trade_data['price']
        time = trade_data['time']

        if is_premarket_time(time):
            return

        self.processed_orders(current_price, time)

        if CURRENT_TIMEFRAME not in self.fix_date:
            self.fix_date[CURRENT_TIMEFRAME] = time.hour

        if self.first_tick_time is None:
            # сбрасываю секунды, чтобы сравнивать "целые" минутные свечи
            self.first_tick_time = time.replace(second=0, microsecond=0)

        if self.fix_date[CURRENT_TIMEFRAME] < time.hour:
            # построение кластерных свечей и графика раз в 1 час
            self.fix_date[CURRENT_TIMEFRAME] = time.hour
            self.clusters = Utils.ticks_to_cluster(self.df, period=CURRENT_TIMEFRAME)

            valid_entry_points, invalid_entry_points = Utils.processed_volume_levels_to_times(
                self.processed_volume_levels)
            if IS_SHOW_CHART:
                self.finplot_graph.render(self.df,
                                          valid_entry_points=valid_entry_points,
                                          invalid_entry_points=invalid_entry_points,
                                          clusters=self.clusters)

        self.df = pd.concat([self.df, trade_df])

        # 60 * 5 для ТФ = 5мин
        if (time - self.first_tick_time).total_seconds() >= 60 * 5:
            # сбрасываю секунды, чтобы сравнивать "целые" минутные свечи
            self.first_tick_time = time.replace(second=0, microsecond=0)
            # если торги доступны, то каждую завершенную минуту проверяю кластера на возможную ТВ
            if is_open_orders(time) and len(self.processed_volume_levels) > 0:
                self.check_entry_points(current_price, time)

        if self.clusters is not None:
            for index, cluster in self.clusters.iterrows():
                cluster_time = cluster['time']
                cluster_price = cluster['max_volume_price']

                # цена может коснуться объемного уровня в заданном процентном диапазоне
                is_price_in_range = Utils.is_price_in_range_cluster(current_price, cluster_price)
                if is_price_in_range:
                    timedelta = time - cluster_time
                    if timedelta < datetime.timedelta(minutes=FIRST_TOUCH_VOLUME_LEVEL):
                        continue

                    if cluster_price not in self.processed_volume_levels:
                        # инициализация первого касания уровня
                        self.processed_volume_levels[cluster_price] = {}
                        self.processed_volume_levels[cluster_price]['count_touches'] = 0
                        self.processed_volume_levels[cluster_price]['times'] = {}
                    else:
                        # обработка второго и последующего касания уровня на основе времени последнего касания
                        if self.processed_volume_levels[cluster_price]['last_touch_time'] is not None:
                            timedelta = time - self.processed_volume_levels[cluster_price]['last_touch_time']
                            if timedelta < datetime.timedelta(minutes=SECOND_TOUCH_VOLUME_LEVEL):
                                continue

                    # установка параметров при касании уровня
                    self.processed_volume_levels[cluster_price]['count_touches'] += 1
                    self.processed_volume_levels[cluster_price]['last_touch_time'] = time
                    self.processed_volume_levels[cluster_price]['times'][time] = None

                    logger.info(f'объемный уровень {cluster_price} сформирован в {cluster_time}')
                    logger.info(
                        f'{time}: цена {current_price} подошла к объемному уровню {self.processed_volume_levels[cluster_price]["count_touches"]} раз\n'
                    )
                    break

    def processed_orders(self, current_price, time):
        for order in self.orders:
            if order['status'] == 'active':
                if not is_open_orders(time):
                    # закрытие сделок по причине приближении закрытии биржи
                    order['status'] = 'close'
                    order['close'] = current_price
                    if order['direction'] == TradeDirection.TRADE_DIRECTION_BUY:
                        order['result'] = order['close'] - order['open']
                        order['is_win'] = order['result'] > 0
                    else:
                        order['result'] = order['open'] - order['close']
                        order['is_win'] = order['result'] > 0
                    logger.info(f'закрытие открытой заявки [time={order["time"]}], результат: {order["result"]}')
                    self.telegram_service.post(f'Закрытие сделки для {order["time"]}, результат {order["result"]}')
                    continue

                if order['direction'] == TradeDirection.TRADE_DIRECTION_BUY:
                    if current_price < order['stop']:
                        # закрываю активные buy-заявки по стопу, если цена ниже стоп-лосса
                        order['status'] = 'close'
                        order['close'] = current_price
                        order['is_win'] = False
                        order['result'] = order['close'] - order['open']
                        logger.info(
                            f'закрыта заявка по стоп-лоссу с результатом {order["result"]}; открыта в {order["time"]}, текущее время {time}')
                        self.telegram_service.post(f'S/L для {order["time"]}, результат {order["result"]}')
                    elif current_price > order['take']:
                        # закрываю активные buy-заявки по тейку, если цена выше тейк-профита
                        order['status'] = 'close'
                        order['close'] = current_price
                        order['is_win'] = True
                        order['result'] = order['close'] - order['open']
                        logger.info(
                            f'закрыта заявка по тейк-профиту с результатом {order["result"]}; открыта в {order["time"]}, текущее время {time}')
                        self.telegram_service.post(f'T/P для {order["time"]}, результат {order["result"]}')
                else:
                    if current_price > order['stop']:
                        # закрываю активные sell-заявки по стопу, если цена выше стоп-лосса
                        order['status'] = 'close'
                        order['close'] = current_price
                        order['is_win'] = False
                        order['result'] = order['open'] - order['close']
                        logger.info(
                            f'закрыта заявка по стоп-лоссу с результатом {order["result"]}; открыта в {order["time"]}, текущее время {time}')
                        self.telegram_service.post(f'S/L для {order["time"]}, результат {order["result"]}')
                    elif current_price < order['take']:
                        # закрываю активные sell-заявки по тейку, если цена ниже тейк-профита
                        order['status'] = 'close'
                        order['close'] = current_price
                        order['is_win'] = True
                        order['result'] = order['open'] - order['close']
                        logger.info(
                            f'закрыта заявка по тейк-профиту с результатом {order["result"]}; открыта в {order["time"]}, текущее время {time}')
                        self.telegram_service.post(f'T/P для {order["time"]}, результат {order["result"]}')

    def check_entry_points(self, current_price, time):
        for volume_price, volume_level in self.processed_volume_levels.items():
            for touch_time, value in volume_level['times'].items():
                if value is not None:
                    continue
                candles = Utils.ticks_to_cluster(self.df, period=SIGNAL_CLUSTER_PERIOD)
                candles = Utils.calculate_ratio(candles)
                prev_candle = candles.iloc[-3]
                current_candle = candles.iloc[-2]
                # todo подумать, как лучше получать свечи: по условию или индексу
                #  с условиями сложнее, нужно дополнительно вычислять
                #  с индексами, с виду, не должно быть проблем, только null
                # prev_candle = candles.loc[candles['time'] == pd.to_datetime(touch_time).floor("10min")]
                # current_candle = candles.loc[candles['time'] == pd.to_datetime(touch_time).floor(SIGNAL_CLUSTER_PERIOD)]
                if current_candle.empty or prev_candle.empty:
                    logger.error('свеча не найдена')
                    continue

                if current_candle['win'] is True:
                    # если свеча является сигнальной, то осуществляю сделку
                    close_price = current_candle['close']
                    max_volume_price = current_candle['max_volume_price']
                    percent = (max_volume_price * PERCENTAGE_STOP_LOSS / 100)
                    self.processed_volume_levels[volume_price]['times'][touch_time] = True

                    if current_candle['direction'] == TradeDirection.TRADE_DIRECTION_BUY:
                        active_order = list(filter(lambda x: x['direction'] == TradeDirection.TRADE_DIRECTION_BUY and
                                                             x['status'] == 'active', self.orders))
                        if len(active_order) > 0:
                            logger.info(f'сделка в лонг уже открыта: {active_order}')
                            return

                        if prev_candle['open'] < current_candle['open']:
                            logger.info('пропуск входа - предыдущая свеча открылась ниже текущей')
                            return

                        # todo условие дает плохое соотношение
                        # если подошли к объемному уровню снизу вверх на лонговой свече, то пропускаю вход
                        # if close_price < volume_price:
                        #     logger.info(f'пропуск входа - цена закрытия ниже объемного уровня', time, current_price)
                        #     return

                        if current_price < max_volume_price:
                            logger.info(
                                f'пропуск входа - цена открытия ниже макс объема в сигнальной свече, time={time}, price={current_price}')
                            return

                        for i in numpy.arange(1, 2, 0.5):
                            stop = max_volume_price - percent
                            take = current_price - ((stop - current_price) * TAKE_PROFIT * i)
                            order = {'open': current_price, 'stop': stop, 'take': take,
                                     'direction': TradeDirection.TRADE_DIRECTION_BUY,
                                     'time': time, 'status': 'active',
                                     'volume_price': volume_price, 'signal_candle': current_candle}
                            self.orders.append(order)
                            logger.info(f'подтверждена точка входа в лонг: {order}')
                            self.telegram_service.post(f"✅ ТВ в лонг: цена {current_price}, тейк {take}, стоп {stop}")
                    else:
                        active_order = list(filter(lambda x: x['direction'] == TradeDirection.TRADE_DIRECTION_SELL and
                                                             x['status'] == 'active', self.orders))
                        if len(active_order) > 0:
                            logger.info(f'сделка в шорт уже открыта: {active_order}')
                            return

                        if prev_candle['open'] > current_candle['open']:
                            logger.info('пропуск входа - предыдущая свеча открылась выше текущей')
                            return

                        # todo условие дает плохое соотношение
                        # если подошли к объемному уровню сверху вниз на шортовой свече, то пропускаю вход
                        # if close_price > volume_price:
                        #     logger.info(f'пропуск входа - цена закрытия выше объемного уровня', time, current_price)
                        #     return

                        if current_price > max_volume_price:
                            logger.info(
                                f'пропуск входа - цена открытия выше макс объема в сигнальной свече, time={time}, price={current_price}')
                            return

                        for i in numpy.arange(1, 2, 0.5):
                            stop = max_volume_price + percent
                            take = current_price - ((stop - current_price) * TAKE_PROFIT * i)
                            order = {'open': current_price, 'stop': stop, 'take': take,
                                     'direction': TradeDirection.TRADE_DIRECTION_SELL,
                                     'time': time, 'status': 'active',
                                     'volume_price': volume_price, 'signal_candle': current_candle}
                            self.orders.append(order)
                            logger.info(f'подтверждена точка входа в шорт: {order}')
                            self.telegram_service.post(f"✅ ТВ в шорт: цена {current_price}, тейк {take}, стоп {stop}")

                    return
                else:
                    # если текущая свеча не сигнальная, то ожидаю следующую для возможного входа
                    self.processed_volume_levels[volume_price]['times'][touch_time] = False
                    self.processed_volume_levels[volume_price]['last_touch_time'] = None

    def write_statistics(self):
        if not self.df.empty:
            # по завершению анализа перестраиваю показания, т.к. закрытие торгов не совпадает целому часу
            # например 15:59:59.230333+00:00
            self.clusters = Utils.ticks_to_cluster(self.df, period=CURRENT_TIMEFRAME)
            valid_entry_points, invalid_entry_points = Utils.processed_volume_levels_to_times(
                self.processed_volume_levels)
            if IS_SHOW_CHART:
                self.finplot_graph.render(self.df,
                                          valid_entry_points=valid_entry_points,
                                          invalid_entry_points=invalid_entry_points,
                                          clusters=self.clusters)

        with open(f'./logs/statistics-{self.instrument_name}.log', 'a', encoding='utf-8') as file:
            take_orders = list(filter(lambda x: x['is_win'], self.orders))
            earned_points = sum(order['result'] for order in take_orders)
            loss_orders = list(filter(lambda x: not x['is_win'], self.orders))
            lost_points = sum(order['result'] for order in loss_orders)

            logger.info(f'инструмент: {self.instrument_name}')
            logger.info(f'количество сделок: {len(self.orders)}')
            logger.info(f'успешных сделок: {len(take_orders)}')
            logger.info(f'заработано пунктов: {earned_points}')
            logger.info(f'отрицательных сделок: {len(loss_orders)}')
            logger.info(f'потеряно пунктов: {lost_points}')
            logger.info(f'итого пунктов: {earned_points + lost_points}')
            logger.info('-------------------------------------')

            file.write(f'количество сделок: {len(self.orders)}\n')
            file.write(f'успешных сделок: {len(take_orders)}\n')
            file.write(f'заработано пунктов: {earned_points}\n')
            file.write(f'отрицательных сделок: {len(loss_orders)}\n')
            file.write(f'потеряно пунктов: {lost_points}\n\n')
            file.write(f'итого пунктов: {earned_points + lost_points}\n')
            file.write('-------------------------------------\n')

            return earned_points + lost_points
