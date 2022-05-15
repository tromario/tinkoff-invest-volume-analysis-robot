import csv
import datetime

import pandas as pd
from tinkoff.invest import TradeDirection

from finplot_graph import FinplotGraph
from settings import CURRENT_TIMEFRAME, FIRST_TOUCH_VOLUME_LEVEL, SECOND_TOUCH_VOLUME_LEVEL, TAKE_PROFIT, \
    PERCENTAGE_STOP_LOSS
from utils.utils import Utils

pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.width = None

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
    # пропускаю премаркет
    available_time = time.replace(hour=7, minute=0, second=0, microsecond=0)
    return time < available_time


class Tester:
    def __init__(self):
        self.df = pd.DataFrame(columns=['figi', 'direction', 'price', 'quantity', 'time'])
        self.df = apply_frame_type(self.df)

        self.first_tick_time = None
        self.fix_date = {}
        self.clusters = None
        self.processed_volume_levels = {}
        self.level_response = []

        self.orders = []

        self.finplot_graph = FinplotGraph()
        self.finplot_graph.start()

    def run(self):
        test_start_time = datetime.datetime.now()
        with open('./data/prod/USD000UTSTOM-20220513.csv', newline='') as file:
            reader = csv.DictReader(file, delimiter=',')
            for row in reader:
                current_price = float(row['price'])
                time = Utils.parse_date(row['time'])
                is_available_open_orders = is_open_orders(time)

                if is_premarket_time(time):
                    continue

                for order in self.orders:
                    if order['status'] == 'active':
                        order['close'] = current_price

                        if not is_available_open_orders:
                            # закрытие сделок по причине приближении закрытии биржи
                            order['status'] = 'close'
                            if order['direction'] == TradeDirection.TRADE_DIRECTION_BUY:
                                order['result'] = order['close'] - order['open']
                                order['is_win'] = order['result'] > 0
                            else:
                                order['result'] = order['open'] - order['close']
                                order['is_win'] = order['result'] > 0
                            print(f'закрытие открытой заявки [time={order["time"]}], результат: {order["result"]}')
                            continue

                        if order['direction'] == TradeDirection.TRADE_DIRECTION_BUY:
                            if current_price < order['stop']:
                                # закрываю активные buy-заявки по стопу, если цена ниже стоп-лосса
                                order['status'] = 'close'
                                order['is_win'] = False
                                order['result'] = order['close'] - order['open']
                                print(
                                    f'закрыта заявка [time={order["time"]}] по стоп-лоссу, результат: {order["result"]}')
                            elif current_price > order['take']:
                                # закрываю активные buy-заявки по тейку, если цена выше тейк-профита
                                order['status'] = 'close'
                                order['is_win'] = True
                                order['result'] = order['close'] - order['open']
                                print(
                                    f'закрыта заявка [time={order["time"]}] по тейк-профиту, результат: {order["result"]}')
                        else:
                            if current_price > order['stop']:
                                # закрываю активные sell-заявки по стопу, если цена выше стоп-лосса
                                order['status'] = 'close'
                                order['is_win'] = False
                                order['result'] = order['open'] - order['close']
                                print(
                                    f'закрыта заявка [time={order["time"]}] по стоп-лоссу, результат: {order["result"]}')
                            elif current_price < order['take']:
                                # закрываю активные sell-заявки по тейку, если цена ниже тейк-профита
                                order['status'] = 'close'
                                order['is_win'] = True
                                order['result'] = order['open'] - order['close']
                                print(
                                    f'закрыта заявка [time={order["time"]}] по тейк-профиту, результат: {order["result"]}')

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
                    self.finplot_graph.render(self.df,
                                              valid_entry_points=valid_entry_points,
                                              invalid_entry_points=invalid_entry_points,
                                              clusters=self.clusters)

                data = pd.DataFrame.from_records([
                    {
                        'figi': row['figi'],
                        'direction': row['direction'],
                        'price': current_price,
                        'quantity': row['quantity'],
                        'time': time,
                    }
                ])
                data = apply_frame_type(data)
                self.df = pd.concat([self.df, data])

                if (time - self.first_tick_time).total_seconds() >= 60:
                    # сбрасываю секунды, чтобы сравнивать "целые" минутные свечи
                    self.first_tick_time = time.replace(second=0, microsecond=0)
                    # если торги доступны, то каждую завершенную минуту проверяю кластера на возможную ТВ
                    if is_available_open_orders and len(self.processed_volume_levels) > 0:
                        for volume_price, volume_level in self.processed_volume_levels.items():
                            for touch_time, value in volume_level['times'].items():
                                if value is not None:
                                    continue
                                candles = Utils.ticks_to_cluster(self.df, period='1min')
                                candles = Utils.calculate_ratio(candles)
                                candle = candles.loc[candles['time'] == touch_time.replace(second=0, microsecond=0)]
                                if candle is None:
                                    print('свеча не найдена')
                                    continue

                                if candle.iloc[0]['win'] is True:
                                    # если свеча является сигнальной, то осуществляю сделку
                                    max_volume_price = candle.iloc[0]['max_volume_price']
                                    percent = (max_volume_price * PERCENTAGE_STOP_LOSS / 100)
                                    self.processed_volume_levels[volume_price]['times'][touch_time] = True

                                    if candle.iloc[0]['direction'] == TradeDirection.TRADE_DIRECTION_BUY:
                                        stop = max_volume_price - percent
                                        take = current_price - ((stop - current_price) * TAKE_PROFIT)
                                        order = {'open': current_price, 'stop': stop, 'take': take,
                                                 'direction': TradeDirection.TRADE_DIRECTION_BUY,
                                                 'time': time, 'status': 'active'}
                                        self.orders.append(order)
                                        print(f'подтверждена точка входа в лонг', order)
                                    else:
                                        stop = max_volume_price + percent
                                        take = current_price - ((stop - current_price) * TAKE_PROFIT)
                                        order = {'open': current_price, 'stop': stop, 'take': take,
                                                 'direction': TradeDirection.TRADE_DIRECTION_SELL,
                                                 'time': time, 'status': 'active'}
                                        self.orders.append(order)
                                        print(f'подтверждена точка входа в шорт', order)
                                else:
                                    # если текущая свеча не сигнальная, то ожидаю следующую для возможного входа
                                    self.processed_volume_levels[volume_price]['times'][touch_time] = False
                                    self.processed_volume_levels[volume_price]['last_touch_time'] = None

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

                            print(f'объемный уровень {cluster_price} сформирован в {cluster_time}', flush=True)
                            print(
                                f'{time}: цена {current_price} подошла к объемному уровню {self.processed_volume_levels[cluster_price]["count_touches"]} раз\n',
                                flush=True)
                            break

        # по завершению анализа перестраиваю показания, т.к. закрытие торгов не совпадает целому часу
        # например 15:59:59.230333+00:00
        self.clusters = Utils.ticks_to_cluster(self.df, period=CURRENT_TIMEFRAME)
        valid_entry_points, invalid_entry_points = Utils.processed_volume_levels_to_times(
            self.processed_volume_levels)
        self.finplot_graph.render(self.df,
                                  valid_entry_points=valid_entry_points,
                                  invalid_entry_points=invalid_entry_points,
                                  clusters=self.clusters)

        test_end_time = datetime.datetime.now()
        print('\nанализ завершен')
        print(f'время тестирования: {(test_end_time - test_start_time).total_seconds() / 60} мин.')

        print(f'\nколичество сделок: {len(self.orders)}\n')

        take_orders = list(filter(lambda x: x['is_win'], self.orders))
        earned_points = sum(order['result'] for order in take_orders)
        print(f'успешных сделок: {len(take_orders)}')
        print(f'заработано пунктов: {earned_points}')

        loss_orders = list(filter(lambda x: not x['is_win'], self.orders))
        lost_points = sum(order['result'] for order in loss_orders)
        print(f'\nотрицательных сделок: {len(loss_orders)}')
        print(f'потеряно пунктов: {lost_points}')

        print(f'\nитого пунктов: {earned_points + lost_points}')


if __name__ == "__main__":
    tester = Tester()
    tester.run()
