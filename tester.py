import csv
import datetime

import pandas as pd
from tinkoff.invest import TradeDirection

from finplot_graph import FinplotGraph
from settings import CURRENT_TIMEFRAME, FIRST_TOUCH_VOLUME_LEVEL, SECOND_TOUCH_VOLUME_LEVEL
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


class Tester:
    def __init__(self):
        self.df = pd.DataFrame(columns=['figi', 'direction', 'price', 'quantity', 'time'])
        self.df = apply_frame_type(self.df)

        self.first_tick_time = None
        self.fix_date = {}
        self.clusters = None
        self.processed_volume_levels = {}
        self.level_response = []

        self.finplot_graph = FinplotGraph()
        self.finplot_graph.start()

    def run(self):
        test_start_time = datetime.datetime.now()
        with open('./data/USD000UTSTOM-20220506-merge.csv', newline='') as file:
            reader = csv.DictReader(file, delimiter=',')
            for row in reader:
                price = float(row['price'])
                time = datetime.datetime.strptime(row['time'], "%Y-%m-%d %H:%M:%S.%f%z")

                # пропускаю премаркет
                if time.hour < 7:
                    continue

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
                        'price': price,
                        'quantity': row['quantity'],
                        'time': time,
                    }
                ])
                data = apply_frame_type(data)
                self.df = pd.concat([self.df, data])

                if (time - self.first_tick_time).total_seconds() >= 60:
                    # сбрасываю секунды, чтобы сравнивать "целые" минутные свечи
                    self.first_tick_time = time.replace(second=0, microsecond=0)
                    # каждую завершенную минуту проверяю кластера на возможную ТВ
                    if len(self.processed_volume_levels) > 0:
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
                                    self.processed_volume_levels[volume_price]['times'][touch_time] = True
                                    if candle.iloc[0]['direction'] == TradeDirection.TRADE_DIRECTION_BUY:
                                        print(f'подтверждена точка входа в лонг', candle)
                                    else:
                                        print(f'подтверждена точка входа в шорт', candle)
                                else:
                                    self.processed_volume_levels[volume_price]['times'][touch_time] = False
                                    self.processed_volume_levels[volume_price]['last_touch_time'] = None
                                    print(f'ТВ не подходит для входа', candle)

                if self.clusters is not None:
                    for index, cluster in self.clusters.iterrows():
                        cluster_time = cluster['time']
                        cluster_price = cluster['max_volume_price']

                        # цена может коснуться объемного уровня в заданном процентном диапазоне
                        is_price_in_range = Utils.is_price_in_range_cluster(price, cluster_price)
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
                                f'{time}: цена {price} подошла к объемному уровню {self.processed_volume_levels[cluster_price]["count_touches"]} раз\n',
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
        print(f'время тестирования: {(test_end_time - test_start_time).total_seconds()} сек.')
        print(f'кластера: {self.clusters}')


if __name__ == "__main__":
    tester = Tester()
    tester.run()
