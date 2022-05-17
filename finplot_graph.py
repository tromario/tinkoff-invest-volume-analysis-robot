import threading
from datetime import timedelta

import finplot as fplt
import pandas as pd

from settings import SIGNAL_CLUSTER_PERIOD
from utils.utils import Utils


class FinplotGraph(threading.Thread):
    def __init__(self):
        super().__init__()
        self.ax2 = None
        self.ax = None
        self.plots = []

    def run(self):
        self.ax = fplt.create_plot('Tinkoff Invest volume profile tester')
        fplt.show()

    def render(self, df, valid_entry_points, invalid_entry_points, clusters=None):
        candles = Utils.ticks_to_cluster(df, period=SIGNAL_CLUSTER_PERIOD)
        # риски в свечах с максимальными объемами
        max_volumes = candles[['time', 'max_volume_price']]

        # округление времени точек входа для сопоставления с графиком
        round_valid_times = [pd.to_datetime(time).floor('min') for time in valid_entry_points]
        candles.loc[candles['time'].isin(round_valid_times), 'valid_entry_point'] = candles['low'] - (
                    candles['low'] * 0.007 / 100)

        round_invalid_times = [pd.to_datetime(time).floor('min') for time in invalid_entry_points]
        candles.loc[candles['time'].isin(round_invalid_times), 'invalid_entry_point'] = candles['low'] - (
                    candles['low'] * 0.007 / 100)

        if not self.plots:
            # первое построение графика
            self.plots.append(fplt.candlestick_ochl(candles))
            self.plots.append(fplt.plot(max_volumes['time'],
                                        max_volumes['max_volume_price'],
                                        style='d',
                                        color='#808080',
                                        ax=self.ax))
            self.plots.append(fplt.plot(candles['time'],
                                        candles['valid_entry_point'],
                                        style='^',
                                        color='#4a5',
                                        ax=self.ax,
                                        legend='Подтвержденная ТВ'))
            self.plots.append(fplt.plot(candles['time'],
                                        candles['invalid_entry_point'],
                                        style='^',
                                        color='#000',
                                        ax=self.ax,
                                        legend='Не подтвержденная ТВ'))
        else:
            # обновление данных графика, после того как он был построен
            # https://github.com/highfestiva/finplot/issues/131#issuecomment-786245998
            self.plots[0].update_data(candles, gfx=True)
            self.plots[1].update_data(max_volumes['max_volume_price'], gfx=False)
            self.plots[2].update_data(candles['valid_entry_point'], gfx=False)
            self.plots[3].update_data(candles['invalid_entry_point'], gfx=False)

            self.plots[0].update_gfx()
            self.plots[1].update_gfx()
            self.plots[2].update_gfx()
            self.plots[3].update_gfx()

        if clusters is not None:
            for index, cluster in clusters.iterrows():
                cluster_time = cluster['time']
                cluster_price = cluster['max_volume_price']
                # todo прибавляю 59мин по той причине, что целый час еще не сформировался на графике
                #  крайней датой может быть 15:59:59.230333+00:00
                end_time = cluster_time + timedelta(minutes=59)
                fplt.add_line((cluster_time, cluster_price),
                              (end_time, cluster_price),
                              color='#80878787',
                              ax=self.ax)

        fplt.autoviewrestore()
