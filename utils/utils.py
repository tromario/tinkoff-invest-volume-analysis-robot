import numpy as np
import pandas as pd
from tinkoff.invest import Quotation, TradeDirection
from tinkoff.invest.utils import quotation_to_decimal

from settings import PERCENTAGE_VOLUME_LEVEL_RANGE


class Utils:
    @staticmethod
    def quotation_to_float(quotation: Quotation):
        return float(quotation_to_decimal(quotation))

        # region прежние реализации:
        # return float(format(quotation.units + quotation.nano / 10 ** 9, '.9f'))

        # str_nano = f'{abs(quotation.nano):09}'
        # str_price = f'{quotation.units}.{str_nano}'
        # return float(str_price)
        # endregion

    @staticmethod
    def is_price_in_range_cluster(current_price, cluster_price):
        level_range = cluster_price * PERCENTAGE_VOLUME_LEVEL_RANGE / 100
        increased_level = cluster_price + level_range
        reduced_level = cluster_price - level_range
        return reduced_level <= current_price <= increased_level

    @staticmethod
    def merge_two_frames(source_df, new_df):
        if new_df is None or len(new_df) == 0:
            return source_df
        if source_df is None or len(source_df) == 0:
            return new_df

        first_time = pd.to_datetime(new_df.iloc[0]['time'], utc=True)
        last_time = pd.to_datetime(new_df.iloc[-1]['time'], utc=True)
        search_condition = (source_df['time'] >= first_time) & (source_df['time'] <= last_time)

        result_df = source_df.drop(source_df.loc[search_condition].index)
        result_df = pd.concat([result_df, new_df]).rename_axis('index')
        return result_df.sort_values(['time', 'index']).reset_index(drop=True)

    @staticmethod
    def agg_ohlc(df):
        price = df['price'].values
        quantity = df['quantity'].values
        names = {
            'low': min(price) if len(price) > 0 else np.nan,
            'high': max(price) if len(price) > 0 else np.nan,
            'open': price[0] if len(price) > 0 else np.nan,
            'close': price[-1] if len(price) > 0 else np.nan,
            'total_volume': sum(quantity) if len(quantity) > 0 else 0,
            'max_volume_price': df.groupby(['price'])[['quantity']].sum().idxmax()[0]
        }
        return pd.Series(names)

    @staticmethod
    def calculate_ratio(candles):
        # процентное соотношение лонгистов/шортистов в свече
        difference = candles['high'] - candles['low']
        long_ratio = (candles['close'] - candles['low']) / difference * 100
        short_ratio = (candles['high'] - candles['close']) / difference * 100
        candles['long'] = long_ratio
        candles['short'] = short_ratio

        # расчет расположения макс. объема относительно открытия свечи
        from_high = abs(candles['high'] - candles['max_volume_price'])
        from_low = abs(candles['max_volume_price'] - candles['low'])
        total = from_high + from_low
        candles.loc[candles['direction'] == TradeDirection.TRADE_DIRECTION_BUY, 'percent'] = from_low / total * 100
        candles.loc[candles['direction'] == TradeDirection.TRADE_DIRECTION_SELL, 'percent'] = from_high / total * 100

        # определение победителя:
        # если соотношение лонгистов больше и макс объем как можно ниже, то приоритет для лонга
        # если соотношение шортистов больше и макс объем как можно выше, то приоритет для шорта
        candles.loc[candles['direction'] == TradeDirection.TRADE_DIRECTION_BUY, 'win'] = (candles['long'] > 50) & (
                    candles['percent'] <= 40)
        candles.loc[candles['direction'] == TradeDirection.TRADE_DIRECTION_SELL, 'win'] = (candles['short'] > 50) & (
                    candles['percent'] <= 40)

        return candles

    @staticmethod
    def ticks_to_cluster(df, period='1min'):
        candles = df.set_index(['time'])
        candles = candles.resample(period).apply(Utils.agg_ohlc)
        candles = candles.ffill()

        candles['time'] = candles.index
        # свеча доджи
        candles.loc[candles['close'] == candles['open'], 'direction'] = TradeDirection.TRADE_DIRECTION_UNSPECIFIED
        # бычья свеча
        candles.loc[candles['close'] > candles['open'], 'direction'] = TradeDirection.TRADE_DIRECTION_BUY
        # медвежья свеча
        candles.loc[candles['open'] > candles['close'], 'direction'] = TradeDirection.TRADE_DIRECTION_SELL

        candles = candles[['time', 'open', 'close', 'high', 'low', 'total_volume', 'direction', 'max_volume_price']]
        return candles.reset_index(drop=True)

    @staticmethod
    def processed_volume_levels_to_times(processed_volume_levels):
        valid_entry_points = []
        invalid_entry_points = []
        if processed_volume_levels is None:
            return valid_entry_points, invalid_entry_points

        for price, volume_level in processed_volume_levels.items():
            for time, is_success in volume_level['times'].items():
                if is_success:
                    valid_entry_points += [time]
                else:
                    invalid_entry_points += [time]
        
        return valid_entry_points, invalid_entry_points
