import pandas as pd
from tinkoff.invest import Quotation
from tinkoff.invest.utils import quotation_to_decimal


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
    def merge_two_frames(source_df, new_df):
        if new_df is None or len(new_df) == 0:
            return source_df
        if source_df is None or len(source_df) == 0:
            return new_df

        first_time = new_df.iloc[0]['time']
        last_time = new_df.iloc[-1]['time']
        search_condition = (source_df['time'] >= first_time) & (source_df['time'] <= last_time)

        result_df = source_df.drop(source_df.loc[search_condition].index)
        result_df = pd.concat([result_df, new_df]).rename_axis('index')
        return result_df.sort_values(['time', 'index']).reset_index(drop=True)
