import csv
import datetime

import pandas as pd

from analyzer import Analyzer
from utils.utils import Utils

pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.width = None


def apply_frame_type(df):
    return df.astype({
        'figi': 'object',
        'direction': 'int64',
        'price': 'float64',
        'quantity': 'int64',
        # 'time': 'datetime64[ms]',
    })


class Tester:
    def __init__(self, instrument_name, file_path):
        self.instrument_name = instrument_name
        self.file_path = file_path

        self.df = pd.DataFrame(columns=['figi', 'direction', 'price', 'quantity', 'time'])
        self.df = apply_frame_type(self.df)

        self.analyzer = Analyzer(instrument_name)
        self.analyzer.start()

    def run(self):
        test_start_time = datetime.datetime.now()
        print(f'начат анализ {self.file_path} в {test_start_time}')

        with open(self.file_path, newline='') as file:
            reader = csv.DictReader(file, delimiter=',')
            for row in reader:
                figi = row['figi']
                current_price = float(row['price'])
                time = Utils.parse_date(row['time'])

                processed_trade_df = pd.DataFrame.from_records([
                    {
                        'figi': figi,
                        'direction': row['direction'],
                        'price': current_price,
                        'quantity': row['quantity'],
                        'time': pd.to_datetime(str(time), utc=True),
                    }
                ])
                processed_trade_df = apply_frame_type(processed_trade_df)
                self.analyzer.analyze(processed_trade_df)

        self.analyzer.write_statistics()

        test_end_time = datetime.datetime.now()
        print('\nанализ завершен')
        print(f'время тестирования: {(test_end_time - test_start_time).total_seconds() / 60} мин.')


if __name__ == "__main__":
    usd_histories = {'name': 'USD000UTSTOM',
                     'files': ['./data/USD000UTSTOM-20220517.csv', './data/USD000UTSTOM-20220516.csv',
                               './data/USD000UTSTOM-20220513.csv', './data/USD000UTSTOM-20220512.csv',
                               './data/USD000UTSTOM-20220511.csv', './data/USD000UTSTOM-20220506.csv',
                               './data/USD000UTSTOM-20220505.csv', './data/USD000UTSTOM-20220504.csv']}

    sber_histories = {'name': 'SBER',
                      'files': ['./data/SBER-20220517.csv', './data/SBER-20220516.csv',
                                './data/SBER-20220513.csv', './data/SBER-20220512.csv',
                                './data/SBER-20220511.csv', './data/SBER-20220506.csv',
                                './data/SBER-20220505.csv', './data/SBER-20220504.csv']}

    gaz_histories = {'name': 'GAZP',
                     'files': ['./data/GAZP-20220517.csv', './data/GAZP-20220516.csv',
                               './data/GAZP-20220513.csv', './data/GAZP-20220512.csv',
                               './data/GAZP-20220511.csv', './data/GAZP-20220506.csv',
                               './data/GAZP-20220505.csv', './data/GAZP-20220504.csv']}

    histories = [usd_histories] + [sber_histories] + [gaz_histories]
    total_result = 0

    for history in histories:
        for file_path in history['files']:
            tester = Tester(history['name'], file_path)
            tester.run()
