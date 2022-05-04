import csv
import datetime
import pandas as pd

pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.width = None

TIMEFRAME = {
    '1min': 1,
    '5min': 5,
    '30min': 30,
    '1h': 60
}
CURRENT_TIMEFRAME = '1h'

# первое касание объемного уровня интересно через Х минут
FIRST_TOUCH_VOLUME_LEVEL = 90

# последующие касание объемного уровня интересно через Х минут
SECOND_TOUCH_VOLUME_LEVEL = 30

# на сколько % выше или ниже цена может подойти к объемному уровню и его не каснуться
PERCENTAGE_VOLUME_LEVEL_RANGE = 0.03


def main():
    df = pd.DataFrame(columns=['figi', 'direction', 'price', 'quantity', 'time'])
    fix_date = {}
    clusters = None
    processed_volume_levels = {}

    with open('./data/statistics-20220428.csv', newline='') as file:
        reader = csv.DictReader(file, delimiter=',')
        for row in reader:
            price = float(row['price'])
            time = datetime.datetime.strptime(row['time'], "%Y-%m-%d %H:%M:%S.%f%z")

            # пропускаю премаркет
            if time.hour < 7:
                continue

            if CURRENT_TIMEFRAME not in fix_date:
                fix_date[CURRENT_TIMEFRAME] = time.hour

            if fix_date[CURRENT_TIMEFRAME] < time.hour:
                fix_date[CURRENT_TIMEFRAME] = time.hour
                clusters = to_cluster(df, CURRENT_TIMEFRAME)

            data = pd.DataFrame.from_records([
                {
                    'figi': row['figi'],
                    'direction': row['direction'],
                    'price': price,
                    'quantity': row['quantity'],
                    'time': time,
                }
            ])
            df = pd.concat([df, data])

            if clusters is not None:
                for index, cluster in clusters.iterrows():
                    level_range = cluster.price * PERCENTAGE_VOLUME_LEVEL_RANGE / 100
                    increased_level = cluster.price + level_range
                    reduced_level = cluster.price - level_range
                    if reduced_level <= price <= increased_level:
                        timedelta = time - cluster.time
                        if timedelta < datetime.timedelta(minutes=FIRST_TOUCH_VOLUME_LEVEL):
                            continue

                        if cluster.price not in processed_volume_levels:
                            processed_volume_levels[cluster.price] = {
                                'count_touches': 1,
                                'last_touch_date': time
                            }
                        else:
                            timedelta = time - processed_volume_levels[cluster.price]['last_touch_date']
                            if timedelta < datetime.timedelta(minutes=SECOND_TOUCH_VOLUME_LEVEL):
                                continue

                            processed_volume_levels[cluster.price] = {
                                'count_touches': processed_volume_levels[cluster.price]['count_touches'] + 1,
                                'last_touch_date': time
                            }

                        print(f'объемный уровень {cluster.price} сформирован в {cluster.time}')
                        print(
                            f'{time}: цена {price} подошла к объемному уровню {processed_volume_levels[cluster.price]["count_touches"]} раз\n')
                        break


def to_cluster(df, timeframe='5min'):
    # df.time = pd.to_datetime(df.time, utc=True)
    df.price = pd.to_numeric(df.price)
    df.quantity = pd.to_numeric(df.quantity)

    time_quantity = df.groupby([pd.Grouper(key='time', freq=timeframe), 'price'])[['quantity']].sum()
    result = time_quantity.reset_index(level='time').groupby('time')['quantity'].idxmax().reset_index(name='price')
    return result


if __name__ == "__main__":
    main()
