from typing import Dict

import pandas as pd


def get_float_value(dictionary: Dict, key):
    if key in dictionary:
        return float(dictionary.get(key))
    return 0


def get_int_value(dictionary: Dict, key):
    if key in dictionary:
        return int(dictionary.get(key))
    return 0


def get_datetime_value(dictionary: Dict, key):
    if key in dictionary:
        return pd.to_datetime(dictionary.get(key))
    return None


class Order(object):
    order_id = None
    close = 0
    result = 0
    is_win = False
    status = 'active'

    def __init__(
            self,
            id,
            group_id,
            instrument,
            open,
            stop,
            take,
            quantity,
            direction,
            time,
            status='active',
            result=0,
            is_win=False,
            close=0
    ):
        self.id = id
        self.group_id = group_id
        self.instrument = instrument
        self.open = open
        self.close = close
        self.stop = stop
        self.take = take
        self.quantity = quantity
        self.direction = direction
        self.time = time
        self.status = status
        self.result = result
        self.is_win = is_win

    def __iter__(self) -> Dict:
        yield 'id', self.id
        yield 'group_id', self.group_id
        yield 'instrument', self.instrument
        yield 'open', self.open
        yield 'close', self.close
        yield 'stop', self.stop
        yield 'take', self.take
        yield 'quantity', self.quantity
        yield 'direction', self.direction
        yield 'time', self.time
        yield 'status', self.status
        yield 'result', self.result
        yield 'is_win', self.is_win

    def __str__(self) -> str:
        return "Order{id=%s, " \
               "group_id=%s, " \
               "instrument=%s, " \
               "open=%s, " \
               "close=%s, " \
               "stop=%s, " \
               "take=%s, " \
               "quantity=%s, " \
               "direction=%s, " \
               "time=%s, " \
               "status=%s, " \
               "result=%s, " \
               "is_win=%s}" % \
               (self.id,
                self.group_id,
                self.instrument,
                self.open,
                self.close,
                self.stop,
                self.take,
                self.quantity,
                self.direction,
                self.time,
                self.status,
                self.result,
                self.is_win)

    def __repr__(self) -> str:
        return "Order{id=%s, " \
               "group_id=%s, " \
               "instrument=%s, " \
               "open=%s, " \
               "close=%s, " \
               "stop=%s, " \
               "take=%s, " \
               "quantity=%s, " \
               "direction=%s, " \
               "time=%s, " \
               "status=%s, " \
               "result=%s, " \
               "is_win=%s}" % \
               (self.id,
                self.group_id,
                self.instrument,
                self.open,
                self.close,
                self.stop,
                self.take,
                self.quantity,
                self.direction,
                self.time,
                self.status,
                self.result,
                self.is_win)

    @staticmethod
    def from_dict(order_dict: Dict):
        return Order(
            id=order_dict.get('id'),
            group_id=order_dict.get('group_id'),
            instrument=order_dict.get('instrument'),
            open=get_float_value(order_dict, 'open'),
            close=get_float_value(order_dict, 'close'),
            stop=get_float_value(order_dict, 'stop'),
            take=get_float_value(order_dict, 'take'),
            quantity=get_int_value(order_dict, 'quantity'),
            direction=get_int_value(order_dict, 'direction'),
            time=get_datetime_value(order_dict, 'time'),
            status=order_dict.get('status'),
            result=get_float_value(order_dict, 'result'),
            is_win=order_dict.get('is_win')
        )
