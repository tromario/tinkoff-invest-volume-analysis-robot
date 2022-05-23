from datetime import datetime

from tinkoff.invest import Quotation
from tinkoff.invest.utils import quotation_to_decimal


def quotation_to_float(quotation: Quotation):
    return float(quotation_to_decimal(quotation))


def fixed_float(number: float) -> str:
    return f"{number:.3f}"


def parse_date(str_date):
    try:
        return datetime.strptime(str_date, "%Y-%m-%d %H:%M:%S.%f%z")
    except ValueError as ex:
        pass

    try:
        return datetime.strptime(str_date, "%Y-%m-%d %H:%M:%S%z")
    except ValueError as ex:
        pass

    return None