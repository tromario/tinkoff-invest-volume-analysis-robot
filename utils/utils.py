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
