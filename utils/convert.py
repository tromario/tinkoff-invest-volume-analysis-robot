class Convert:
    @staticmethod
    def quotation_to_price(quotation):
        str_nano = f'{abs(quotation.nano):09}'
        str_price = f'{quotation.units}.{str_nano}'
        return float(str_price)
