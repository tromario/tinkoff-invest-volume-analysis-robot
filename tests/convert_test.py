import unittest

from tinkoff_invest.utils.convert import Convert


class ConvertTest(unittest.TestCase):
    def test_convert_quotation_to_price(self):
        self.assertEqual(Convert.quotation_to_price('114', 250000000), 114.25)
        self.assertEqual(Convert.quotation_to_price('-200', -200000000), -200.20)
        self.assertEqual(Convert.quotation_to_price('-0', -10000000), -0.01)
        self.assertEqual(Convert.quotation_to_price('0', 37180000), 0.037180)


if __name__ == '__main__':
    unittest.main()