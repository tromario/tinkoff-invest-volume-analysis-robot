import unittest
from tinkoff.invest import OrderDirection
from utils.order_util import prepare_orders


class TestOrders(unittest.TestCase):
    def test_prepare_buy_two_orders(self):
        actual_orders = prepare_orders(
            instrument='SBER',
            time='2022-05-20 15:13:15.830627+00:00',
            direction=OrderDirection.ORDER_DIRECTION_BUY,
            current_price=100,
            stop_loss=80,
            count_lots=10,
            count_goals=2,
            first_goal=3,
            goal_step=0.5,
        )
        expected = [
            {
                'id': actual_orders[0]['id'],
                'group_id': actual_orders[0]['group_id'],
                'instrument': 'SBER',
                'open': 100,
                'stop': 80,
                'take': 160,
                'quantity': 5,
                'direction': 1,
                'time': '2022-05-20 15:13:15.830627+00:00',
                'status': 'active'
            },
            {
                'id': actual_orders[1]['id'],
                'group_id': actual_orders[1]['group_id'],
                'instrument': 'SBER',
                'open': 100,
                'stop': 80,
                'take': 190,
                'quantity': 5,
                'direction': 1,
                'time': '2022-05-20 15:13:15.830627+00:00',
                'status': 'active'
            }
        ]
        self.assertEqual(len(actual_orders), 2)
        self.assertEqual(actual_orders, expected)

    def test_prepare_sell_three_orders(self):
        actual_orders = prepare_orders(
            instrument='SBER',
            time='2022-05-20 15:13:15.830627+00:00',
            direction=OrderDirection.ORDER_DIRECTION_SELL,
            current_price=100,
            stop_loss=120,
            count_lots=9,
            count_goals=3,
            first_goal=3,
            goal_step=0.5,
        )
        expected = [
            {
                'id': actual_orders[0]['id'],
                'group_id': actual_orders[0]['group_id'],
                'instrument': 'SBER',
                'open': 100,
                'stop': 120,
                'take': 40,
                'quantity': 3,
                'direction': 2,
                'time': '2022-05-20 15:13:15.830627+00:00',
                'status': 'active'
            },
            {
                'id': actual_orders[1]['id'],
                'group_id': actual_orders[1]['group_id'],
                'instrument': 'SBER',
                'open': 100,
                'stop': 120,
                'take': 10,
                'quantity': 3,
                'direction': 2,
                'time': '2022-05-20 15:13:15.830627+00:00',
                'status': 'active'
            },
            {
                'id': actual_orders[2]['id'],
                'group_id': actual_orders[2]['group_id'],
                'instrument': 'SBER',
                'open': 100,
                'stop': 120,
                'take': -20,
                'quantity': 3,
                'direction': 2,
                'time': '2022-05-20 15:13:15.830627+00:00',
                'status': 'active'
            }
        ]
        self.assertEqual(len(actual_orders), 3)
        self.assertEqual(actual_orders, expected)


if __name__ == '__main__':
    unittest.main()
