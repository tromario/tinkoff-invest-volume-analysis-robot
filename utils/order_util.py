from typing import List
from uuid import uuid4

from domains.order import Order


def prepare_orders(
        instrument,
        current_price,
        time,
        stop_loss,
        direction,
        count_lots,
        count_goals,
        goal_step,
        first_goal
) -> List[Order]:
    group_id = str(uuid4())
    quantity = int(count_lots / count_goals)

    orders = []
    step = 1
    final_step = (goal_step * count_goals) + 1
    while step < final_step:
        take = current_price - ((stop_loss - current_price) * first_goal * step)
        order = Order(
            id=str(uuid4()),
            group_id=group_id,
            instrument=instrument,
            open= current_price,
            stop=stop_loss,
            take=take,
            quantity=quantity,
            direction=direction.value,
            time=time
        )
        orders.append(order)
        step += goal_step
    return orders


def is_order_already_open(orders: List[Order], order: Order):
    active_order = list(filter(
        lambda item: item.direction == order.direction and
                     item.status == 'active', orders)
    )
    if len(active_order) > 0:
        # если уже есть активная заявка, но она с одной группы (точки входа), то считаю ее новой
        if order.group_id == active_order[0].group_id:
            return False
        # если уже есть активная заявка, но она не совпадает с текущей группой (точкой входа),
        # то запрещаю создание новой до тех пор, пока активная заявка не будет закрыта
        return True
    return False
