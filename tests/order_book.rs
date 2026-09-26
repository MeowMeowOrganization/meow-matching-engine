use meow_matching_engine::{Order, OrderBook, OrderId, PriceTicks, QuantityLots, Side};

fn price(value: i64) -> PriceTicks {
    PriceTicks::new(value).expect("valid price")
}

fn quantity(value: i64) -> QuantityLots {
    QuantityLots::new(value).expect("valid quantity")
}

fn order(id: u64, side: Side, price_value: i64, quantity_value: i64) -> Order {
    Order::new(
        OrderId::new(id),
        side,
        price(price_value),
        quantity(quantity_value),
    )
    .expect("valid resting order")
}

#[test]
fn public_order_book_api_preserves_price_and_fifo_priority() {
    let mut book = OrderBook::new();

    book.insert(order(1, Side::Buy, 100, 4)).expect("insert");
    book.insert(order(2, Side::Buy, 105, 3)).expect("insert");
    book.insert(order(3, Side::Buy, 105, 7)).expect("insert");
    book.insert(order(4, Side::Sell, 110, 5)).expect("insert");
    book.insert(order(5, Side::Sell, 108, 6)).expect("insert");

    let best_bid = book.best_bid().expect("best bid");
    let best_ask = book.best_ask().expect("best ask");

    assert_eq!(best_bid.price(), price(105));
    assert_eq!(best_ask.price(), price(108));

    assert_eq!(
        best_bid.order_ids().collect::<Vec<_>>(),
        vec![OrderId::new(2), OrderId::new(3)],
    );

    assert_eq!(best_bid.aggregate_quantity(), quantity(10));
    assert_eq!(best_ask.aggregate_quantity(), quantity(6));

    assert_eq!(
        book.order(OrderId::new(3))
            .expect("order exists")
            .quantity(),
        quantity(7),
    );
}
