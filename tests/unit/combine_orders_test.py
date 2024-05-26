from src.data_ops_detailed import combine_orders


def test_combine_orders(customers_fixture, orders_fixture):
    df = combine_orders(customers=customers_fixture, orders=orders_fixture)

    assert df.count() == 5
    assert df.columns == [
        "customer_id",
        "order_id",
        "order_name",
        "order_amount",
        "quantity",
        "created_at",
        "total_amount",
        "amount_type",
        "currency_type",
        "order_amount_rank",
        "pipeline_ts",
    ]
    assert df.head(1)[0].customer_id == 1
