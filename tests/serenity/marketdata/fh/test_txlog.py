from serenity.marketdata.fh.txlog import create_trade_message, create_level1_order_book_update_message


def test_load_capnp():
    create_trade_message()
    create_level1_order_book_update_message()
