import fire
from cryptofeed import FeedHandler
from cryptofeed.callback import BookCallback, TradeCallback
from cryptofeed.defines import BID, ASK, L2_BOOK, TRADES
from cryptofeed.exchanges import Gemini

from serenity.app.daemon import AIODaemon
from serenity.exchange.gemini import GeminiConnection


# noinspection PyUnusedLocal
async def trade(feed, symbol, order_id, timestamp, side, amount, price, receipt_timestamp, order_type):
    print(f"Timestamp: {timestamp} Feed: {feed} Pair: {symbol} ID: {order_id} Side: {side} Amount: {amount} "
          f"Price: {price} Order Type {order_type}")


# noinspection PyUnusedLocal
async def book(feed, symbol, book, timestamp, receipt_timestamp):
    print(f'Timestamp: {timestamp} Feed: {feed} Pair: {symbol} Book Bid Size is {len(book[BID])} '
          f'Ask Size is {len(book[ASK])}')


class GeminiFeedhandler(AIODaemon):
    def get_service_id(self):
        return 'serenity/feedhandlers/gemini'

    def get_service_name(self):
        return 'gemini-fh'

    def __init__(self, config_path: str):
        super().__init__(config_path)
        self.get_event_loop().call_soon(GeminiFeedhandler.run)

    @staticmethod
    def run():
        f = FeedHandler()

        gemini_conn = GeminiConnection()
        products = gemini_conn.get_products()
        product_details = [gemini_conn.get_product_details(product) for product in products]
        normalized_products = [f'{details["base_currency"]}-{details["quote_currency"]}' for details in product_details]
        normalized_products.remove('BTC-GUSD')
        normalized_products.remove('ETH-GUSD')

        f.add_feed(Gemini(subscription={TRADES: normalized_products, L2_BOOK: normalized_products}, callbacks={
            TRADES: TradeCallback(trade, include_order_type=True),
            L2_BOOK: BookCallback(book)
        }))

        f.run(start_loop=False)


def main(config_path: str):
    fh = GeminiFeedhandler(config_path)
    fh.run_forever()


if __name__ == '__main__':
    fire.Fire(main)
