import fire
from cryptofeed.exchanges import Gemini

from serenity.exchange.gemini import GeminiConnection
from serenity.marketdata.fh.feedhandler import CryptofeedFeedHandler


class GeminiFeedHandler(CryptofeedFeedHandler):
    def __init__(self, config_path: str):
        super().__init__(config_path)

    def get_service_id(self):
        return 'serenity/feedhandlers/gemini'

    def get_service_name(self):
        return 'gemini-fh'

    def get_feed_code(self):
        return 'GEMINI'

    def _create_feed(self, subscription: dict, callbacks: dict):
        return Gemini(subscription=subscription, callbacks=callbacks)

    def _load_symbols(self):
        gemini_conn = GeminiConnection()
        products = gemini_conn.get_products()
        self.logger.debug(f'downloaded {len(products)} symbols from Gemini exchange')

        product_details = [gemini_conn.get_product_details(product) for product in products]
        symbols = [f'{details["base_currency"]}-{details["quote_currency"]}' for details in product_details]

        # workaround for bug in handling longer symbol names in cryptofeed's Gemini support
        # Issue reported here: https://github.com/bmoscon/cryptofeed/issues/531
        symbols.remove('BTC-GUSD')
        symbols.remove('ETH-GUSD')

        return symbols


def main(config_path: str):
    fh = GeminiFeedHandler(config_path)
    fh.run_forever()


if __name__ == '__main__':
    fire.Fire(main)
