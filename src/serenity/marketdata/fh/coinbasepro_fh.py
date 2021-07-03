import coinbasepro
import fire

from cryptofeed.exchange.coinbase import Coinbase
from serenity.marketdata.fh.feedhandler import CryptofeedFeedHandler


class CoinbaseProFeedHandler(CryptofeedFeedHandler):
    """
    Market data feedhandler for the Coinbase Pro exchange. Supports both trade print and order book feeds.
    """

    def get_service_id(self):
        return 'serenity/feedhandlers/coinbasepro'

    def get_service_name(self):
        return 'feedhandler'

    def get_feed_code(self):
        return 'COINBASE_PRO'

    def _create_feed(self, subscription: dict, callbacks: dict):
        return Coinbase(subscription=subscription, callbacks=callbacks)

    def _load_symbols(self):
        instance_id = self.get_config('exchange', 'instance_id', 'prod')
        if instance_id == 'prod':
            cbp_client = coinbasepro.PublicClient()
        elif instance_id == 'test':
            cbp_client = coinbasepro.PublicClient(api_url='https://api-public.sandbox.pro.coinbase.com')
        else:
            raise ValueError(f'Unknown instance_id: {instance_id}')

        self.logger.info(f'Downloading supported products from {instance_id} exchange')
        symbols = []
        for product in cbp_client.get_products():
            base_ccy = product['base_currency']
            quote_ccy = product['quote_currency']
            symbol = f'{base_ccy}-{quote_ccy}'
            symbols.append(symbol)
        return symbols


def main(config_path: str):
    fh = CoinbaseProFeedHandler(config_path)
    fh.run_forever()


if __name__ == '__main__':
    fire.Fire(main)
