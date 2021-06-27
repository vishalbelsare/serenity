import fire

from serenity.app.daemon import ZeroMQDaemon


class MarketdataRecorder(ZeroMQDaemon):
    def get_service_id(self):
        return 'serenity/marketdata/recorder'

    def get_service_name(self):
        return 'marketdata-recorder'


def main(config_path: str):
    fh = MarketdataRecorder(config_path)
    fh.run_forever()


if __name__ == '__main__':
    fire.Fire(main)
