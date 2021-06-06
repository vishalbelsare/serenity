import datetime

from pathlib import Path


class TransactionLog:
    def __init__(self, base_path: Path):
        self.base_path = base_path

    def create_reader(self, date: datetime.date = datetime.datetime.utcnow().date()):
        return TransactionLogReader(self, date)

    def create_writer(self, date: datetime.date = datetime.datetime.utcnow().date()):
        return TransactionLogWriter(self, date)

    def get_tx_file_path(self, date: datetime.date):
        return self.base_path.joinpath(Path('%4d%02d%02d/journal.dat' % (date.year, date.month, date.day)))


class TransactionLogWriter:
    def __init__(self, txlog: TransactionLog, current_date):
        self.txlog = txlog
        self.current_date = current_date
        self.txlog_file = self._open_tx_file(self.current_date)

    # noinspection PyUnresolvedReferences
    def append_msg(self, msg: object):
        msg.write_packed(self._get_current_tx_file())

    def close(self):
        self.txlog_file.close()

    def _get_current_tx_file(self):
        now_date = datetime.datetime.utcnow().date()
        if self.current_date != now_date:
            self.current_date = now_date
            self.close()
            self.txlog_file = self._open_tx_file(self.current_date)

        return self.txlog_file

    def _open_tx_file(self, date: datetime.date):
        tx_file_path = self.txlog.get_tx_file_path(date)
        tx_file_path.parent.mkdir(parents=True, exist_ok=True)
        return open(str(tx_file_path), 'a+b')


class TransactionLogReader:
    def __init__(self, txlog: TransactionLog, current_date):
        self.txlog = txlog
        self.current_date = current_date
        tx_file_path = self.txlog.get_tx_file_path(current_date)
        self.txlog_file = open(str(tx_file_path), 'r+b')

    # noinspection PyUnresolvedReferences
    def read_messages(self, msg_class: object):
        return msg_class.read_multiple_packed(self.txlog_file)



