from datetime import datetime
from pathlib import Path

from serenity.marketdata.fh.feedhandler import capnp_def
from serenity.marketdata.fh.txlog import TransactionLog
from tempfile import mkstemp


def test_txlog_read_write():
    (tmp_fd, tmp_txfile) = mkstemp()
    tmp_txfile_path = Path(tmp_txfile)
    txlog = TransactionLog(tmp_txfile_path)

    txlog_writer = txlog.create_writer()

    book_msg = capnp_def.Level1BookUpdateMessage.new_message()
    book_msg.time = datetime.utcnow().timestamp()
    book_msg.bestBidQty = 5.0
    book_msg.bestBidPx = 38000.0
    book_msg.bestAskQty = 9.0
    book_msg.bestAskPx = 38001.0

    txlog_writer.append_msg(book_msg)
    txlog_writer.close()

    txlog_reader = txlog.create_reader()
    book_msgs = txlog_reader.read_messages(capnp_def.Level1BookUpdateMessage)
    assert len(book_msgs) == 1
    loaded_book_msg = book_msgs[0]
    assert book_msg.bestBidQty == loaded_book_msg.bestBidQty
    assert book_msg.bestBidPx == loaded_book_msg.bestBidPx
    assert book_msg.bestAskQty == loaded_book_msg.bestAskQty
    assert book_msg.bestAskPx == loaded_book_msg.bestAskPx
