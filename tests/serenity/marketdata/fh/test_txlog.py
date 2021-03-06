import tempfile
from datetime import datetime
from pathlib import Path

from serenity.marketdata.fh.feedhandler import feedhandler_capnp
from serenity.marketdata.fh.txlog import TransactionLog


def test_txlog_read_write():
    tmp_txfile_dir = tempfile.TemporaryDirectory()
    tmp_txfile_path = Path(tmp_txfile_dir.name)
    txlog = TransactionLog(tmp_txfile_path)

    txlog_writer = txlog.create_writer()

    book_msg = feedhandler_capnp.Level1BookUpdateMessage.new_message()
    book_msg.time = datetime.utcnow().timestamp()
    book_msg.bestBidQty = 5.0
    book_msg.bestBidPx = 38000.0
    book_msg.bestAskQty = 9.0
    book_msg.bestAskPx = 38001.0

    txlog_writer.append_msg(book_msg)
    txlog_writer.close()

    txlog_reader = txlog.create_reader()
    book_msgs = txlog_reader.read_messages(feedhandler_capnp.Level1BookUpdateMessage)
    loaded_book_msg = next(book_msgs)
    assert book_msg.bestBidQty == loaded_book_msg.bestBidQty
    assert book_msg.bestBidPx == loaded_book_msg.bestBidPx
    assert book_msg.bestAskQty == loaded_book_msg.bestAskQty
    assert book_msg.bestAskPx == loaded_book_msg.bestAskPx
