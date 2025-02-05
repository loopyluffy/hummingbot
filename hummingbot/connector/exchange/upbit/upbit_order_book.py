from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class UpbitOrderBook(OrderBook):

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a snapshot message with the order book snapshot message
        :param msg: the response from the exchange when requesting the order book snapshot
        :param timestamp: the snapshot timestamp
        :param metadata: a dictionary with extra information to add to the snapshot data
        :return: a snapshot message with the snapshot information received from the exchange
        """
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            # "update_id": msg["lastUpdateId"],
            "update_id": msg["tms"],  # timestamp
            "bids": [(order["bp"], order["bs"]) for order in msg["obu"]],  # orderbook_units.bid_price, bid_size
            "asks": [(order["ap"], order["as"]) for order in msg["obu"]]   # orderbook_units.ask_price, ask_size
        }, timestamp=timestamp)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a diff message with the changes in the order book received from the exchange
        :param msg: the changes in the order book
        :param timestamp: the timestamp of the difference
        :param metadata: a dictionary with extra information to add to the difference data
        :return: a diff message with the changes in the order book notified by the exchange
        """
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["trading_pair"],
            # "first_update_id": msg["U"],
            # "update_id": msg["u"],
            "update_id": msg["tms"],  # msg["timestamp"],
            "bids": [(order["bp"], order["bs"]) for order in msg["obu"]],
            "asks": [(order["ap"], order["as"]) for order in msg["obu"]]
        }, timestamp=timestamp)

    @classmethod
    def trade_message_from_exchange(cls, msg: Dict[str, any], metadata: Optional[Dict] = None):
        """
        Creates a trade message with the information from the trade event sent by the exchange
        :param msg: the trade event details sent by the exchange
        :param metadata: a dictionary with extra information to add to trade message
        :return: a trade message with the details of the trade as provided by the exchange
        """
        if metadata:
            msg.update(metadata)
        ts = float(msg["tms"])
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": msg["trading_pair"],
            "trade_type": float(TradeType.SELL.value) if msg["ab"] == "ASK" else float(TradeType.BUY.value),  # ask_bid
            "trade_id": msg["sid"],   # sid;sequential_id
            "update_id": msg["tms"],  # tms;timestamp
            "price": msg["tp"],       # tp;trade_price
            "amount": msg["tv"]       # tv;trade_volume
        }, timestamp=ts * 1e-3)
        # }, timestamp=ts)
