import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.binance import binance_constants as CONSTANTS, binance_web_utils as web_utils
from hummingbot.connector.exchange.binance.binance_order_book import BinanceOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.binance.binance_exchange import BinanceExchange


class BinanceAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'BinanceExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._domain = domain
        self._api_factory = api_factory

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "limit": "1000"
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.SNAPSHOT_PATH_URL, domain=self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SNAPSHOT_PATH_URL,
        )

        return data

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Doc : https://docs.upbit.com/docs/upbit-quotation-websocket

        For subscription, ticket information is commonly required.
        In order to reduce the data size, format parameter is set to 'SIMPLE' instead of 'DEFAULT'


        Examples (Note that the positions of the base and quote currencies are swapped.)

        1. In order to get TRADES of "BTC-KRW" and "XRP-BTC" markets.
        > [{"ticket":"UNIQUE_TICKET"},{"type":"trade","codes":["KRW-BTC","BTC-XRP"]}]

        2. In order to get ORDERBOOK of "BTC-KRW" and "XRP-BTC" markets.
        > [{"ticket":"UNIQUE_TICKET"},{"type":"orderbook","codes":["KRW-BTC","BTC-XRP"]}]

        3. In order to get TRADES of "BTC-KRW" and ORDERBOOK of "ETH-KRW"
        > [{"ticket":"UNIQUE_TICKET"},{"type":"trade","codes":["KRW-BTC"]},{"type":"orderbook","codes":["KRW-ETH"]}]

        4. In order to get TRADES of "BTC-KRW", ORDERBOOK of "ETH-KRW and TICKER of "EOS-KRW"
        > [{"ticket":"UNIQUE_TICKET"},{"type":"trade","codes":["KRW-BTC"]},{"type":"orderbook","codes":["KRW-ETH"]},{"type":"ticker", "codes":["KRW-EOS"]}]

        5. In order to get TRADES of "BTC-KRW", ORDERBOOK of "ETH-KRW and TICKER of "EOS-KRW" with in shorter format
        > [{"ticket":"UNIQUE_TICKET"},{"format":"SIMPLE"},{"type":"trade","codes":["KRW-BTC"]},{"type":"orderbook","codes":["KRW-ETH"]},{"type":"ticker", "codes":["KRW-EOS"]}]
        
        :param isOnlySnapshot: 시세 스냅샷만 제공 여부
        :type isOnlySnapshot: bool

        :param isOnlyRealtime: 실시간 시세만 제공 여부
        :type isOnlyRealtime: bool

        :param format: 포맷 (`SIMPLE`: 간소화된 필드명, `DEFAULT`: 기본값 (생략 가능))
        :type format: str
        """

        try:
            payload = []
            trading_pairs: List[str] = []
            for tp in self._trading_pairs:
                trading_pairs.append(tp.upper())
                # trading_pairs.append(convert_to_exchange_trading_pair(tp, '/'))
                # symbol = convert_to_exchange_trading_pair(tp, ',')
                # symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                # trading_pairs.append(symbol.upper())
            # codes = ['KRW-BTC', 'KRW-ETH', 'KRW-BCH', 'KRW-XRP'] 
            # codes = ["KRW-BTC.5", "KRW-ETH.5"]

            trades_payload = {
                "type": "trade",
                "codes": codes,
                'isOnlyRealtime': True
                # 'isOnlySnapshot': True
            }
            order_book_payload = {
                "type": "orderbook",
                "codes": codes,
                'isOnlyRealtime': True
                # 'isOnlySnapshot': True
            }

            import uuid
            ticket = str(uuid.uuid4())
            payload.append( { "ticket": ticket } )
            payload.extend(trades_payload)
            payload.extend(order_book_payload)
            payload.append( { "format": "SIMPLE" } ) # "SIMPLE" or "DEFAULT"
            # json.dumps(payload)

            subscribe_request: WSJSONRequest = WSJSONRequest(payload=payload)
            await ws.send(subscribe_request)

            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_URL.format(self._domain),
                         ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = BinanceOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if "result" not in raw_message:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["s"])
            trade_message = BinanceOrderBook.trade_message_from_exchange(
                raw_message, {"trading_pair": trading_pair})
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if "result" not in raw_message:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["s"])
            order_book_message: OrderBookMessage = BinanceOrderBook.diff_message_from_exchange(
                raw_message, time.time(), {"trading_pair": trading_pair})
            message_queue.put_nowait(order_book_message)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if "result" not in event_message:
            event_type = event_message.get("e")
            channel = (self._diff_messages_queue_key if event_type == CONSTANTS.DIFF_EVENT_TYPE
                       else self._trade_messages_queue_key)
        return channel
