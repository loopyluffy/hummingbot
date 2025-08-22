import asyncio

# import math
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.upbit import (
    upbit_constants as CONSTANTS,
    upbit_utils as upbit_utils,
    upbit_web_utils as web_utils,
)
from hummingbot.connector.exchange.upbit.upbit_api_order_book_data_source import UpbitAPIOrderBookDataSource
from hummingbot.connector.exchange.upbit.upbit_api_user_stream_data_source import UpbitAPIUserStreamDataSource
from hummingbot.connector.exchange.upbit.upbit_auth import UpbitAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource

# from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
# from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class UpbitExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 upbit_api_key: str,
                 upbit_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = upbit_api_key
        self.secret_key = upbit_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_binance_timestamp = 1.0
        super().__init__(client_config_map)

    @staticmethod
    def upbit_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(binance_type: str) -> OrderType:
        return OrderType[binance_type]

    @property
    def authenticator(self):
        return UpbitAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return "upbit"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_MARKET_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_MARKET_PATH_URL

    @property
    def check_network_request_path(self):
        # return CONSTANTS.PING_PATH_URL
        return CONSTANTS.EXCHANGE_INFO_MARKET_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        # return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]
        return [OrderType.LIMIT]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        res = []
        params = {
            "quote_currencies": "KRW"  # "KRW,BTC"
        }
        pairs_prices = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PRICE_ALL_PATH_URL,
            params=params
        )
        for pair_price_data in pairs_prices:
            result = {}
            result["trading_pair"] = await self.trading_pair_associated_to_exchange_symbol(pair_price_data["market"])
            result["price"] = pair_price_data["trade_price"]
            res.append(result)
        return res

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        # error_description = str(request_exception)
        # is_time_synchronizer_related = ("-1021" in error_description and "Timestamp for this request" in error_description)
        # return is_time_synchronizer_related
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_lost_order_removed_if_not_found_during_order_status_update
        # when replacing the dummy implementation
        return str(CONSTANTS.ORDER_ERROR_CODE) in str(
            status_update_exception
        )  # and CONSTANTS.CANCELED_ORDER_MESSAGE in str(cancelation_exception)
        # return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        # TODO: implement this method correctly for the connector
        # The default implementation was added when the functionality to detect not found orders was introduced in the
        # ExchangePyBase class. Also fix the unit test test_cancel_order_not_found_in_the_exchange when replacing the
        # dummy implementation
        return str(CONSTANTS.ORDER_ERROR_CODE) in str(
            cancelation_exception
        )  # and CONSTANTS.CANCELED_ORDER_MESSAGE in str(cancelation_exception)
        # return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            # time_synchronizer=self._time_synchronizer,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return UpbitAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return UpbitAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        # is_maker = order_type is OrderType.LIMIT_MAKER
        is_maker = True
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        order_result = None
        amount_str = f"{amount:f}"
        type_str = UpbitExchange.upbit_order_type(order_type)
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        api_params = {
            "identifier": order_id,
            'market': symbol,
            'side': side_str,
            'ord_type': type_str,
            'volume': amount_str
        }

        if order_type is OrderType.LIMIT or order_type is OrderType.LIMIT_MAKER:
            price_str = f"{price:f}"
            api_params["price"] = price_str
        # if order_type == OrderType.LIMIT:
        #     api_params["time_in_force"] = CONSTANTS.TIME_IN_FORCE_GTC

        try:
            order_result = await self._api_post(
                path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
                params=api_params,
                # data=api_params,
                is_auth_required=True)
            o_id = str(order_result["uuid"])
            # transact_time = order_result["created_at"] * 1e-3
            # transact_time = datetime.fromisoformat(order_result["created_at"])
            # Convert to datetime object
            dt = datetime.fromisoformat(order_result["created_at"])
            # Convert to Unix timestamp (seconds since epoch)
            transact_time = dt.timestamp()
        except IOError as e:
            error_description = str(e)
            is_server_overloaded = ("status is 503" in error_description
                                    and "Unknown error, please check your request or try again later." in error_description)
            if is_server_overloaded:
                o_id = "UNKNOWN"
                transact_time = self._time_synchronizer.time()
            else:
                raise
        return o_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        # symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        api_params = {
            # "symbol": symbol,
            "identifier": order_id,
        }
        cancel_result = await self._api_delete(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            params=api_params,
            is_auth_required=True)
        # if cancel_result.get("status") == "CANCELED":
        if cancel_result.get("identifier") == order_id:
            return True
        return False

    # async def _status_polling_loop_fetch_updates(self):
    #     await self._update_order_fills_from_trades()
    #     await super()._status_polling_loop_fetch_updates()

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                # preprocessing for message
                event_message = upbit_utils.preprocessing_message(event_message)
                event_type = event_message.get("ty")  # ty;type
                # Refer to https://docs.upbit.com/reference/websocket-myorder
                if event_type == CONSTANTS.PRIVATE_ORDER_CHANNEL_TYPE:
                    client_order_id = event_message.get("id")  # id; identifier
                    fillable_order = self._order_tracker.all_fillable_orders.get(client_order_id)
                    updatable_order = self._order_tracker.all_updatable_orders.get(client_order_id)

                    execution_type = event_message.get("s")  # s;state (wait, watch, trade, done, cancel)
                    new_state = CONSTANTS.ORDER_STATE[execution_type]
                    event_timestamp = int(event_message["tms"]) * 1e-3  # tms; timestamp

                    if fillable_order is not None:
                        # is_fill_candidate_by_state = new_state in [OrderState.PARTIALLY_FILLED,
                        #                                            OrderState.FILLED]
                        # is_fill_candidate_by_amount = fillable_order.executed_amount_base < Decimal(event_message["ev"]) # ev;executed_volume
                        if execution_type == "trade":
                            # fee = TradeFeeBase.new_spot_fee(
                            #     fee_schema=self.trade_fee_schema(),
                            #     trade_type=fillable_order.trade_type,
                            #     percent_token="KRW",
                            #     flat_fees=[TokenAmount(amount=Decimal(event_message["pf"]), token="KRW")] # pf;paid_fee
                            # )
                            trade_update = TradeUpdate(
                                trade_id=str(event_message["tuid"]),  # tuid;trade_uuid
                                client_order_id=client_order_id,
                                exchange_order_id=str(event_message["uid"]),  # uid;uuid
                                trading_pair=fillable_order.trading_pair,
                                fee=event_message["pf"],  # pf;paid_fee
                                fill_base_amount=Decimal(event_message["ev"]),  # ev;executed_volume
                                fill_quote_amount=Decimal(event_message["ef"]),  # ef;executed_funds
                                # fill_quote_amount=Decimal(event_message["ev"]) * Decimal(event_message["ap"]),
                                fill_price=Decimal(event_message["ap"]),  # ap;avg_price
                                fill_timestamp=event_timestamp,
                            )
                            self._order_tracker.process_trade_update(trade_update)
                    if updatable_order is not None:
                        order_update = OrderUpdate(
                            trading_pair=updatable_order.trading_pair,
                            update_timestamp=event_timestamp,
                            new_state=new_state,
                            client_order_id=client_order_id,
                            exchange_order_id=event_message["uid"],
                        )
                        self._order_tracker.process_order_update(order_update=order_update)
                # Refer to https://docs.upbit.com/reference/websocket-myasset
                elif event_type == CONSTANTS.PRIVATE_WALLET_CHANNEL_TYPE:
                    balances = event_message["ast"]  # ast;assets
                    for balance_entry in balances:
                        asset_name = balance_entry["cu"]  # cu;currency
                        free_balance = Decimal(balance_entry["b"])  # b;balance
                        locked_balance = Decimal(balance_entry["l"])  # l;locked
                        total_balance = free_balance + locked_balance
                        self._account_available_balances[asset_name] = free_balance
                        self._account_balances[asset_name] = total_balance

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []
        try:
            if order.exchange_order_id is not None:
                trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
                order_response = await self._api_get(
                    path_url=CONSTANTS.ORDER_PATH_URL,
                    params={
                        "uuid": order.exchange_order_id
                    },
                    is_auth_required=True)

                for trade in order_response["trades"]:
                    # fee = TradeFeeBase.new_spot_fee(
                    #     fee_schema=self.trade_fee_schema(),
                    #     trade_type=fillable_order.trade_type,
                    #     percent_token="KRW",
                    #     flat_fees=[TokenAmount(amount=Decimal(event_message["pf"]), token="KRW")] # pf;paid_fee
                    # )
                    fee = order_response["paid_fee"] / order_response["trades_count"]
                    trade_update = TradeUpdate(
                        trade_id=trade["uuid"],
                        client_order_id=order.client_order_id,
                        exchange_order_id=order_response["uuid"],
                        trading_pair=trading_pair,
                        fee=fee,
                        fill_base_amount=Decimal(trade["volume"]),
                        fill_quote_amount=Decimal(trade["funds"]),
                        fill_price=Decimal(trade["price"]),  # ap;avg_price
                        fill_timestamp=datetime.fromisoformat(trade["created_at"]).timestamp(),
                    )
                    trade_updates.append(trade_update)
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            is_error_caused_by_unexistent_order = '"code":50005' in str(ex)
            if not is_error_caused_by_unexistent_order:
                raise

        return trade_updates

    async def _all_trade_updates_for_order_legacy(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []
        try:
            if order.exchange_order_id is not None:
                trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
                all_fills_response = await self._api_get(
                    path_url=CONSTANTS.ORDER_PATH_URL,
                    params={
                        # "market": trading_pair,
                        "uuids[]": [order.exchange_order_id]
                    },
                    is_auth_required=True)

                for trade in all_fills_response:
                    # fee = TradeFeeBase.new_spot_fee(
                    #     fee_schema=self.trade_fee_schema(),
                    #     trade_type=fillable_order.trade_type,
                    #     percent_token="KRW",
                    #     flat_fees=[TokenAmount(amount=Decimal(event_message["pf"]), token="KRW")] # pf;paid_fee
                    # )
                    fee = trade["paid_fee"]
                    trade_update = TradeUpdate(
                        # trade_id=str(trade["trade_uuid"]), # tuid;trade_uuid
                        client_order_id=order.client_order_id,
                        exchange_order_id=str(trade["uuid"]),  # uid;uuid
                        trading_pair=trading_pair,
                        fee=fee,
                        fill_base_amount=Decimal(trade["executed_volume"]),  # ev;executed_volume
                        fill_quote_amount=Decimal(trade["executed_funds"]),  # ef;executed_funds
                        # fill_quote_amount=Decimal(event_message["ev"]) * Decimal(event_message["ap"]),
                        fill_price=Decimal(trade["price"]),  # ap;avg_price
                        # fill_timestamp=event_timestamp,
                    )
                    trade_updates.append(trade_update)
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            is_error_caused_by_unexistent_order = '"code":50005' in str(ex)
            if not is_error_caused_by_unexistent_order:
                raise

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        updated_order_data = await self._api_get(
            path_url=CONSTANTS.ORDER_STATUS_URL,
            params={
                "market": trading_pair,
                "uuids[]": [tracked_order.exchange_order_id]},
            is_auth_required=True)

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=updated_order_data[0]["uuid"],
            trading_pair=tracked_order.trading_pair,
            new_state=CONSTANTS.ORDER_STATE[updated_order_data[0]["state"]],
            update_timestamp=self.current_timestamp,
            # update_timestamp=updated_order_data[0]["updateTime"] * 1e-3,
        )

        return order_update

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_info = await self._api_get(
            path_url=CONSTANTS.ACCOUNTS_PATH_URL,
            is_auth_required=True)

        for balance_entry in account_info:
            asset_name = balance_entry["currency"]
            free_balance = Decimal(balance_entry["balance"])
            locked_balance = Decimal(balance_entry["locked"])
            total_balance = free_balance + locked_balance
            self._account_available_balances[asset_name] = free_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info):
        mapping = bidict()
        # for symbol_data in filter(upbit_utils.is_exchange_information_valid, exchange_info.values()):
        for symbol_data in exchange_info:
            quote_asset, base_asset = symbol_data["market"].split("-")
            mapping[symbol_data["market"]] = combine_to_hb_trading_pair(base=base_asset,
                                                                        quote=quote_asset)
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        params = {
            "markets": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        }

        resp_json = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PRICE_PATH_URL,
            params=params
        )

        return float(resp_json[0]["trade_price"])

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        # return []
        trading_rules = {}
        for exchange_info in exchange_info_dict:
            if upbit_utils.is_exchange_information_valid(exchange_info=exchange_info):
                try:
                    exchange_symbol = exchange_info["market"]
                    trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=exchange_symbol)
                    # collateral_token = instrument["supportMarginCoins"][0]
                    trading_rules[trading_pair] = TradingRule(
                        trading_pair=trading_pair,
                        # min_price_increment=self.get_order_price_quantum(trading_pair), # tick_size
                        min_price_increment=self.get_order_size_quantum(trading_pair),  # tick_size
                        min_base_amount_increment=self.get_order_size_quantum(trading_pair),  # lot_size
                        # min_order_size=self.get_min_order_size(trading_pair, price=),
                        min_order_size=0,
                        min_notional_size=self.get_min_notional_size(trading_pair),
                    )
                except Exception:
                    self.logger().exception(f"Error parsing the trading pair rule: {exchange_info}. Skipping.")
        return list(trading_rules.values())

    # async def _update_trading_rules(self):
    #     # upbit's rule is static...
    #     # define order_price_quantum and order_size_quantum manually;;
    #     # dummy data for compatibility
    #     self._trading_rules["LOOPY-KRW"] = TradingRule("LOOPY-KRW")
    # ExchangePyBase._update_trading_rules
    #   exchange_info = await self._make_trading_rules_request()
    #   trading_rules_list = await self._format_trading_rules(exchange_info)
    #   self._trading_rules.clear()
    #   for trading_rule in trading_rules_list:
    #       self._trading_rules[trading_rule.trading_pair] = trading_rule
    #   self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)

    def get_order_price_quantum(self, trading_pair: str, price: Decimal) -> Decimal:
        # reference https://docs.upbit.com/docs/krw-market-info
        base_asset, quote_asset = trading_pair.split("-")
        tick_size = 0
        exception_krw_market = ["ADA", "ALGO", "BLUR", "CELO", "ELF", "EOS", "GRS", "GRT", "ICX", "MANA", "MINA", "POL", "SAND", "SEI", "STG", "TRX"]

        # if price is None:
        #     price = self.get_price_by_type(trading_pair, PriceType.LastTrade)

        if quote_asset == "KRW":
            if price >= 2000000:
                tick_size = 1000
            elif price >= 1000000:
                tick_size = 500
            elif price >= 500000:
                tick_size = 100
            elif price >= 100000:
                tick_size = 50
            elif price >= 10000:
                tick_size = 10
            elif price >= 1000:
                tick_size = 1
            elif price >= 100:
                if base_asset in exception_krw_market:
                    tick_size = 1
                else:
                    tick_size = 0.1
            elif price >= 10:
                tick_size = 0.01
            elif price >= 1:
                tick_size = 0.001
            elif price >= 0.1:
                tick_size = 0.0001
            elif price >= 0.01:
                tick_size = 0.00001
            elif price >= 0.001:
                tick_size = 0.000001
            elif price >= 0.0001:
                tick_size = 0.0000001
            else:
                tick_size = 0.00000001
        elif quote_asset == "BTC":
            tick_size = 0.00000001
        elif quote_asset == "USDT":
            if price >= 10:
                tick_size = 0.01
            elif price >= 1:
                tick_size = 0.001
            elif price >= 0.1:
                tick_size = 0.0001
            elif price >= 0.01:
                tick_size = 0.00001
            elif price >= 0.001:
                tick_size = 0.000001
            elif price >= 0.0001:
                tick_size = 0.0000001
            else:
                tick_size = 0.00000001

        return Decimal(tick_size)

    def get_order_size_quantum(self, trading_pair: str, order_size: Decimal = None) -> Decimal:
        base_asset, quote_asset = trading_pair.split("-")
        size_quantum = 0.00000001

        return Decimal(size_quantum)

    def get_min_notional_size(self, trading_pair: str) -> Decimal:
        base_asset, quote_asset = trading_pair.split("-")
        min_notional = 0

        if quote_asset == "KRW":
            min_notional = 5000
        elif quote_asset == "BTC":
            min_notional = 0.00005
        elif quote_asset == "USDT":
            min_notional = 0.5

        return Decimal(min_notional)

    def get_min_order_size(self, trading_pair: str, price: Decimal) -> Decimal:
        base_asset, quote_asset = trading_pair.split("-")
        min_notional_size = self.get_min_notional_size(trading_pair)
        min_order_size = min_notional_size / price

        return Decimal(min_order_size)

    # def get_order_price_quantum(self, trading_pair: str, price: Decimal) -> Decimal:
    #     """
    #     Used by quantize_order_price() in _create_order()
    #     Returns a price step, a minimum price increment for a given trading pair.

    #     :param trading_pair: the trading pair to check for market conditions
    #     :param price: the starting point price
    #     """
    #     trading_rule = self._trading_rules[trading_pair]
    #     return Decimal(trading_rule.min_price_increment)

    # def get_order_size_quantum(self, trading_pair: str, order_size: Decimal) -> Decimal:
    #     """
    #     Used by quantize_order_price() in _create_order()
    #     Returns an order amount step, a minimum amount increment for a given trading pair.

    #     :param trading_pair: the trading pair to check for market conditions
    #     :param order_size: the starting point order price
    #     """
    #     trading_rule = self._trading_rules[trading_pair]
    #     return Decimal(trading_rule.min_base_amount_increment)
