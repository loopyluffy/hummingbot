import asyncio

# import time
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.upbit import upbit_constants as CONSTANTS  # , upbit_web_utils as web_utils
from hummingbot.connector.exchange.upbit.upbit_auth import UpbitAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource

# from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest  # , RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.upbit.upbit_exchange import UpbitExchange


class UpbitAPIUserStreamDataSource(UserStreamTrackerDataSource):

    # HEARTBEAT_TIME_INTERVAL = 30.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: UpbitAuth,
                 trading_pairs: List[str],
                 connector: 'UpbitExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: UpbitAuth = auth
        self._current_listen_key = None
        self._domain = domain
        self._api_factory = api_factory

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """
        ws: WSAssistant = await self._get_ws_assistant()
        # url = f"{CONSTANTS.WSS_URL.format(self._domain)}/{self._current_listen_key}"
        url = f"{CONSTANTS.WSS_PRIVATE_URL.format(CONSTANTS.PUBLIC_API_VERSION)}/{self._current_listen_key}"
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            payload = []
            asset_payload = {
                "type": "myAsset",
                # "codes": trading_pairs,
            }
            order_payload = {
                "type": "myOrder",
                # "codes": trading_pairs,
            }

            import uuid
            ticket = str(uuid.uuid4())
            payload.append({"ticket": ticket})
            payload.extend(asset_payload)
            payload.extend(order_payload)
            payload.append({"format": "SIMPLE"})  # "SIMPLE" or "DEFAULT"
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

    # async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
    #     if len(event_message) > 0 and "table" in event_message and "data" in event_message:
    #         queue.put_nowait(event_message)

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant
