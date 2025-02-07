import asyncio

# import time
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.upbit import (  # upbit_utils as upbit_utils,; upbit_web_utils as web_utils,
    upbit_constants as CONSTANTS,
)
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
                 api_factory: WebAssistantsFactory):
        super().__init__()
        self._auth: UpbitAuth = auth
        self._api_factory = api_factory

    async def _connected_websocket_assistant(self) -> WSAssistant:
        # ws: WSAssistant = await self._api_factory.get_ws_assistant()
        ws: WSAssistant = await self._get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_PRIVATE_URL.format(CONSTANTS.PRIVATE_API_VERSION),
                         ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL,
                         ws_headers=self._auth.get_auth_headers())
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
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
            payload.append(asset_payload)
            payload.append(order_payload)
            payload.append({"format": "SIMPLE"})  # "SIMPLE" or "DEFAULT"
            # json.dumps(payload)

            subscribe_request: WSJSONRequest = WSJSONRequest(payload=payload, is_auth_required=True)
            await websocket_assistant.send(subscribe_request)

            self.logger().info("Subscribed to private order and asset channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to private order and asset streams...",
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
