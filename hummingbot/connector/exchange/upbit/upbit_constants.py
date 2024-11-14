
from hummingbot.core.api_throttler.data_types import RateLimit  # , LinkedLimitWeightPair
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = ""
HBOT_ORDER_ID_PREFIX = "UPBIT-"
MAX_ORDER_ID_LEN = 32

# Base URL
REST_URL = "https://api.upbit.com/v1"
WSS_PUBLIC_URL = "wss://api.upbit.com/websocket/v1"
WSS_PRIVATE_URL = "wss://api.upbit.com/websocket/v1/private"

# Public API endpoints
# EXCHANGE_INFO_PATH_URL = "/exchangeInfo"
# PING_PATH_URL = "/ping"
# SERVER_TIME_PATH_URL = "/time"
EXCHANGE_INFO_MARKET_PATH_URL = "/market/all"
EXCHANGE_INFO_ORDER_PATH_URL = "orders/chance"
TICKER_PRICE_CHANGE_PATH_URL = "/ticker"
SNAPSHOT_PATH_URL = "/orderbook"

# Private API endpoints
ACCOUNTS_PATH_URL = "/accounts"
WALLET_PATH_URL = "/status/wallet"
CLOSED_ORDER_PATH_URL = "/orders/closed"
OPEN_ORDER_PATH_URL = "/orders/open"
CREATE_ORDER_PATH_URL = "/orders" # post
CANCEL_ORDER_PATH_URL = "/order"  # delete

# Public Websocket types
PRIVATE_TICKER_TYPE = "ticker"
PRIVATE_TRADE_TYPE = "trade"
PRIVATE_ORDERBOOK_TYPE = "orderbook"

# Private Websocket types
PRIVATE_ORDER_TYPE = "myOrder"
PRIVATE_WALLET_TYPE = "myAsset"

# Upbit params
SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

# default; GTC
# TIME_IN_FORCE_GTC = "GTC"  # Good till cancelled
TIME_IN_FORCE_IOC = "IOC"  # Immediate or cancel
TIME_IN_FORCE_FOK = "FOK"  # Fill or kill

# Rate Limit Type
# REQUEST_WEIGHT = "REQUEST_WEIGHT"
# ORDERS = "ORDERS"
# ORDERS_24HR = "ORDERS_24HR"
# RAW_REQUESTS = "RAW_REQUESTS"

# # Rate Limit time intervals
# ONE_MINUTE = 60
# ONE_SECOND = 1
# ONE_DAY = 86400

# MAX_REQUEST = 5000

# Order States
ORDER_STATE = {
    "wait": OrderState.OPEN,
    "watch": OrderState.PENDING_CREATE,
    "done": OrderState.FILLED,
    "cancel": OrderState.CANCELED
}

WS_HEARTBEAT_TIME_INTERVAL = 30

# Request error codes
# RET_CODE_OK = 0
RET_CODE_PARAMS_ERROR = 400
CREATE_ASK_ERROR = "create_ask_error"
CREATE_BID_ERROR = "create_bid_error"
INSUFFICIENT_FUNDS_ASK_ERROR = "insufficient_funds_ask"
INSUFFICIENT_FUNDS_BID_ERROR = "insufficient_funds_bid"
MIN_TOTAL_ASK_ERROR = "under_min_total_ask"
MIN_TOTAL_BID_ERROR = "under_min_total_bid"
VALIDATION_ERROR = "validation_error"

RET_CODE_AUTH_ERROR = 401
INVALID_PAYLOAD_ERROR = "invalid_query_payload"
JWT_ERROR = "jwt_verification"
EXPIRED_ACCESS_KEY_ERROR = "expired_access_key"
NONCE_ERROR = "nonce_used"
IP_ERROR = "no_authorization_i_p"
OUT_OF_SCOPE_ERROR = "out_of_scope"

# Upbit has a per method API limit
RATE_LIMITS = [
    RateLimit(limit_id=EXCHANGE_INFO_MARKET_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=EXCHANGE_INFO_ORDER_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=TICKER_PRICE_CHANGE_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=SNAPSHOT_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=ACCOUNTS_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=WALLET_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CLOSED_ORDER_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=OPEN_ORDER_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=8, time_interval=1),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=30, time_interval=1)
]
