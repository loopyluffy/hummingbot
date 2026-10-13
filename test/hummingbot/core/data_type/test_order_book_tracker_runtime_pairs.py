import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.perpetual_trading import PerpetualTrading
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource


class _DataSource(OrderBookTrackerDataSource):
    """Subscriptions and snapshots without websockets, sharing the tracker's list like the connectors do."""

    def __init__(self, trading_pairs):
        super().__init__(trading_pairs)
        self.subscribed, self.unsubscribed = [], []
        self.subscribe_ok, self.snapshot_fails = True, False

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        if not self.subscribe_ok:
            return False
        self.subscribed.append(trading_pair)
        self.add_trading_pair(trading_pair)
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        self.unsubscribed.append(trading_pair)
        self.remove_trading_pair(trading_pair)
        return True

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        if self.snapshot_fails:
            raise IOError("snapshot refused")
        book = OrderBook()
        book.apply_snapshot([OrderBookRow(99.0, 1.0, 100)], [OrderBookRow(101.0, 1.0, 100)], 100)
        return book

    async def get_last_traded_prices(self, trading_pairs, domain=None):
        return {}


def _diff(pair: str, update_id: int) -> OrderBookMessage:
    return OrderBookMessage(OrderBookMessageType.DIFF,
                            {"trading_pair": pair, "update_id": update_id, "bids": [], "asks": []}, timestamp=1.0)


class RuntimeTradingPairsTest(IsolatedAsyncioWrapperTestCase):
    async def asyncSetUp(self):
        self.pairs = ["BTC-USDT"]
        self.ds = _DataSource(self.pairs)
        self.tracker = OrderBookTracker(data_source=self.ds, trading_pairs=self.pairs)
        self.tracker._order_books_initialized.set()

    async def asyncTearDown(self):
        for task in self.tracker._tracking_tasks.values():
            task.cancel()

    async def test_add_subscribes_snapshots_and_tracks(self):
        saved = self.tracker._saved_message_queues["ETH-USDT"]
        saved.extend([_diff("ETH-USDT", 99), _diff("ETH-USDT", 100), _diff("ETH-USDT", 101)])
        self.assertTrue(await self.tracker.add_trading_pair("ETH-USDT"))
        self.assertEqual(self.pairs, ["BTC-USDT", "ETH-USDT"])                 # once, in the shared list
        self.assertIn("ETH-USDT", self.tracker.order_books)
        self.assertIn("ETH-USDT", self.tracker._tracking_tasks)
        self.assertEqual([m.update_id for m in saved], [100, 101])           # the snapshot held update 99
        self.assertTrue(await self.tracker.add_trading_pair("ETH-USDT"))      # already tracked
        self.assertEqual(self.ds.subscribed, ["ETH-USDT"])

    async def test_add_waits_for_the_boot_snapshots(self):
        self.tracker._order_books_initialized.clear()
        self.assertFalse(await self.tracker.add_trading_pair("ETH-USDT"))
        self.assertEqual(self.ds.subscribed, [])

    async def test_a_refused_subscription_adds_nothing(self):
        self.ds.subscribe_ok = False
        self.assertFalse(await self.tracker.add_trading_pair("ETH-USDT"))
        self.assertEqual(self.pairs, ["BTC-USDT"])
        self.assertNotIn("ETH-USDT", self.tracker.order_books)

    async def test_a_failed_snapshot_unsubscribes_and_cleans_up(self):
        self.ds.snapshot_fails = True
        self.assertFalse(await self.tracker.add_trading_pair("ETH-USDT"))
        self.assertEqual(self.ds.unsubscribed, ["ETH-USDT"])
        self.assertEqual(self.pairs, ["BTC-USDT"])
        self.assertNotIn("ETH-USDT", self.tracker._tracking_message_queues)

    async def test_remove_unsubscribes_and_drops_the_book(self):
        await self.tracker.add_trading_pair("ETH-USDT")
        task = self.tracker._tracking_tasks["ETH-USDT"]
        self.assertTrue(await self.tracker.remove_trading_pair("ETH-USDT"))
        await asyncio.sleep(0)
        self.assertTrue(task.cancelled())
        self.assertEqual(self.ds.unsubscribed, ["ETH-USDT"])
        self.assertEqual(self.pairs, ["BTC-USDT"])
        self.assertNotIn("ETH-USDT", self.tracker.order_books)
        self.assertFalse(await self.tracker.remove_trading_pair("ETH-USDT"))   # not tracked

    async def test_perpetual_trading_shares_the_list_and_drops_funding_info(self):
        perp = PerpetualTrading(self.pairs)
        perp.initialize_funding_info(FundingInfo("ETH-USDT", Decimal(1), Decimal(1), 0, Decimal(0)))
        perp.add_trading_pair("ETH-USDT")
        perp.add_trading_pair("ETH-USDT")
        self.assertEqual(self.pairs, ["BTC-USDT", "ETH-USDT"])
        perp.remove_trading_pair("ETH-USDT")
        self.assertEqual(self.pairs, ["BTC-USDT"])
        self.assertNotIn("ETH-USDT", perp._funding_info)
