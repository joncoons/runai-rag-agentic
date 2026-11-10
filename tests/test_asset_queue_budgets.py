"""Offline asset queue budgets, concurrency and untrusted capture-cache tests."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace
import unittest

from application import web_asset_acquisition as assets
from application.web_acquisition import AcquisitionPolicy, Asset


class Session:
    def __init__(self, *, fail=None, gate=None, raw=b'%PDF-1.7'):
        self.calls = []
        self.active = self.peak = self.cancelled = 0
        self.fail, self.gate, self.raw = fail, gate, raw

    def get(self, url, **kwargs):
        owner = self

        class Response:
            status = 200
            headers = {'Content-Type': 'application/pdf'}

            async def __aenter__(self):
                owner.calls.append(url)
                owner.active += 1
                owner.peak = max(owner.peak, owner.active)
                return self

            async def __aexit__(self, *args):
                owner.active -= 1

            @property
            def content(self):
                return self

            async def iter_chunked(self, size):
                try:
                    await asyncio.sleep(0)
                    if url == owner.fail:
                        raise assets.AssetAcquisitionError('specific fixture failure')
                    if owner.gate is not None:
                        await owner.gate.wait()
                    else:
                        await asyncio.sleep(.001)
                    yield owner.raw
                except asyncio.CancelledError:
                    owner.cancelled += 1
                    raise

        return Response()


class Store:
    def __init__(self, captures=None):
        self.captures = captures or {}
        self.gets, self.puts = [], []

    async def get_asset(self, url):
        self.gets.append(url)
        return self.captures.get(url)

    async def put_asset(self, url, capture):
        self.puts.append(url)
        self.captures[url] = capture


def relation(number, parent='page', locator=None):
    return Asset(f'https://docs.example/{number}.pdf', 'binary', f'https://docs.example/{parent}',
                 f'alt {parent}', locator or f'a[{number}]')


def capture(*relations):
    return SimpleNamespace(pages=(SimpleNamespace(assets=relations),))


class AssetQueueBudgetTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.policy = AcquisitionPolicy(('docs.example',))

    async def test_default_accepts_more_than_100_assets_with_four_fetch_workers(self):
        session = Session()
        results = await assets.acquire_assets(capture(*(relation(i) for i in range(111))), self.policy, session=session)
        self.assertEqual(len(results), 111)
        self.assertEqual([r.requested_uri for r in results], [relation(i).url for i in range(111)])
        self.assertEqual(len(session.calls), 111)
        self.assertEqual(session.peak, 4)
        self.assertEqual(session.active, 0)

    async def test_dedup_retains_order_and_each_parent_locator_and_alt(self):
        session, store = Session(), Store()
        relations = (relation(1), relation(2), relation(1, 'other', 'img[9]'))
        results = await assets.acquire_assets(capture(*relations), self.policy, session=session, capture_store=store)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(store.gets, [relation(1).url, relation(2).url])
        self.assertEqual(store.puts, [relation(1).url, relation(2).url])
        self.assertEqual([(r.parent_url, r.locator, r.alt) for r in results],
                         [(r.parent_url, r.locator, r.alt) for r in relations])
        again = await assets.acquire_assets(capture(*reversed(relations)), self.policy, session=session, capture_store=store)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual([r.parent_url for r in again], [r.parent_url for r in reversed(relations)])

    async def test_explicit_unique_count_and_byte_budgets_fail_without_partial_results(self):
        for kwargs in ({'max_assets': 1}, {'max_aggregate_bytes': 15}):
            with self.subTest(kwargs=kwargs), self.assertRaises(assets.AssetAcquisitionError):
                await assets.acquire_assets(capture(relation(1), relation(2)), self.policy, session=Session(), **kwargs)
        for key in ('max_assets', 'max_aggregate_bytes'):
            for value in (0, -1, True, 1.5):
                with self.subTest(key=key, value=value), self.assertRaises(assets.AssetAcquisitionError):
                    await assets.acquire_assets(capture(), self.policy, session=Session(), **{key: value})
        results = await assets.acquire_assets(capture(relation(1), relation(1, 'other')), self.policy,
                                              session=Session(), max_assets=1, max_aggregate_bytes=8)
        self.assertEqual(len(results), 2)

    async def test_failure_cancels_and_awaits_inflight_requests_and_preserves_error(self):
        session = Session(fail=relation(1).url, gate=asyncio.Event())
        with self.assertRaisesRegex(assets.AssetAcquisitionError, 'specific fixture failure'):
            await asyncio.wait_for(assets.acquire_assets(capture(*(relation(i) for i in range(12))), self.policy,
                                                         session=session), 1)
        self.assertEqual(session.active, 0)
        self.assertEqual(session.cancelled, 3)
        self.assertEqual(len(session.calls), 4)

    async def test_cached_body_metadata_scope_hash_and_response_bound_are_revalidated(self):
        original = await assets.acquire_asset(relation(1), self.policy, session=Session())
        invalid = [replace(original, final_uri='http://127.0.0.1/1.pdf'),
                   replace(original, requested_uri=relation(2).url),
                   replace(original, parent_url='https://forbidden.example/page'),
                   replace(original, media_type='text/html'), replace(original, filename='evil.exe'),
                   replace(original, raw_bytes=b'<html/>'), replace(original, raw_sha256='sha256:bad'),
                   replace(original, evidence={**original.evidence, 'byte_size': 999}),
                   replace(original, evidence={**original.evidence, 'redirect_chain': [{'uri': 'https://forbidden.example/x', 'status': 200}]}),
                   replace(original, evidence={**original.evidence, 'http_status': 404})]
        for cached in invalid:
            session = Session()
            with self.subTest(cached=cached), self.assertRaises(assets.AssetAcquisitionError):
                await assets.acquire_assets(capture(relation(1)), self.policy, session=session,
                                            capture_store=Store({relation(1).url: cached}))
            self.assertEqual(session.calls, [])
        with self.assertRaises(assets.AssetAcquisitionError):
            await assets.acquire_assets(capture(relation(1)), replace(self.policy, max_response_bytes=7),
                                        session=Session(), capture_store=Store({relation(1).url: original}))

    async def test_deduplicated_parent_authorization_cannot_be_bypassed(self):
        invalid = replace(relation(1), parent_url='https://forbidden.example/page')
        with self.assertRaises(assets.AssetAcquisitionError):
            await assets.acquire_assets(capture(relation(1), invalid), self.policy, session=Session())

    async def test_caller_cancellation_drains_all_workers(self):
        session = Session(gate=asyncio.Event())
        task = asyncio.create_task(assets.acquire_assets(capture(*(relation(i) for i in range(12))), self.policy,
                                                        session=session))
        for _ in range(100):
            if session.active == 4:
                break
            await asyncio.sleep(.001)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(session.active, 0)
        self.assertEqual(session.cancelled, 4)

    async def test_default_total_bytes_can_exceed_64_mib(self):
        session = Session(raw=b'%PDF-1.7' + b'x' * (1024 * 1024))
        results = await assets.acquire_assets(capture(*(relation(i) for i in range(65))), self.policy,
                                              session=session)
        self.assertEqual(len(results), 65)
        self.assertGreater(sum(len(r.raw_bytes) for r in results), 64 * 1024 * 1024)

    async def test_current_format_selection_rechecked_on_cached_relationship(self):
        original = await assets.acquire_asset(relation(1), self.policy, session=Session())
        wrong_kind = replace(relation(1), kind='image')
        with self.assertRaises(assets.AssetAcquisitionError):
            await assets.acquire_assets(capture(wrong_kind), self.policy, session=Session(),
                                        capture_store=Store({relation(1).url: original}))
