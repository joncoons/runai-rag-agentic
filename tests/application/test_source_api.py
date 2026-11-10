"""Offline HTTP contracts for saved website controls and public policy boundaries."""
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4
import unittest

import httpx2
from fastapi import FastAPI
from application.source_api import install_source_routes
from collection_control.repository import Conflict


class SourceAPITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.source_id, self.run_id = uuid4(), uuid4()
        self.source = dict(source_id=self.source_id, collection_id='c', name='Docs',
            seed_urls=['https://docs.example/'], policy={'allowed_hosts': ['docs.example']},
            schedule={'enabled': False, 'timezone': 'America/Chicago', 'start': '02:00', 'end': '06:00'},
            paused=False, version=1, next_fire=None, last_successful_run_id=None)
        self.run = dict(run_id=self.run_id, source_id=self.source_id, state='queued', job_id=None,
            counts={'new': 0, 'changed': 0, 'unchanged': 0, 'missing': 0, 'failed': 0}, nominal_fire=None, error_code=None)
        self.store = SimpleNamespace(create=AsyncMock(return_value=self.source), list=AsyncMock(return_value=[self.source]),
            update=AsyncMock(return_value=self.source), request=AsyncMock(return_value=self.run), history=AsyncMock(return_value=[self.run]))
        self.connection = SimpleNamespace(fetchrow=AsyncMock(return_value={'deleted': False}))
        @asynccontextmanager
        async def transaction():
            yield
        self.connection.transaction = transaction
        @asynccontextmanager
        async def acquire():
            yield self.connection
        self.pool = SimpleNamespace(fetchrow=self.connection.fetchrow, acquire=acquire)
        self.registry_patch = patch('application.source_registry.SourceRegistry', return_value=self.store)
        self.factory = self.registry_patch.start(); self.addCleanup(self.registry_patch.stop)
        self.app = FastAPI(); self.app.state.context = SimpleNamespace(pool=self.pool)
        install_source_routes(self.app)
        self.client = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=self.app), base_url='http://fixture')
        self.addAsyncCleanup(self.client.aclose)
        self.base = '/api/collections/c/web-sources'
        self.body = {'name': 'Docs', 'seed_urls': ['https://docs.example/'], 'allowed_hosts': ['docs.example']}

    async def test_create_saves_public_policy_and_queues_initial_check(self):
        response = await self.client.post(self.base, json=self.body)
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()['source']['source_id'], str(self.source_id))
        self.assertEqual(response.json()['run']['state'], 'queued')
        kwargs = self.store.create.call_args.kwargs
        self.assertFalse(kwargs['schedule']['enabled'])
        self.assertNotIn('private_hosts', kwargs['policy'])
        self.assertNotIn('private_ports', kwargs['policy'])
        self.store.request.assert_awaited_once_with(self.source_id, 'c')
        self.assertIs(self.factory.call_args.args[0].connection, self.connection)

    async def test_strict_public_validation_blocks_before_mutation(self):
        for changes in ({'private_hosts': ['docs.example']}, {'private_ports': [9000]}, {'max_pages': True},
                {'seed_urls': ['https://user:password@docs.example/']}, {'seed_urls': ['http://127.0.0.1/']},
                {'seed_urls': ['https://other.example/']}, {'allowed_hosts': ['*.example']},
                {'schedule': {'enabled': 'true'}}, {'schedule': {'timezone': 'Not/A_Zone'}},
                {'schedule': {'start': '25:00'}}, {'schedule': {'end': '3:00'}}, {'schedule': {'start': '06:00', 'end': '06:00'}}, {'schedule': {'start': '23:00', 'end': '02:00'}},
                {'schedule': {'private_hosts': ['docs.example']}}, {'name': '  '}):
            with self.subTest(changes=changes):
                response = await self.client.post(self.base, json={**self.body, **changes})
                self.assertEqual(response.status_code, 422)
        self.store.create.assert_not_awaited(); self.store.request.assert_not_awaited()

    async def test_operator_scope_validates_seeds_but_never_persists_grants(self):
        body = {**self.body, 'seed_urls': ['http://127.0.0.1:9876/'], 'allowed_hosts': ['127.0.0.1']}
        with patch.dict('os.environ', {'WEB_ASSET_PRIVATE_HOSTS': '127.0.0.1,unused.internal', 'WEB_ASSET_PRIVATE_PORTS': '9876'}):
            response = await self.client.post(self.base, json=body)
        self.assertEqual(response.status_code, 202)
        policy = self.store.create.call_args.kwargs['policy']
        self.assertEqual(policy['allowed_hosts'], ['127.0.0.1'])
        self.assertNotIn('private_hosts', policy); self.assertNotIn('private_ports', policy)
        response = await self.client.post(self.base, json=body)
        self.assertEqual(response.status_code, 422)

    async def test_list_history_and_check_fence_by_collection(self):
        response = await self.client.get(self.base)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()['sources']), 1)
        self.store.list.assert_awaited_once_with('c')
        source = self.base + '/' + str(self.source_id)
        response = await self.client.get(source + '/runs')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['runs'][0]['run_id'], str(self.run_id))
        self.store.history.assert_awaited_once_with(self.source_id, 'c')
        response = await self.client.post(source + '/check')
        self.assertEqual(response.status_code, 202)
        self.store.request.assert_awaited_once_with(self.source_id, 'c')
        self.store.request.side_effect = Conflict('private credential detail')
        response = await self.client.post('/api/collections/other/web-sources/' + str(self.source_id) + '/check')
        self.assertEqual(response.status_code, 409)
        self.assertNotIn('credential', response.text)
        self.assertEqual(self.store.request.call_args.args, (self.source_id, 'other'))

    async def test_pause_and_schedule_updates_are_strict_and_explicit(self):
        source = self.base + '/' + str(self.source_id)
        response = await self.client.patch(source, json={'paused': True})
        self.assertEqual(response.status_code, 200)
        self.store.update.assert_awaited_with(self.source_id, 'c', schedule=None, paused=True)
        response = await self.client.patch(source, json={'schedule': {'enabled': True, 'timezone': 'UTC', 'start': '01:00', 'end': '02:00'}})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.store.update.call_args.kwargs['schedule']['enabled'])
        for body in ({}, {'paused': 'true'}, {'paused': None}, {'policy': {}}, {'schedule': None}, {'schedule': {'private_ports': [80]}}):
            self.assertEqual((await self.client.patch(source, json=body)).status_code, 422)

    async def test_missing_collection_and_invalid_source_id_never_mutate(self):
        self.pool.fetchrow.return_value = None
        for method, suffix, body in [('POST', '', self.body), ('GET', '', None), ('PATCH', '/' + str(self.source_id), {'paused': True}), ('POST', '/' + str(self.source_id) + '/check', None), ('GET', '/' + str(self.source_id) + '/runs', None)]:
            response = await self.client.request(method, self.base + suffix, json=body)
            self.assertEqual(response.status_code, 404)
        self.store.create.assert_not_awaited(); self.store.request.assert_not_awaited()
        self.assertEqual((await self.client.post(self.base + '/bad/check')).status_code, 422)

    async def test_private_internal_fields_and_raw_errors_are_not_exposed(self):
        self.source['policy'].update(private_hosts=['secret.internal'], private_ports=[9999])
        self.source['lease_token'] = 'secret'
        self.run.update(error_code='https://user:password@private.internal/error', lease_token='secret')
        response = await self.client.get(self.base)
        self.assertNotIn('secret', response.text); self.assertNotIn('private_ports', response.text)
        response = await self.client.get(self.base + '/' + str(self.source_id) + '/runs')
        self.assertNotIn('password', response.text); self.assertNotIn('lease_token', response.text)
        self.assertIsNone(response.json()['runs'][0]['error_code'])
