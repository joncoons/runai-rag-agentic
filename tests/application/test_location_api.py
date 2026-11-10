"""Offline HTTP contracts for actual app location route installation.

No lifespan services, filesystem roots, credentials, models or databases run.
The transaction fixture records API ownership of the create/request boundary.
"""
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4
import unittest

import httpx2

from application.api import create_app
from collection_control.repository import Conflict


class LocationAPITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.source_id, self.run_id = uuid4(), uuid4()
        self.events = []
        self.schedule = {'enabled': False, 'timezone': 'America/Chicago', 'start': '02:00', 'end': '06:00'}
        self.source = {'source_id': self.source_id, 'collection_id': 'c', 'name': 'Reference',
                       'seed_urls': [], 'policy': {}, 'kind': 'location', 'location': {},
                       'schedule': self.schedule, 'captions_enabled': False,
                       'paused': False, 'version': 1, 'next_fire': None,
                       'last_successful_run_id': None}
        self.run = {'run_id': self.run_id, 'source_id': self.source_id, 'state': 'queued',
                    'job_id': None, 'counts': {}, 'nominal_fire': None, 'error_code': None}
        self.connection = SimpleNamespace(fetchrow=AsyncMock(return_value={'deleted': False}), in_transaction=False)

        @asynccontextmanager
        async def transaction():
            self.assertFalse(self.connection.in_transaction)
            self.connection.in_transaction = True
            self.events.append('begin')
            try:
                yield
            except BaseException:
                self.events.append('rollback')
                raise
            else:
                self.events.append('commit')
            finally:
                self.connection.in_transaction = False

        @asynccontextmanager
        async def acquire():
            yield self.connection

        async def create_location(collection_id, **selection):
            self.assertTrue(self.connection.in_transaction)
            self.events.append('create')
            self.source.update(collection_id=collection_id, **selection)
            return self.source

        async def request(source_id, collection_id):
            self.assertTrue(self.connection.in_transaction)
            self.events.append('request')
            self.assertEqual((source_id, collection_id), (self.source_id, 'c'))
            return self.run

        self.connection.transaction = transaction
        self.pool = SimpleNamespace(fetchrow=self.connection.fetchrow, acquire=acquire)
        self.store = SimpleNamespace(create_location=AsyncMock(side_effect=create_location),
                                     request=AsyncMock(side_effect=request), update=AsyncMock(return_value=self.source))
        self.registry_patch = patch('application.source_registry.SourceRegistry', return_value=self.store)
        self.factory = self.registry_patch.start()
        self.addCleanup(self.registry_patch.stop)
        roots_patch = patch('application.location_api._roots', return_value={
            'docs': '/operator-private/approved-read-only', 'archive': '/operator-private/archive-read-only'})
        roots_patch.start()
        self.addCleanup(roots_patch.stop)
        environment = patch.dict('os.environ', {'WEB_ASSET_PRIVATE_HOSTS': '', 'WEB_ASSET_PRIVATE_PORTS': ''})
        environment.start()
        self.addCleanup(environment.stop)
        # Registry must be patched before create_app: route installers capture it.
        self.app = create_app(context=SimpleNamespace(pool=self.pool))
        self.client = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=self.app), base_url='http://fixture')
        self.addAsyncCleanup(self.client.aclose)
        self.base = '/api/collections/c/location-sources'
        self.body = {'name': 'Reference', 'uri': 'location://docs/reference'}

    async def test_actual_app_lists_only_sorted_registered_aliases(self):
        response = await self.client.get('/api/location-roots')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'roots': [
            {'alias': 'archive', 'uri': 'location://archive/'},
            {'alias': 'docs', 'uri': 'location://docs/'}]})
        self.assertNotIn('/operator-private', response.text)
        self.factory.assert_not_called()

    async def test_registered_source_and_initial_run_share_pinned_transaction(self):
        response = await self.client.post(self.base, json=self.body)
        self.assertEqual(response.status_code, 202)
        data = response.json()
        self.assertEqual(data['source']['source_id'], str(self.source_id))
        self.assertEqual(data['run']['run_id'], str(self.run_id))
        self.assertEqual(data['run']['state'], 'queued')
        self.assertEqual(data['source']['kind'], 'location')
        self.assertEqual(data['source']['location'], {
            'uri': 'location://docs/reference', 'recursive': True, 'max_depth': 8, 'max_files': 128})
        self.assertEqual(data['source']['schedule'], self.schedule)
        self.assertIs(data['source']['captions_enabled'], False)
        self.assertEqual(self.events, ['begin', 'create', 'request', 'commit'])
        self.assertIs(self.factory.call_args.args[0].connection, self.connection)
        self.assertNotIn('/operator-private', response.text)

    async def test_public_caption_schedule_and_directory_choices_are_preserved(self):
        schedule = {'enabled': True, 'timezone': 'Europe/London', 'start': '09:00', 'end': '11:00'}
        response = await self.client.post(self.base, json={**self.body,
            'uri': 'location://archive/notes%20and%20tables', 'recursive': False,
            'max_depth': 0, 'max_files': 12, 'captions_enabled': True, 'schedule': schedule})
        self.assertEqual(response.status_code, 202)
        source = response.json()['source']
        self.assertEqual(source['schedule'], schedule)
        self.assertIs(source['captions_enabled'], True)
        self.assertEqual(source['location'], {'uri': 'location://archive/notes%20and%20tables',
            'recursive': False, 'max_depth': 0, 'max_files': 12})
        self.assertEqual(self.events, ['begin', 'create', 'request', 'commit'])

    async def test_unsupported_unregistered_traversal_and_private_uris_never_create(self):
        for uri in ('file:///etc/passwd', '/etc/passwd', 's3://bucket/a.pdf',
                    'location://unknown/', 'location://docs/../escape.txt',
                    'location://docs/%2e%2e/escape.txt', 'location://docs/%2Fetc/passwd',
                    'location://docs/?scope=all', 'location://user:password@docs/',
                    'http://127.0.0.1/a.pdf', 'http://localhost/a.pdf',
                    'https://user:password@docs.example/a.pdf', 'https://docs.example:9876/a.pdf'):
            with self.subTest(uri=uri):
                response = await self.client.post(self.base, json={**self.body, 'uri': uri})
                self.assertEqual(response.status_code, 422)
                self.assertNotIn('password', response.text)
        self.store.create_location.assert_not_awaited()
        self.store.request.assert_not_awaited()
        self.factory.assert_not_called()
        self.assertEqual(self.events, [])

    async def test_strict_body_rejects_private_grants_boolean_bounds_and_schedule_coercion(self):
        for change in ({'private_hosts': ['127.0.0.1']}, {'private_ports': [9876]},
                       {'root_path': '/etc'}, {'location': {'uri': 'file:///etc/passwd'}},
                       {'max_depth': True}, {'max_files': True}, {'max_files': '12'},
                       {'max_depth': -1}, {'max_depth': 17}, {'max_depth': 33},
                       {'max_files': 0}, {'max_files': 513}, {'recursive': 'true'},
                       {'captions_enabled': 'true'}, {'schedule': {'enabled': 'true'}},
                       {'schedule': {'timezone': 'Not/A_Zone'}},
                       {'schedule': {'start': '23:00', 'end': '02:00'}},
                       {'schedule': {'private_hosts': ['127.0.0.1']}}):
            with self.subTest(change=change):
                self.assertEqual((await self.client.post(self.base, json={**self.body, **change})).status_code, 422)
        self.store.create_location.assert_not_awaited()
        self.store.request.assert_not_awaited()
        self.factory.assert_not_called()
        self.assertEqual(self.events, [])

    async def test_public_http_source_saves_single_file_scope_without_directory_fields(self):
        response = await self.client.post(self.base, json={**self.body,
            'uri': 'https://documents.example/a.pdf', 'allowed_hosts': ['documents.example'],
            'recursive': True, 'max_depth': 16, 'max_files': 5})
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()['source']['location'], {
            'uri': 'https://documents.example/a.pdf', 'max_files': 5, 'allowed_hosts': ['documents.example']})
        self.assertEqual(self.events, ['begin', 'create', 'request', 'commit'])

    async def test_only_operator_environment_can_authorize_private_http_and_port(self):
        body = {**self.body, 'uri': 'http://127.0.0.1:9876/a.pdf', 'allowed_hosts': ['127.0.0.1']}
        with patch.dict('os.environ', {'WEB_ASSET_PRIVATE_HOSTS': '127.0.0.1,unused.internal',
                                       'WEB_ASSET_PRIVATE_PORTS': '9876'}):
            response = await self.client.post(self.base, json=body)
        self.assertEqual(response.status_code, 202)
        spec = response.json()['source']['location']
        self.assertEqual(spec, {'uri': body['uri'], 'max_files': 128, 'allowed_hosts': ['127.0.0.1']})
        self.assertNotIn('private_hosts', response.text)
        self.assertNotIn('private_ports', response.text)
        self.assertNotIn('recursive', spec)
        self.assertNotIn('max_depth', spec)
        self.store.create_location.reset_mock()
        self.store.request.reset_mock()
        self.assertEqual((await self.client.post(self.base, json=body)).status_code, 422)
        self.store.create_location.assert_not_awaited()
        self.store.request.assert_not_awaited()

    async def test_missing_or_deleted_collection_never_constructs_registry(self):
        for row in (None, {'deleted': True}):
            with self.subTest(row=row):
                self.pool.fetchrow.return_value = row
                response = await self.client.post(self.base, json=self.body)
                self.assertEqual(response.status_code, 404)
        self.factory.assert_not_called()
        self.store.create_location.assert_not_awaited()
        self.store.request.assert_not_awaited()
        self.assertEqual(self.events, [])

    async def test_initial_request_conflict_rolls_back_source_transaction_and_sanitizes_error(self):
        async def conflict(*args):
            self.assertTrue(self.connection.in_transaction)
            self.events.append('request')
            raise Conflict('password and private credential detail')
        self.store.request.side_effect = conflict
        response = await self.client.post(self.base, json=self.body)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.events, ['begin', 'create', 'request', 'rollback'])
        self.assertNotIn('password', response.text)
        self.assertNotIn('credential', response.text)
        self.assertIs(self.factory.call_args.args[0].connection, self.connection)
