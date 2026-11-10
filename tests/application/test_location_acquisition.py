"""Generated fixtures verify the acquisition boundary without user documents."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import unittest
import tempfile
from unittest import mock
from application.location_acquisition import LocationLimits, capture_location, read_root_registry
from application.web_acquisition import AcquisitionPolicy

def capture(spec, roots=None, **kwargs):
    return asyncio.run(capture_location(spec, roots, **kwargs))

def cases(name, values):
    def decorate(function):
        function.case_values = (name, values)
        return function
    return decorate


def expand_cases(cls):
    for name, function in list(vars(cls).items()):
        if not hasattr(function, 'case_values'):
            continue
        parameter, values = function.case_values
        delattr(cls, name)
        for index, value in enumerate(values):
            def run(self, function=function, value=value):
                function(self, value)
            run.__doc__ = f'{parameter}={value!r}'
            setattr(cls, f'{name}_{index}', run)
    return cls


@expand_cases
class LocationAcquisitionTests(unittest.TestCase):

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def test_inventory_hashes_all_bytes_and_escapes_canonical_paths(self):
        tmp_path = self.root
        (tmp_path / 'a b.txt').write_bytes(b'first\n')
        (tmp_path / 'nested').mkdir()
        (tmp_path / 'nested' / 'notes.md').write_bytes(b'# notes\n')
        roots = read_root_registry({'docs': str(tmp_path)})
        result = capture({'uri': 'location://docs/', 'recursive': True}, roots)
        assert result.complete and result.errors == ()
        assert [f.canonical_uri for f in result.files] == ['location://docs/a%20b.txt', 'location://docs/nested/notes.md']
        assert result.files[0].raw_bytes == b'first\n'
        assert result.files[0].raw_sha256 == 'sha256:' + hashlib.sha256(b'first\n').hexdigest()
        assert result.inventory_manifest['root_map_digest'] == roots.map_digest
        assert result.inventory_manifest['entries'][0]['raw_sha256'] == result.files[0].raw_sha256
        assert str(tmp_path) not in json.dumps(result.inventory_manifest)
        (tmp_path / 'a b.txt').write_bytes(b'second\n')
        again = capture('location://docs/a%20b.txt', roots)
        assert again.files[0].canonical_uri == result.files[0].canonical_uri
        assert again.files[0].raw_sha256 != result.files[0].raw_sha256

    def test_empty_directory_is_complete_without_fictitious_files(self):
        tmp_path = self.root
        result = capture('location://docs/', {'docs': str(tmp_path)})
        assert result.complete and result.files == () and (result.errors == ())
        assert result.inventory_manifest['entries'] == []

    @cases('uri', ['file:///etc/passwd', '/etc/passwd', 's3://bucket/file.pdf', 'location://unknown/', 'location://docs/../escape.txt', 'location://docs/%2e%2e/escape.txt', 'location://docs/%2fetc/passwd', 'location://docs/?x=1'])
    def test_unregistered_and_escaping_locators_fail_closed(self, uri):
        tmp_path = self.root
        result = capture(uri, {'docs': str(tmp_path)})
        assert not result.complete and result.files == () and result.errors

    def test_symlink_files_directories_and_roots_cannot_escape(self):
        tmp_path = self.root
        root = tmp_path / 'root'
        root.mkdir()
        outside = tmp_path / 'outside'
        outside.mkdir()
        (outside / 'secret.txt').write_text('secret')
        (root / 'link.txt').symlink_to(outside / 'secret.txt')
        (root / 'subdir').symlink_to(outside, target_is_directory=True)
        result = capture('location://docs/', {'docs': str(root)})
        assert not result.complete and result.files == ()
        with self.assertRaises(ValueError):
            read_root_registry({'docs': str(root / 'subdir')})

    def test_invalid_registry_is_rejected_and_identity_changes_with_root(self):
        tmp_path = self.root
        other = tmp_path / 'other'
        other.mkdir()
        first = read_root_registry({'docs': str(tmp_path)})
        assert first.map_digest != read_root_registry({'docs': str(other)}).map_digest
        for config in [{'bad/alias': str(tmp_path)}, {'docs': 'relative'}, {'docs': '/does/not/exist'}, []]:
            with self.assertRaises(ValueError):
                read_root_registry(config)

    @cases('bounds', [{'max_files': 1}, {'max_total_bytes': 5}, {'max_file_bytes': 2}, {'max_entries': 1}, {'max_depth': 0}, {'recursive': False}])
    def test_exhausted_budget_never_claims_complete_inventory(self, bounds):
        tmp_path = self.root
        (tmp_path / 'a.txt').write_text('alpha')
        (tmp_path / 'b.txt').write_text('bravo')
        (tmp_path / 'sub').mkdir()
        (tmp_path / 'sub' / 'c.txt').write_text('charlie')
        result = capture({'uri': 'location://docs/', **bounds}, {'docs': str(tmp_path)})
        assert not result.complete and result.errors
        assert result.inventory_manifest['complete'] is False

    def test_unsupported_files_are_explicitly_excluded_and_missing_is_unavailable(self):
        tmp_path = self.root
        (tmp_path / 'ignored.png').write_bytes(b'image')
        result = capture('location://docs/', {'docs': str(tmp_path)})
        assert result.complete and result.files == ()
        assert result.inventory_manifest['excluded'][0]['reason'] == 'unsupported_extension'
        missing = capture('location://docs/missing.txt', {'docs': str(tmp_path)})
        assert not missing.complete
        assert missing.errors[0]['observation'] == 'unavailable'
        assert missing.inventory_manifest['missing_means_deleted'] is False

    @cases('mutation', ['truncate', 'replace', 'rewrite'])
    def test_mutation_during_file_read_is_unavailable(self, mutation):
        tmp_path = self.root
        path = tmp_path / 'a.txt'
        path.write_text('original bytes')
        original_read = os.read
        mutated = False

        def read(fd, size):
            nonlocal mutated
            data = original_read(fd, size)
            if not mutated and data:
                mutated = True
                if mutation == 'truncate':
                    path.write_bytes(b'x')
                elif mutation == 'replace':
                    replacement = tmp_path / 'new'
                    replacement.write_text('replaced bytes')
                    replacement.replace(path)
                else:
                    path.write_text('different text')
            return data
        patcher = mock.patch.object(os, 'read', read)
        patcher.start()
        self.addCleanup(patcher.stop)
        result = capture('location://docs/a.txt', {'docs': str(tmp_path)})
        assert not result.complete and result.files == ()

    def test_root_replacement_is_not_read_under_old_identity(self):
        tmp_path = self.root
        path = tmp_path / 'root'
        path.mkdir()
        roots = read_root_registry({'docs': str(path)})
        path.rename(tmp_path / 'old')
        path.mkdir()
        (path / 'a.txt').write_text('new root')
        result = capture('location://docs/', roots)
        assert not result.complete and result.files == ()

    @cases('value', [0, -1, True, 1.2, '10'])
    def test_invalid_bounds_rejected_without_reading(self, value):
        tmp_path = self.root
        result = capture({'uri': 'location://docs/', 'max_files': value}, {'docs': str(tmp_path)})
        assert not result.complete and result.files == ()

    def test_caller_cannot_supply_private_network_authority(self):
        tmp_path = self.root
        result = capture({'uri': 'http://127.0.0.1/file.txt', 'allowed_hosts': ['127.0.0.1'], 'private_hosts': ['127.0.0.1']}, {})
        assert not result.complete and result.files == ()
        result = capture({'uri': 'http://127.0.0.1/file.txt', 'allowed_hosts': ['127.0.0.1']}, {})
        assert not result.complete and result.files == ()

    def test_http_fetches_single_document_through_real_asset_validation(self):
        from tests.test_web_asset_acquisition import Session, Response
        session = Session({'https://example.test/a.txt': Response(b'full bytes', 'text/plain')})
        result = capture({'uri': 'https://example.test/a.txt', 'allowed_hosts': ['example.test']}, {}, network_scope=AcquisitionPolicy(allowed_hosts=('example.test',)), session=session)
        assert result.complete and len(result.files) == 1
        assert result.files[0].raw_bytes == b'full bytes'
        assert result.files[0].canonical_uri == 'https://example.test/a.txt'

    def test_all_filesystem_work_runs_off_the_async_event_loop(self):
        import threading
        main_thread = threading.get_ident()
        observed_threads = []
        real_open = os.open
        def tracked_open(*args, **kwargs):
            observed_threads.append(threading.get_ident())
            return real_open(*args, **kwargs)
        with mock.patch.object(os, 'open', tracked_open):
            result = capture('location://docs/', {'docs': str(self.root)})
        self.assertTrue(result.complete)
        self.assertTrue(observed_threads)
        self.assertNotIn(main_thread, observed_threads)
