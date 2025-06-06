import os
import shutil
import tempfile
import types
import pytest
import builtins
import sys

from http import HTTPStatus

from ros.processor.suggestions_engine import SuggestionsEngine

@pytest.fixture
def suggestions_engine(tmp_path, monkeypatch):
    # Patch consumer and producer to be simple objects with needed methods
    class DummyConsumer:
        def commit(self): pass
        def poll(self, timeout=None): return None
        def close(self): pass
    class DummyProducer:
        def produce(self, **kwargs): pass
        def poll(self): pass
    se = SuggestionsEngine()
    se.consumer = DummyConsumer()
    se.producer = DummyProducer()
    return se

def test_handle_create_event_happy_path(tmp_path, monkeypatch):
    # Setup payload
    payload = {
        'type': 'created',
        'platform_metadata': {
            'is_ros_v2': True,
            'is_pcp_raw_data_collected': True,
            'url': 'http://example.com/archive',
            'request_id': 'req123'
        },
        'host': {'id': 'host1', 'org_id': 'org1'}
    }

    # Patch download_and_extract to yield a dummy object with tmp_dir
    class DummyExtractDir:
        tmp_dir = str(tmp_path)
    monkeypatch.setattr(SuggestionsEngine, 'download_and_extract', lambda self, url, host, org_id: (d for d in [DummyExtractDir()]))

    # Patch find_root_directory to return a dummy extracted_dir_root
    monkeypatch.setattr(SuggestionsEngine, 'find_root_directory', lambda self, d, f: str(tmp_path))

    # Patch get_index_file_path to return a dummy index file path
    monkeypatch.setattr(SuggestionsEngine, 'get_index_file_path', lambda self, host, root: str(tmp_path / "dummy.index"))

    # Patch run_pcp_commands to record call
    called = {}
    def fake_run_pcp_commands(self, host, index_file_path, request_id, extracted_dir_root):
        called['run'] = (host, index_file_path, request_id, extracted_dir_root)
    monkeypatch.setattr(SuggestionsEngine, 'run_pcp_commands', fake_run_pcp_commands)

    # Patch run_rules to return a dict with report_metadata and report keys
    monkeypatch.setattr('ros.processor.suggestions_engine.run_rules', lambda root: {'report_metadata': 'meta', 'report': 'rep'})
    # Patch report_metadata and report to be keys
    monkeypatch.setattr('ros.processor.suggestions_engine.report_metadata', 'report_metadata')
    monkeypatch.setattr('ros.processor.suggestions_engine.report', 'report')

    # Patch print to capture output
    printed = []
    monkeypatch.setattr(builtins, 'print', lambda x: printed.append(x))

    se = suggestions_engine
    se.handle_create_update(payload)
    assert called['run'][1].endswith("dummy.index")
    assert 'meta' in printed
    assert 'rep' in printed

def test_create_and_cleanup_output_dir(tmp_path, monkeypatch):
    se = SuggestionsEngine()
    # Patch os.makedirs to actually create in tmp_path
    output_dir = tmp_path / "pmlogextract-output-reqid"
    monkeypatch.setattr(os, 'makedirs', lambda path: os.mkdir(path) if not os.path.exists(path) else None)
    monkeypatch.setattr(os, 'path', os.path)
    # Patch logging.debug to a no-op
    monkeypatch.setattr('ros.processor.suggestions_engine.logging', types.SimpleNamespace(debug=lambda *a, **k: None, info=lambda *a, **k: None, error=lambda *a, **k: None, warning=lambda *a, **k: None))
    # Test create_output_dir
    dir_path = se.create_output_dir('reqid', {'id': 'host1'})
    assert os.path.exists(dir_path)
    # Now test cleanup in run_pcp_commands
    monkeypatch.setattr(SuggestionsEngine, 'run_pmlogextract', lambda *a, **k: None)
    monkeypatch.setattr(SuggestionsEngine, 'run_pmlogsummary', lambda *a, **k: None)
    se.run_pcp_commands({'id': 'host1'}, 'index', 'reqid', str(tmp_path))
    assert not os.path.exists(dir_path)

def test_run_pmlogextract_retries_on_timeout(tmp_path, monkeypatch):
    se = SuggestionsEngine()
    # Patch subprocess.run to raise TimeoutExpired twice, then succeed
    call_count = {'count': 0}
    def fake_run(cmd, check, timeout):
        if call_count['count'] < 2:
            call_count['count'] += 1
            raise subprocess.TimeoutExpired(cmd, timeout)
        call_count['count'] += 1
        return 0
    monkeypatch.setattr('subprocess.run', fake_run)
    # Patch logging to no-op
    monkeypatch.setattr('ros.processor.suggestions_engine.logging', types.SimpleNamespace(debug=lambda *a, **k: None, info=lambda *a, **k: None, error=lambda *a, **k: None, warning=lambda *a, **k: None))
    se.event = "Create event"
    se.service = "SUGGESTIONS_ENGINE"
    se.run_pmlogextract({'id': 'host1'}, 'index', str(tmp_path))
    assert call_count['count'] == 3

def test_handle_create_update_missing_fields(suggestions_engine, monkeypatch):
    # Patch logging.info to capture calls
    infos = []
    monkeypatch.setattr('ros.processor.suggestions_engine.logging', types.SimpleNamespace(debug=lambda *a, **k: None, info=infos.append, error=lambda *a, **k: None, warning=lambda *a, **k: None))
    # Missing host
    payload = {'type': 'created', 'platform_metadata': {'is_ros_v2': True}}
    suggestions_engine.handle_create_update(payload)
    # Missing platform_metadata
    payload = {'type': 'created', 'host': {'id': 'host1'}}
    suggestions_engine.handle_create_update(payload)
    assert any("Missing host or/and platform_metadata" in str(msg) for msg in infos)

def test_download_and_extract_http_error(suggestions_engine, monkeypatch):
    # Patch requests.get to return a dummy response with non-OK status
    class DummyResponse:
        status_code = HTTPStatus.BAD_REQUEST
        reason = "Bad Request"
        content = b''
    monkeypatch.setattr('requests.get', lambda url, timeout: DummyResponse())
    # Patch consumer.commit to record call
    committed = []
    suggestions_engine.consumer.commit = lambda: committed.append(True)
    # Patch logging.error to record calls
    errors = []
    monkeypatch.setattr('ros.processor.suggestions_engine.logging', types.SimpleNamespace(debug=lambda *a, **k: None, info=lambda *a, **k: None, error=errors.append, warning=lambda *a, **k: None))
    # Use context manager
    with suggestions_engine.download_and_extract('http://example.com', {'id': 'host1'}, 'org1') as result:
        assert result is None
    assert committed
    assert any("Unable to download the report" in str(msg) for msg in errors)

def test_run_pmlogextract_calledprocesserror_handling(tmp_path, monkeypatch):
    se = SuggestionsEngine()
    # Patch subprocess.run to raise CalledProcessError
    class DummyError(Exception): pass
    def fake_run(cmd, check, timeout):
        raise subprocess.CalledProcessError(1, cmd, output="fail")
    monkeypatch.setattr('subprocess.run', fake_run)
    # Patch logging.error to record calls
    errors = []
    monkeypatch.setattr('ros.processor.suggestions_engine.logging', types.SimpleNamespace(debug=lambda *a, **k: None, info=lambda *a, **k: None, error=errors.append, warning=lambda *a, **k: None))
    se.event = "Create event"
    se.service = "SUGGESTIONS_ENGINE"
    with pytest.raises(subprocess.CalledProcessError):
        se.run_pmlogextract({'id': 'host1'}, 'index', str(tmp_path))
    assert any("Error running pmlogextract command" in str(msg) for msg in errors)