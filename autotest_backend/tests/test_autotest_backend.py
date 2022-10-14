import pytest
import fakeredis
import autotest_backend


@pytest.fixture
def fake_redis_conn():
    yield fakeredis.FakeStrictRedis(decode_responses=True)


@pytest.fixture(autouse=True)
def fake_redis_db(monkeypatch, fake_redis_conn):
    monkeypatch.setattr(autotest_backend.redis.Redis, "from_url", lambda *a, **kw: fake_redis_conn)


def test_redis_connection(fake_redis_conn):
    assert autotest_backend.redis_connection() == fake_redis_conn
