import asyncio
import datetime as dt
import pytest
from app.scraper.render import RenderService

class DummyS3:
    def __init__(self):
        self.calls = []
    def put_object(self, **kwargs):
        self.calls.append(kwargs)


@pytest.mark.asyncio
async def test_save_snapshot_s3(monkeypatch):
    rs = RenderService()
    dummy = DummyS3()
    rs._s3 = dummy
    rs._s3_bucket = "bucket"
    rs._snapshot_ttl = 2
    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)
    monkeypatch.setattr(asyncio, "to_thread", fake_to_thread)
    await rs.save_snapshot("http://example.com", "<html></html>", b"img")
    assert len(dummy.calls) == 2
    exp = dummy.calls[0]["Expires"]
    assert exp - dt.datetime.utcnow() > dt.timedelta(days=1)
