from __future__ import annotations

import time

import boto3
from botocore.stub import Stubber, ANY

from render_pool.cache import ListingTTLCache
from render_pool.context import create
from render_pool.diagnostics import save_error


def test_ttl_cache_expiration():
    cache = ListingTTLCache(ttl_min=1, ttl_max=1)
    cache.set("url", "data")
    assert cache.get("url") == "data"
    time.sleep(1.2)
    assert cache.get("url") is None


def test_context_sets_cookies():
    ctx = create("213")
    names = {c["name"] for c in ctx.cookies}
    assert "yandex_gid" in names
    assert "region" in names


def test_diagnostics_saves_to_s3():
    s3 = boto3.client("s3", region_name="us-east-1")
    stubber = Stubber(s3)
    bucket = "test-bucket"
    stubber.add_response(
        "put_object",
        {},
        {
            "Bucket": bucket,
            "Key": ANY,
            "Body": b"<html>",
            "ContentType": "text/html",
        },
    )
    stubber.add_response(
        "put_object",
        {},
        {
            "Bucket": bucket,
            "Key": ANY,
            "Body": b"img",
            "ContentType": "image/png",
        },
    )
    stubber.activate()
    save_error("http://example.com", "<html>", b"img", bucket, s3_client=s3)
    stubber.deactivate()
