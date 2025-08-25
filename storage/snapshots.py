from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

import boto3

from app.config import settings


def _client():
    return boto3.client(
        "s3",
        endpoint_url=getattr(settings, "S3_ENDPOINT", None),
        aws_access_key_id=getattr(settings, "S3_ACCESS_KEY", None),
        aws_secret_access_key=getattr(settings, "S3_SECRET_KEY", None),
    )


def save_snapshot(
    html: str,
    screenshot: Optional[bytes] = None,
    *,
    bucket: Optional[str] = None,
    s3_client=None,
) -> str:
    """Сохраняет HTML и скриншот в S3/MinIO.

    Возвращает базовое имя сохранённых файлов.
    """
    bucket = bucket or settings.S3_BUCKET
    if not bucket:
        raise RuntimeError("S3 bucket not configured")
    s3 = s3_client or _client()
    base = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}"
    s3.put_object(Bucket=bucket, Key=f"{base}.html", Body=html.encode("utf-8"), ContentType="text/html")
    if screenshot:
        s3.put_object(
            Bucket=bucket, Key=f"{base}.png", Body=screenshot, ContentType="image/png"
        )
    return base
