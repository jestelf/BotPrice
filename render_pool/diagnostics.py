import uuid
from datetime import datetime
from typing import Optional

import boto3


def save_error(url: str, html: str, screenshot: Optional[bytes], bucket: str, s3_client=None) -> str:
    """Сохраняет HTML и скриншот в S3, возвращая базовое имя файлов."""
    s3 = s3_client or boto3.client("s3")
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    base = f"{ts}_{uuid.uuid4().hex}"
    s3.put_object(Bucket=bucket, Key=f"{base}.html", Body=html.encode("utf-8"), ContentType="text/html")
    if screenshot:
        s3.put_object(
            Bucket=bucket, Key=f"{base}.png", Body=screenshot, ContentType="image/png"
        )
    return base
