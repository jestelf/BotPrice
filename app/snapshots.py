import argparse
import os
import boto3
from .config import settings


def _client():
    return boto3.client(
        "s3",
        endpoint_url=getattr(settings, "S3_ENDPOINT", None),
        aws_access_key_id=getattr(settings, "S3_ACCESS_KEY", None),
        aws_secret_access_key=getattr(settings, "S3_SECRET_KEY", None),
    )


def download_keys(keys: list[str], dest: str) -> None:
    s3 = _client()
    bucket = settings.S3_BUCKET
    if not bucket:
        raise SystemExit("S3 bucket not configured")
    os.makedirs(dest, exist_ok=True)
    for key in keys:
        target = os.path.join(dest, key.split("/")[-1])
        s3.download_file(bucket, key, target)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download snapshots from S3/MinIO")
    parser.add_argument("keys", nargs="*", help="S3 object keys to download")
    parser.add_argument("--prefix", dest="prefix", help="Prefix to fetch objects")
    parser.add_argument("--dest", dest="dest", default="snapshots", help="Destination directory")
    args = parser.parse_args()

    keys = list(args.keys)
    if args.prefix:
        s3 = _client()
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=settings.S3_BUCKET, Prefix=args.prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
    if not keys:
        raise SystemExit("Nothing to download")
    download_keys(keys, args.dest)


if __name__ == "__main__":
    main()
