"""boto3 S3 adapter, with the spec's bucket posture."""

from __future__ import annotations

from typing import Any

from ..constants import PRESIGNED_URL_TTL_S
from ..errors import ObjectNotFound


class S3ObjectStore:
    def __init__(self, client: Any, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    def put(self, key: str, data: bytes, content_type: str) -> None:
        self._client.put_object(
            Bucket=self._bucket, Key=key, Body=data, ContentType=content_type
        )

    def get(self, key: str) -> bytes:
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=key)
        except self._client.exceptions.NoSuchKey as exc:
            raise ObjectNotFound(key) from exc
        body: bytes = resp["Body"].read()
        return body

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            keys.extend(o["Key"] for o in page.get("Contents", []))
        return keys

    def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=key)

    def presign(self, key: str) -> str:
        url: str = self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._bucket, "Key": key},
            ExpiresIn=PRESIGNED_URL_TTL_S,
        )
        return url

    def delete_prefix(self, prefix: str) -> int:
        count = 0
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            contents = page.get("Contents", [])
            if not contents:
                continue
            self._client.delete_objects(
                Bucket=self._bucket,
                Delete={"Objects": [{"Key": o["Key"]} for o in contents]},
            )
            count += len(contents)
        return count


def ensure_bucket_posture(client: Any, bucket: str) -> None:
    """Public access blocked, versioning on, lifecycle rules (old spec §6.4)."""
    client.put_public_access_block(
        Bucket=bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    client.put_bucket_versioning(
        Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
    )
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "expire-noncurrent-versions",
                    "Status": "Enabled",
                    "Filter": {},
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
                },
                {
                    "ID": "abort-incomplete-multipart",
                    "Status": "Enabled",
                    "Filter": {},
                    "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
                },
            ]
        },
    )
