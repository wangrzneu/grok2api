"""
S3 upload service for generated images/videos.

Supports AWS S3, MinIO, Cloudflare R2, and other S3-compatible storage.
"""

import asyncio
from typing import Optional

from app.core.config import get_config
from app.core.logger import logger


def _is_s3_enabled() -> bool:
    if not get_config("s3.enabled"):
        return False
    try:
        import boto3  # noqa: F401
        return True
    except ImportError:
        logger.warning("S3 enabled in config but boto3 is not installed. Install with: pip install boto3")
        return False


def _get_s3_client():
    """Create a boto3 S3 client from config."""
    import boto3

    endpoint_url = get_config("s3.endpoint_url") or None
    access_key_id = get_config("s3.access_key_id") or None
    secret_access_key = get_config("s3.secret_access_key") or None
    region = get_config("s3.region") or None

    kwargs = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    if region:
        kwargs["region_name"] = region

    return boto3.client(
        "s3",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        **kwargs,
    )


def _build_s3_url(key: str) -> str:
    """Build the public URL for an S3 object."""
    custom_domain = get_config("s3.custom_domain")
    if custom_domain:
        return f"{custom_domain.rstrip('/')}/{key}"

    endpoint_url = get_config("s3.endpoint_url")
    bucket = get_config("s3.bucket")
    region = get_config("s3.region")

    if endpoint_url:
        return f"{endpoint_url.rstrip('/')}/{bucket}/{key}"

    if region:
        return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
    return f"https://{bucket}.s3.amazonaws.com/{key}"


def _get_content_type(ext: Optional[str]) -> str:
    """Map file extension to content type."""
    ext = (ext or "").lower()
    mapping = {
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "webp": "image/webp",
        "mp4": "video/mp4",
        "webm": "video/webm",
    }
    return mapping.get(ext, "application/octet-stream")


async def upload_to_s3(data: bytes, filename: str, ext: Optional[str] = None) -> Optional[str]:
    """
    Upload binary data to S3 and return the public URL.

    Args:
        data: The binary content to upload.
        filename: The object filename (without prefix).
        ext: File extension for content-type detection.

    Returns:
        The public URL string, or None if S3 is not enabled or upload fails.
    """
    if not _is_s3_enabled():
        return None

    bucket = get_config("s3.bucket")
    if not bucket:
        logger.warning("S3 enabled but bucket is not configured")
        return None

    prefix = get_config("s3.prefix") or ""
    if prefix:
        key = f"{prefix.strip('/')}/{filename}"
    else:
        key = filename

    content_type = _get_content_type(ext)

    def _do_upload():
        client = _get_s3_client()
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )

    try:
        await asyncio.to_thread(_do_upload)
        url = _build_s3_url(key)
        logger.info(f"S3 upload success: {key}")
        return url
    except Exception as e:
        logger.error(f"S3 upload failed for {key}: {e}")
        return None


__all__ = ["upload_to_s3"]
