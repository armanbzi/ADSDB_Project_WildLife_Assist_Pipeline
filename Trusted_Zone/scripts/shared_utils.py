"""
-Arman Bazarchi-
Shared utilities for Trusted Zone scripts
=========================================
Common functions used across Trusted Zone processing scripts.
"""

from minio import Minio
import os


def get_minio_config():
    """Load MinIO configuration from environment variables (set by orchestrator)."""
    # Get configuration from environment variables (set by orchestrator)
    endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'admin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'admin123')
    
    print(f"Using MinIO configuration from environment variables: endpoint={endpoint}, access_key={access_key[:3]}***")
    return endpoint, access_key, secret_key


def setup_minio_client_and_bucket(minio, access_key, secret_key, trusted_zone):
    """Setup MinIO client and create bucket if it doesn't exist."""
    client = Minio(minio, access_key=access_key, secret_key=secret_key, secure=False)
    if not client.bucket_exists(trusted_zone):
        client.make_bucket(trusted_zone)
        print(f" Created trusted zone bucket: {trusted_zone}")
    return client


def get_trusted_zone_config():
    """Get common configuration constants for Trusted Zone."""
    return {
        'formatted_zone': 'formatted-zone',
        'trusted_zone': 'trusted-zone',
        'meta_prefix': 'metadata/',
        'img_prefix': 'images/'
    }
