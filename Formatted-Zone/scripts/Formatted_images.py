"""
-Arman Bazarchi-
Formatted_Images

Here we Move all images from Persistent Landing to Formatted Zone,
we ensure that all images have same format JPEG, and have a correct path.
connects to minIO, creates formatted-zone bucket, raises an error if pesistant_landing or temporal-zone does not exist.
reads images from persistent and converts to only JPEG and store in formatted-zone with same foldering structure.
it avoids storing duplicate data in formatted-zone, also removes any temporal file in local storage.

"""

from minio import Minio
from PIL import Image, UnidentifiedImageError
import io
import re
import os
from tqdm import tqdm

# ==============================
#          Functions
# ==============================

def get_minio_config():
    # Load MinIO configuration from environment variables (set by orchestrator).
    
    import os
    
    # Get configuration from environment variables (set by orchestrator)
    endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'admin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'admin123')
    
    print(f"Using MinIO configuration from environment variables: endpoint={endpoint}, access_key={access_key[:3]}***")
    return endpoint, access_key, secret_key

def setup_minio_client_and_buckets(minio, access_key, secret_key, landing_zone, persistent_prefix, formatted_zone):
    # Setup MinIO client and validate/create buckets.
    client = Minio(
        minio,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    
    # Break if temporal landing or temporal-zone does not exist
    if not client.bucket_exists(landing_zone):
        sys.exit(" ERROR: Root bucket 'Landing' does not exist in MinIO.")
    
    persistent_objects = list(client.list_objects(landing_zone, prefix=f"{persistent_prefix}/", recursive=False))
    if not persistent_objects:
        raise FileNotFoundError(" Required prefix '{persistent_prefix}/' not found inside '{landing_zone}' bucket.")
    
    # Ensure formatted-zone bucket exists
    if not client.bucket_exists(formatted_zone):
        client.make_bucket(formatted_zone)
        print(" Created formatted-zone bucket")
    else:
        print(" Formatted-zone bucket already exists")
    
    return client

def scan_existing_formatted_images(client, formatted_zone, formatted_prefix):
    # Scan existing formatted images.
    existing_formatted_uuids = set()
    for obj in client.list_objects(formatted_zone, prefix=formatted_prefix + "/", recursive=True):
        match = re.match(r".*/([a-f0-9\-]+)\.jpg", obj.object_name)
        if match:
            existing_formatted_uuids.add(match.group(1))
    
    print(f" Found {len(existing_formatted_uuids)} existing formatted images.")
    return existing_formatted_uuids

def process_single_image(client, landing_zone, persistent_prefix, formatted_prefix, formatted_zone, obj, existing_formatted_uuids):
    # Process a single image: download, convert to JPEG, and upload.
    try:
        # Extract UUID from filename (e.g., *.jpg, *.png, *.webp, etc.)
        match = re.match(rf"{persistent_prefix}/.+?/([a-f0-9\-]+)\.\w+$", obj.object_name)
        if not match:
            print(f" Skipping invalid filename: {obj.object_name}")
            return "skipped", None
        
        img_uuid = match.group(1)
        
        # Skip if already processed
        if img_uuid in existing_formatted_uuids:
            return "skipped", None
        
        # Download image
        data = client.get_object(landing_zone, obj.object_name)
        img_bytes = data.read()
        data.close()
        data.release_conn()
        
        # Convert to JPEG
        try:
            img = Image.open(io.BytesIO(img_bytes))
            buf = io.BytesIO()
            img.convert("RGB").save(buf, format="JPEG")
            img_bytes = buf.getvalue()
        except UnidentifiedImageError:
            print(f" Could not identify as image: {obj.object_name}")
            return "failed", None
        
        # Construct formatted path
        formatted_path = obj.object_name.replace(persistent_prefix, formatted_prefix)
        formatted_path = re.sub(r"\.\w+$", ".jpg", formatted_path)  # force .jpg extension
        
        # Upload formatted image
        client.put_object(
            formatted_zone,
            formatted_path,
            data=io.BytesIO(img_bytes),
            length=len(img_bytes),
            content_type="image/jpeg"
        )
        
        existing_formatted_uuids.add(img_uuid)
        print(f" Uploaded: {formatted_path}")
        return "processed", formatted_path
        
    except Exception as e:
        print(f" Error processing {obj.object_name}: {e}")
        return "failed", None

# ==============================
#        Configuration
# ==============================
def process_formatted_images(   
    minio = "localhost:9000",
    access_key = "admin",
    secret_key = "password123"):

    # Get MinIO configuration from environment variables (set by orchestrator)
    minio, access_key, secret_key = get_minio_config()

    landing_zone = "temporal-zone"
    persistent_prefix = "persistent_landing/images"
    formatted_prefix = "images"  # images folder in formatted-zone bucket
    formatted_zone = "formatted-zone"
    
    # Setup MinIO client and buckets
    client = setup_minio_client_and_buckets(minio, access_key, secret_key, landing_zone, persistent_prefix, formatted_zone)
    
    # Scan existing formatted images
    existing_formatted_uuids = scan_existing_formatted_images(client, formatted_zone, formatted_prefix)
    
    # List all files under Persistent Landing (any extension)
    persistent_files = list(client.list_objects(landing_zone, prefix=persistent_prefix, recursive=True))
    print(f" Found {len(persistent_files)} files in Persistent Landing.")
    
    processed = 0
    skipped = 0
    failed = 0
    
    for obj in tqdm(persistent_files, desc="Processing images"):
        result, _ = process_single_image(client, landing_zone, persistent_prefix, formatted_prefix, formatted_zone, obj, existing_formatted_uuids)
        
        if result == "processed":
            processed += 1
        elif result == "skipped":
            skipped += 1
        elif result == "failed":
            failed += 1
    
    # Summary
    print("\n All Persistent Landing images processed.")
    print(f" Processed successfully: {processed}")
    print(f" Skipped (existing/invalid): {skipped}")
    print(f" Failed conversions: {failed}")

process_formatted_images();