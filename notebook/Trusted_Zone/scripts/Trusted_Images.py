#!/usr/bin/env python
# coding: utf-8

# In[3]:


"""
-Arman Bazarchi-
Trusted Zone — Images notebook
-----------------------------------------------------------
This notebook moves cleaned and anonymized images from the
Formatted Zone to the Trusted Zone bucket in MinIO.
raises an error if no metadata is available in trusted-zone

Generic data-quality steps:
   Remove duplicates by UUID
   cross-checks with the available latest metadata to only store images that are recorded.
   Standardize format (JPEG) and color mode (RGB)
   Normalize resolution/aspect ratio (max 1024 px)
   Compress with uniform JPEG quality (85)
   Blur license plates for privacy
-----------------------------------------------------------
"""

from minio import Minio
from PIL import Image, ImageFilter
import io, os, re, numpy as np
from tqdm import tqdm
import pandas as pd

# Try to import cv2, fallback to None if not available
try:
    import cv2
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    cv2 = None

# -----------------------
# 1. Configuration
# -----------------------
def process_trusted_images(
    minio_endpoint = "localhost:9000",
    access_key = "admin",
    secret_key = "password123"):
    """Main function to process trusted images"""
    
    # Setup configuration
    config = _setup_trusted_images_config()
    
    # Connect to MinIO and setup buckets
    client = _connect_to_minio_trusted_images(minio_endpoint, access_key, secret_key)
    
    # Load trusted metadata
    valid_uuids = _load_trusted_metadata(client, config)
    
    # Get existing trusted images
    existing_trusted_uuids = _get_existing_trusted_images(client, config)
    
    # Initialize license plate detector
    plate_cascade = _initialize_license_plate_detector()
    
    # Process formatted images
    _process_formatted_images(client, valid_uuids, existing_trusted_uuids, plate_cascade, config)

def _setup_trusted_images_config():
    """Setup configuration for trusted images"""
    return {
        'FORMATTED_ZONE': "formatted-zone",
        'TRUSTED_ZONE': "trusted-zone",
        'IMG_PREFIX': "images/",
        'META_PREFIX': "metadata/"
    }

def _connect_to_minio_trusted_images(minio_endpoint, access_key, secret_key):
    """Connect to MinIO and setup buckets"""
    # Connect to MinIO
    client = Minio(minio_endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    if not client.bucket_exists("trusted-zone"):
        client.make_bucket("trusted-zone")
        print(" Created trusted zone bucket: trusted-zone")
    return client

def _load_trusted_metadata(client, config):
    """Load trusted metadata and return valid UUIDs"""
    # Load trusted metadata
    trusted_meta_files = [
        o.object_name for o in client.list_objects(config['TRUSTED_ZONE'], prefix=config['META_PREFIX'], recursive=True)
        if o.object_name.lower().endswith(".csv") and "trusted_metadata_" in o.object_name]

    if not trusted_meta_files:
        raise SystemExit(" No trusted metadata found.")

    trusted_meta_files.sort(reverse=True)
    latest_meta_file = trusted_meta_files[0]
    local_meta = "temp_trusted_metadata.csv"
    client.fget_object(config['TRUSTED_ZONE'], latest_meta_file, local_meta)
    trusted_df = pd.read_csv(local_meta)
    os.remove(local_meta)

    valid_uuids = set(trusted_df["uuid"].dropna())
    print(f" Found {len(valid_uuids)} valid UUIDs in trusted metadata.")
    return valid_uuids

def _get_existing_trusted_images(client, config):
    """Get existing trusted images to avoid duplicates"""
    existing_trusted_uuids = set()
    for o in client.list_objects(config['TRUSTED_ZONE'], prefix=config['IMG_PREFIX'], recursive=True):
        m = re.match(r".*/([a-f0-9\-]+)\.jpg$", o.object_name, re.IGNORECASE)
        if m:
            existing_trusted_uuids.add(m.group(1))
    print(f" Found {len(existing_trusted_uuids)} existing images in Trusted Zone.")
    return existing_trusted_uuids

def _initialize_license_plate_detector():
    """Initialize license plate detector"""
    # Only license plate detection, we cant apply bluring face because it can blur faces of species.
    if CV2_AVAILABLE and cv2 is not None:
        try:
            plate_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_russian_plate_number.xml")
            return plate_cascade
        except Exception:
            return None
    return None

def _process_formatted_images(client, valid_uuids, existing_trusted_uuids, plate_cascade, config):
    """Process formatted images and upload to trusted zone"""
    formatted_images = list(client.list_objects(config['FORMATTED_ZONE'], prefix=config['IMG_PREFIX'], recursive=True))
    print(f" Found {len(formatted_images)} images in Formatted Zone.")

    for obj in tqdm(formatted_images, desc="Processing images"):
        try:
            result = _process_single_image(client, obj, valid_uuids, existing_trusted_uuids, plate_cascade, config)
            if result:
                existing_trusted_uuids.add(result)
        except Exception as e:
            print(f" Error processing {obj.object_name}: {e}")
            continue

    print("✅ All trusted images processed and uploaded.")

def _process_single_image(client, obj, valid_uuids, existing_trusted_uuids, plate_cascade, config):
    """Process a single image"""
    m = re.match(r".*/([a-f0-9\-]+)\.\w+$", obj.object_name)
    if not m:
        return None
    img_uuid = m.group(1)
    if img_uuid not in valid_uuids or img_uuid in existing_trusted_uuids:
        return None

    # Download image
    data = client.get_object(config['FORMATTED_ZONE'], obj.object_name)
    img_bytes = data.read()
    data.close()
    data.release_conn()
    img = Image.open(io.BytesIO(img_bytes)).convert("RGB")

    # Process image
    processed_img = _process_image_quality(img, plate_cascade)
    
    # Upload processed image
    _upload_processed_image(client, obj, processed_img, config)
    
    return img_uuid

def _process_image_quality(img, plate_cascade):
    """Process image quality: normalize size, anonymize license plates, compress"""
    # Normalize size/aspect ratio (max 1024px)
    max_dim = 1024
    if max(img.size) > max_dim:
        img.thumbnail((max_dim, max_dim))

    # Anonymize license plates if cv2 is available
    if CV2_AVAILABLE and cv2 is not None and plate_cascade is not None:
        try:
            # Convert to OpenCV for license plate anonymization
            cv_img = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
            gray = cv2.cvtColor(cv_img, cv2.COLOR_BGR2GRAY)
            plates = plate_cascade.detectMultiScale(gray, 1.1, 3, minSize=(50, 15))

            for (x, y, w, h) in plates:
                roi = cv_img[y:y+h, x:x+w]
                cv_img[y:y+h, x:x+w] = cv2.GaussianBlur(roi, (51, 51), 30)

            # Back to PIL and compress as JPEG with quality of 85
            img = Image.fromarray(cv2.cvtColor(cv_img, cv2.COLOR_BGR2RGB))
        except Exception as e:
            print(f" Warning: License plate detection failed: {e}, skipping anonymization")

    # Compress as JPEG with quality of 85
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()

def _upload_processed_image(client, obj, processed_img_bytes, config):
    """Upload processed image to trusted zone"""
    # Upload
    trusted_path = obj.object_name.replace(config['IMG_PREFIX'], config['IMG_PREFIX'])
    client.put_object(
        config['TRUSTED_ZONE'],
        trusted_path,
        data=io.BytesIO(processed_img_bytes),
        length=len(processed_img_bytes),
        content_type="image/jpeg"
    )

if __name__ == "__main__":
    process_trusted_images()


# In[ ]:




