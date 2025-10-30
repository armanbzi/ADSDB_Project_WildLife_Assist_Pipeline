"""
-Arman Bazarchi-
Trusted Zone — Images notebook
-----------------------------------------------------------
Here we move cleaned and anonymized images from the
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
from PIL import Image
import io, os, re, cv2, numpy as np
from tqdm import tqdm
import pandas as pd
from shared_utils import get_minio_config, setup_minio_client_and_bucket, get_trusted_zone_config

# -----------------------
#     Functions
# -----------------------

# -----------------------
#     Helper Functions
# -----------------------


def _load_trusted_metadata(client, trusted_zone, meta_prefix):
    """Load trusted metadata and extract valid UUIDs."""
    trusted_meta_files = [
        o.object_name for o in client.list_objects(trusted_zone, prefix=meta_prefix, recursive=True)
        if o.object_name.lower().endswith(".csv") and "trusted_metadata_" in o.object_name]
    
    if not trusted_meta_files:
        raise SystemExit(" No trusted metadata found.")
    
    trusted_meta_files.sort(reverse=True)
    latest_meta_file = trusted_meta_files[0]
    local_meta = "temp_trusted_metadata.csv"
    client.fget_object(trusted_zone, latest_meta_file, local_meta)
    trusted_df = pd.read_csv(local_meta)
    os.remove(local_meta)
    
    valid_uuids = set(trusted_df["uuid"].dropna())
    print(f" Found {len(valid_uuids)} valid UUIDs in trusted metadata.")
    return valid_uuids

def _scan_existing_images(client, trusted_zone, img_prefix):
    """Scan existing images in trusted zone to avoid duplicates."""
    existing_trusted_uuids = set()
    for o in client.list_objects(trusted_zone, prefix=img_prefix, recursive=True):
        m = re.match(r".*/([a-f0-9\-]+)\.jpg$", o.object_name, re.IGNORECASE)
        if m:
            existing_trusted_uuids.add(m.group(1))
    print(f" Found {len(existing_trusted_uuids)} existing images in Trusted Zone.")
    return existing_trusted_uuids

def _anonymize_license_plates(img, plate_cascade):
    """Anonymize license plates in image using OpenCV."""
    cv_img = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
    gray = cv2.cvtColor(cv_img, cv2.COLOR_BGR2GRAY)
    plates = plate_cascade.detectMultiScale(gray, 1.1, 3, minSize=(50, 15))
    
    for (x, y, w, h) in plates:
        roi = cv_img[y:y+h, x:x+w]
        cv_img[y:y+h, x:x+w] = cv2.GaussianBlur(roi, (51, 51), 30)
    
    return Image.fromarray(cv2.cvtColor(cv_img, cv2.COLOR_BGR2RGB))

def _extract_image_uuid(obj):
    """Extract UUID from image object name."""
    m = re.match(r".*/([a-f0-9\-]+)\.\w+$", obj.object_name)
    if not m:
        return None
    return m.group(1)

def _is_valid_image_for_processing(img_uuid, valid_uuids, existing_trusted_uuids):
    """Check if image should be processed based on UUID validation."""
    if not img_uuid:
        return False
    if img_uuid not in valid_uuids:
        return False
    if img_uuid in existing_trusted_uuids:
        return False
    return True

def _process_and_upload_image(client, obj, formatted_zone, trusted_zone, img_prefix, 
                              plate_cascade, existing_trusted_uuids):
    """Process a single image: download, normalize, anonymize, compress, and upload."""
    # Download image
    data = client.get_object(formatted_zone, obj.object_name)
    img_bytes = data.read()
    data.close()
    data.release_conn()
    img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    
    # Normalize size/aspect ratio (max 1024px)
    max_dim = 1024
    if max(img.size) > max_dim:
        img.thumbnail((max_dim, max_dim))
    
    # Anonymize license plates
    img_clean = _anonymize_license_plates(img, plate_cascade)
    
    # Compress as JPEG with quality of 85
    buf = io.BytesIO()
    img_clean.save(buf, format="JPEG", quality=85)
    img_bytes = buf.getvalue()
    
    # Extract UUID for tracking
    img_uuid = _extract_image_uuid(obj)
    
    # Upload
    trusted_path = obj.object_name.replace(img_prefix, img_prefix)
    client.put_object(
        trusted_zone,
        trusted_path,
        data=io.BytesIO(img_bytes),
        length=len(img_bytes),
        content_type="image/jpeg"
    )
    if img_uuid:
        existing_trusted_uuids.add(img_uuid)
    return True

def _process_single_image_safe(client, obj, formatted_zone, trusted_zone, img_prefix,
                                plate_cascade, valid_uuids, existing_trusted_uuids):
    """Process a single image with error handling and validation."""
    img_uuid = _extract_image_uuid(obj)
    
    if not _is_valid_image_for_processing(img_uuid, valid_uuids, existing_trusted_uuids):
        return False
    
    try:
        _process_and_upload_image(client, obj, formatted_zone, trusted_zone, img_prefix,
                                 plate_cascade, existing_trusted_uuids)
        return True
    except Exception as e:
        print(f" Error processing {obj.object_name}: {e}")
        return False

# -----------------------
#     Configuration
# -----------------------
def process_trusted_images():

    # Get MinIO configuration from environment variables (set by orchestrator)
    minio, access_key, secret_key = get_minio_config()

    config = get_trusted_zone_config()
    formatted_zone = config['formatted_zone']
    trusted_zone = config['trusted_zone']
    img_prefix = config['img_prefix']
    meta_prefix = config['meta_prefix']
    
    # Setup MinIO client and bucket
    client = setup_minio_client_and_bucket(minio, access_key, secret_key, trusted_zone)
    
    # Load trusted metadata
    valid_uuids = _load_trusted_metadata(client, trusted_zone, meta_prefix)
    
    # Scan existing images
    existing_trusted_uuids = _scan_existing_images(client, trusted_zone, img_prefix)
    
    # Initialize license plate cascade
    plate_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_russian_plate_number.xml")
    
    # ----------------------------
    #   Process formatted images
    # ----------------------------
    formatted_images = list(client.list_objects(formatted_zone, prefix=img_prefix, recursive=True))
    print(f" Found {len(formatted_images)} images in Formatted Zone.")
    
    for obj in tqdm(formatted_images, desc="Processing images"):
        _process_single_image_safe(client, obj, formatted_zone, trusted_zone, img_prefix,
                                   plate_cascade, valid_uuids, existing_trusted_uuids)
    
    print(" All trusted images processed and uploaded.")

process_trusted_images();