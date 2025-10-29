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

# -----------------------
#     Configuration
# -----------------------
def process_trusted_images(
    MINIO = "localhost:9000", # defalut configurations
    ACCESS_KEY = "admin",
    SECRET_KEY = "password123"):

    FORMATTED_ZONE = "formatted-zone"
    TRUSTED_ZONE = "trusted-zone"
    
    IMG_PREFIX = "images/"
    META_PREFIX = "metadata/"
    
    # Connect to MinIO
    client = Minio(MINIO, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
    if not client.bucket_exists(TRUSTED_ZONE):
        client.make_bucket(TRUSTED_ZONE)
        print(f" Created trusted zone bucket: {TRUSTED_ZONE}")
    
    # --------------------------
    #   Load trusted metadata
    # --------------------------
    trusted_meta_files = [
        o.object_name for o in client.list_objects(TRUSTED_ZONE, prefix=META_PREFIX, recursive=True)
        if o.object_name.lower().endswith(".csv") and "trusted_metadata_" in o.object_name]
    
    if not trusted_meta_files:
        raise SystemExit(" No trusted metadata found.")
    
    trusted_meta_files.sort(reverse=True)
    latest_meta_file = trusted_meta_files[0]
    local_meta = "temp_trusted_metadata.csv"
    client.fget_object(TRUSTED_ZONE, latest_meta_file, local_meta)
    trusted_df = pd.read_csv(local_meta)
    os.remove(local_meta)
    
    valid_uuids = set(trusted_df["uuid"].dropna())
    print(f" Found {len(valid_uuids)} valid UUIDs in trusted metadata.")
    
    
    
    #  Get existing images to avoid duplicates
    existing_trusted_uuids = set()
    for o in client.list_objects(TRUSTED_ZONE, prefix=IMG_PREFIX, recursive=True):
        m = re.match(r".*/([a-f0-9\-]+)\.jpg$", o.object_name, re.IGNORECASE)
        if m:
            existing_trusted_uuids.add(m.group(1))
    print(f" Found {len(existing_trusted_uuids)} existing images in Trusted Zone.")
    
    
    
    # Only license plate detection, we cant apply bluring face because it can blur faces of species.
    plate_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_russian_plate_number.xml")
    
    # ----------------------------
    #   Process formatted images
    # ----------------------------
    formatted_images = list(client.list_objects(FORMATTED_ZONE, prefix=IMG_PREFIX, recursive=True))
    print(f" Found {len(formatted_images)} images in Formatted Zone.")
    
    for obj in tqdm(formatted_images, desc="Processing images"):
        try:
            m = re.match(r".*/([a-f0-9\-]+)\.\w+$", obj.object_name)
            if not m:
                continue
            img_uuid = m.group(1)
            if img_uuid not in valid_uuids or img_uuid in existing_trusted_uuids:
                continue
    
            # Download image
            data = client.get_object(FORMATTED_ZONE, obj.object_name)
            img_bytes = data.read(); data.close(); data.release_conn()
            img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    
            # Normalize size/aspect ratio (max 1024px)
            max_dim = 1024
            if max(img.size) > max_dim:
                img.thumbnail((max_dim, max_dim))
    
            # Convert to OpenCV for license plate anonymization
            cv_img = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
            gray = cv2.cvtColor(cv_img, cv2.COLOR_BGR2GRAY)
            plates = plate_cascade.detectMultiScale(gray, 1.1, 3, minSize=(50, 15))
    
            for (x, y, w, h) in plates:
                roi = cv_img[y:y+h, x:x+w]
                cv_img[y:y+h, x:x+w] = cv2.GaussianBlur(roi, (51, 51), 30)
    
            # Back to PIL and compress as JPEG with quality of 85
            img_clean = Image.fromarray(cv2.cvtColor(cv_img, cv2.COLOR_BGR2RGB))
            buf = io.BytesIO()
            img_clean.save(buf, format="JPEG", quality=85)
            img_bytes = buf.getvalue()
    
            # Upload
            trusted_path = obj.object_name.replace(IMG_PREFIX, IMG_PREFIX)
            client.put_object(
                TRUSTED_ZONE,
                trusted_path,
                data=io.BytesIO(img_bytes),
                length=len(img_bytes),
                content_type="image/jpeg"
            )
            existing_trusted_uuids.add(img_uuid)
    
        except Exception as e:
            print(f" Error processing {obj.object_name}: {e}")
            continue
    
    print(" All trusted images processed and uploaded.")

process_trusted_images();