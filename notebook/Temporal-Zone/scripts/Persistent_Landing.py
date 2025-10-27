"""
-Arman Bazarchi-
Persistent_Landing Zone
here we retrieve the stored raw data in temporal-landing and place them more organized in persistent-landing.
connects to minIO and creates a persistent_landing subbucket in temporal-zone bucket,
raises an error if the temporal_landing subbucket or temporal-zone bucket does not exist.
saves text data in a csv file in a folder 'metadata',
saves each image in its specie folder inside its family folder, inside its class, inside the kingdom it belongs to.
so we ensure organized data having easy access to each one.
example path of each image: temporal-zone/persistent_landing/images/{kingdom}/{class}/{family}/{specie}/{img_uuid}.jpg
it avoids storing duplicate data in persistent_landing.
in end removes temporary files from local storage and delets the temporal_landing as we have now moved the data to persistent.

"""


from minio import Minio
import pandas as pd
import io
import os
from datetime import datetime
import re
import sys

# ==============================
# Helper Functions
# ==============================

def setup_minio_client_and_buckets(minio, access_key, secret_key, root_bucket, temp_prefix, persist_prefix):
    """Setup MinIO client and validate/create buckets."""
    client = Minio(
        minio,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    
    # Validate buckets
    if not client.bucket_exists(root_bucket):
        sys.exit(" ERROR: Root bucket 'Landing' does not exist in MinIO.")
    
    temporal_exists = any(
        obj.object_name.startswith(f"{temp_prefix}/")
        for obj in client.list_objects(root_bucket, recursive=False)
    )
    if not temporal_exists:
        sys.exit(" ERROR: 'Temporal_Landing' does not exist inside 'Landing' bucket.")
    
    # Create Persistent_Landing if missing
    persistent_exists = any(
        obj.object_name.startswith(f"{persist_prefix}/")
        for obj in client.list_objects(root_bucket, recursive=False)
    )
    if not persistent_exists:
        client.put_object(
            root_bucket,
            f"{persist_prefix}/.init",
            data=io.BytesIO(b"init"),
            length=4,
            content_type="text/plain"
        )
        print(f" Created 'Persistent_Landing' inside '{root_bucket}'.")
    
    return client

def load_metadata_from_temporal(client, root_bucket, temp_prefix):
    """Load metadata from Temporal_Landing."""
    print("📥 Loading metadata from Temporal_Landing...")
    local_metadata = "temp_metadata.csv"
    temp_metadata_path = f"{temp_prefix}/metadata/metadata_final.csv"
    try:
        client.fget_object(root_bucket, temp_metadata_path, local_metadata)
    except Exception as e:
        sys.exit(f" ERROR: Failed to find metadata at {temp_metadata_path} → {e}")
    
    metadata_df = pd.read_csv(local_metadata)
    print(f" Loaded metadata with {len(metadata_df)} records.")
    return metadata_df, local_metadata

def scan_existing_persistent_images(client, root_bucket, persist_prefix):
    """Scan existing images in Persistent_Landing."""
    print(" Checking existing images in Persistent_Landing...")
    existing_persistent_uuids = set()
    for obj in client.list_objects(root_bucket, prefix=f"{persist_prefix}/images/", recursive=True):
        match = re.match(rf"{persist_prefix}/images/.+?/([a-f0-9\-]+)\.jpg", obj.object_name)
        if match:
            existing_persistent_uuids.add(match.group(1))
    
    print(f" Found {len(existing_persistent_uuids)} existing images in Persistent_Landing.")
    return existing_persistent_uuids

def move_single_image(client, root_bucket, persist_prefix, row, existing_persistent_uuids):
    """Move a single image from Temporal to Persistent."""
    try:
        img_uuid = row.get("uuid")
        if not img_uuid:
            return None, False
        
        # Skip if image already exists in Persistent
        if img_uuid in existing_persistent_uuids:
            print(f" Skipping duplicate UUID: {img_uuid}")
            return None, False
        
        object_name = row.get("temporal_path") 
        src_path = object_name
        
        kingdom = str(row.get("kingdom", "Unknown")).replace(" ", "_")
        cls = str(row.get("class", "Unknown")).replace(" ", "_")
        family = str(row.get("family", "Unknown")).replace(" ", "_")
        specie = str(row.get("species", "Unknown")).replace(" ", "_")
        
        # Destination path 
        dest_path = f"{persist_prefix}/images/{kingdom}/{cls}/{family}/{specie}/{img_uuid}.jpg"
        
        # Download image from Temporal
        data = client.get_object(root_bucket, src_path)
        image_bytes = data.read()
        data.close()
        data.release_conn()
        
        # Upload image to Persistent
        client.put_object(
            root_bucket,
            dest_path,
            data=io.BytesIO(image_bytes),
            length=len(image_bytes),
            content_type="image/jpeg"
        )
        
        existing_persistent_uuids.add(img_uuid)
        print(f" Moved {src_path} → {dest_path}")
        
        return dest_path, True
        
    except Exception as e:
        print(f" Error processing {object_name}: {e}")
        return None, False

def process_metadata_group(client, root_bucket, persist_prefix, kingdom_name, cls_name, group):
    """Process metadata for a single kingdom-class group."""
    kingdom_safe = str(kingdom_name).replace(" ", "_")
    cls_safe = str(cls_name).replace(" ", "_")
    
    # Create a new filename with current timestamp for updated metadata
    timestamp = datetime.now().strftime("%Y_%m_%d_%H:%M")
    metadata_filename = f"{kingdom_safe}_{cls_safe}_metadata_{timestamp}.csv"
    local_metadata_file = f"{kingdom_safe}_{cls_safe}_metadata_temp.csv"
    persistent_metadata_dir = f"{persist_prefix}/metadata/"
    
    # Look for any existing metadata file for this kingdom+class
    existing_metadata_files = [
        obj.object_name for obj in client.list_objects(root_bucket, prefix=persistent_metadata_dir, recursive=True)
        if re.match(rf"{persistent_metadata_dir}{kingdom_safe}_{cls_safe}_metadata_.*\.csv", obj.object_name)
    ]
    
    if existing_metadata_files:
        # Take the latest metadata file (any timestamp)
        existing_metadata_files.sort(reverse=True)
        existing_metadata_path = existing_metadata_files[0]
        existing_local_file = f"existing_{kingdom_safe}_{cls_safe}.csv"
        client.fget_object(root_bucket, existing_metadata_path, existing_local_file)
        
        # Read existing metadata
        existing_df = pd.read_csv(existing_local_file)
        
        # Delete old file from Persistent
        client.remove_object(root_bucket, existing_metadata_path)
        os.remove(existing_local_file)
        
        # Merge Temporal rows that are not already in Persistent
        new_rows = group[~group["uuid"].isin(existing_df["uuid"])]
        merged_df = pd.concat([existing_df, new_rows], ignore_index=True)
    else:
        # No existing metadata -> use all rows from Temporal
        group["persistent_path"]
        merged_df = group
    
    # Save merged metadata with updated timestamp
    merged_df.to_csv(local_metadata_file, index=False)
    persistent_metadata_path_new = f"{persistent_metadata_dir}{metadata_filename}"
    client.fput_object(
        root_bucket,
        persistent_metadata_path_new,
        local_metadata_file,
        content_type="text/csv"
    )
    
    os.remove(local_metadata_file)
    print(f" Updated/uploaded metadata for '{kingdom_safe}-{cls_safe}' → {persistent_metadata_path_new}")

def cleanup_temporal_landing(client, root_bucket, temp_prefix):
    """Cleanup Temporal Landing (files only)."""
    print(" Cleaning up Temporal_Landing zone (files only)...")
    
    try:
        temporal_objects = list(client.list_objects(root_bucket, prefix=f"{temp_prefix}/", recursive=True))
        if not temporal_objects:
            print(" Temporal_Landing is already empty.")
        else:
            deleted_count = 0
            for obj in temporal_objects:
                # Skip the temporal-landing folder itself 
                if (obj.object_name == f"{temp_prefix}/" or 
                    obj.object_name == f"{temp_prefix}"):
                    continue
                
                client.remove_object(root_bucket, obj.object_name)
                print(f" Deleted file: {obj.object_name}")
                deleted_count += 1
            
            print(f" Cleaned up {deleted_count} files from Temporal-Landing (folders kept).")
    except Exception as e:
        print(f" Warning: Failed to fully clean Temporal_Landing → {e}")

# ==============================
# 1.  Configuration
# ==============================
def process_landing_zone(
    minio="localhost:9000",
    access_key="admin",
    secret_key="password123"): 
    
    root_bucket="temporal-zone" # main bucket
    temp_prefix="temporal-landing" # source bucket
    persist_prefix="persistent_landing" # destination bucket
    
    # Setup MinIO client and buckets
    client = setup_minio_client_and_buckets(minio, access_key, secret_key, root_bucket, temp_prefix, persist_prefix)
    
    # Load metadata from Temporal_Landing
    metadata_df, local_metadata = load_metadata_from_temporal(client, root_bucket, temp_prefix)
    
    # Scan existing Persistent images by UUID
    existing_persistent_uuids = scan_existing_persistent_images(client, root_bucket, persist_prefix)
    metadata_df["persistent_path"] = None
    
    # Move images from Temporal -> Persistent
    moved_records = []
    
    for idx, row in metadata_df.iterrows():
        dest_path, success = move_single_image(client, root_bucket, persist_prefix, row, existing_persistent_uuids)
        if success:
            metadata_df.loc[idx, "persistent_path"] = dest_path
            moved_records.append(row)
    
    # Save or merge metadata files by (Kingdom, Class)
    if not metadata_df.empty:
        print(" Updating kingdom-class based metadata files...")
        
        for (kingdom_name, cls_name), group in metadata_df.groupby(["kingdom", "class"]):
            process_metadata_group(client, root_bucket, persist_prefix, kingdom_name, cls_name, group)
        
        # Cleanup local temp
        os.remove(local_metadata)
        print(" Cleaned up local metadata files.")
    else:
        print(" No new images moved; skipping metadata upload.")
    
    # Cleanup Temporal Landing (files only)
    cleanup_temporal_landing(client, root_bucket, temp_prefix)
    
    print(" Persistent Landing Zone completed successfully.")

process_landing_zone();