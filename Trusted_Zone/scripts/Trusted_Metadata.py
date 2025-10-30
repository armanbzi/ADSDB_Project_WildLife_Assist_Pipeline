"""
-Arman Bazarchi-
Trusted Zone — Metadata notebook

   Here we apply some cleaning on text data and move from fomatted-zone to trusted-zone
   connects to minIO, creates trusted-zone bucket, raises an error if formatted-zone does not exist.
   removes rows with missing id or a correct image path, makes sure of normality of all columns.
   text data gets normalized for another time here to ensure it completely.
   replaces missing valaues with 'none',
   
   avoiding storing duplicate data in formatted-zone, also removes any temporal file in local storage.


"""

from minio import Minio
import pandas as pd
import io, os, shutil
from datetime import datetime
from shared_utils import get_minio_config, setup_minio_client_and_bucket, get_trusted_zone_config

# -----------------------
#    Functions
# -----------------------

# -----------------------
#    Configuration
# -----------------------


def _load_formatted_metadata(client, formatted_zone, meta_prefix):
    """Load all formatted metadata files from MinIO and concatenate them."""
    metadata_objs = [
        obj.object_name for obj in client.list_objects(formatted_zone, prefix=meta_prefix, recursive=True)
        if obj.object_name.lower().endswith(".csv")
    ]
    
    if not metadata_objs:
        raise SystemExit(" No formatted metadata files found.")
    
    all_dfs = []
    for obj_name in metadata_objs:
        resp = client.get_object(formatted_zone, obj_name)
        data = resp.read()
        resp.close()
        resp.release_conn()
        df = pd.read_csv(io.BytesIO(data))
        all_dfs.append(df)
    
    return pd.concat(all_dfs, ignore_index=True)

def _clean_metadata(metadata_df, target_columns):
    """Clean metadata: keep target columns, remove duplicates, remove missing values, normalize strings."""
    # Keep only target columns
    for col in target_columns:
        if col not in metadata_df.columns:
            metadata_df[col] = pd.NA
    metadata_df = metadata_df[target_columns]
    
    # Remove duplicates by uuid
    before_dupe = len(metadata_df)
    metadata_df = metadata_df.drop_duplicates(subset=["uuid"], keep="first").reset_index(drop=True)
    after_dupe = len(metadata_df)
    print(f"Removed {before_dupe - after_dupe} duplicate rows by uuid.")
    
    # Remove rows missing uuid or formatted_path
    before_missing = len(metadata_df)
    metadata_df = metadata_df[metadata_df["uuid"].notna()]
    metadata_df = metadata_df[metadata_df["formatted_path"].notna()]
    after_missing = len(metadata_df)
    print(f"Removed {before_missing - after_missing} rows missing uuid or formatted_path.")
    
    # Normalize strings
    str_cols = ["kingdom","phylum","class","order","family","genus","species","scientific_name","common","persistent_path","formatted_path","image_url"]
    for col in str_cols:
        metadata_df[col] = metadata_df[col].astype("string").str.strip().replace({"": pd.NA})
    
    # Replace "None" strings with pd.NA
    metadata_df.replace({"NA": pd.NA, "None": pd.NA}, inplace=True)
    
    return metadata_df

def _merge_with_existing_trusted(client, trusted_zone, meta_prefix, metadata_df):
    """Merge new metadata with existing trusted metadata to avoid duplicates."""
    trusted_metadata_files = [
        obj.object_name for obj in client.list_objects(trusted_zone, prefix=meta_prefix, recursive=True)
        if obj.object_name.lower().endswith(".csv") and "trusted_metadata_" in obj.object_name
    ]
    
    if not trusted_metadata_files:
        print(" No existing trusted metadata found; creating new one.")
        return metadata_df
    
    # Take the latest trusted metadata CSV
    trusted_metadata_files.sort(reverse=True)
    latest_file = trusted_metadata_files[0]
    local_existing = "temp_existing_trusted_metadata.csv"
    
    # Download existing trusted metadata
    client.fget_object(trusted_zone, latest_file, local_existing)
    existing_trusted_df = pd.read_csv(local_existing)
    
    # Remove old trusted metadata files 
    for obj in client.list_objects(trusted_zone, prefix=meta_prefix, recursive=True):
        if obj.object_name.startswith("metadata/trusted_metadata_"):
            client.remove_object(trusted_zone, obj.object_name)
            print(f" Removed old trusted metadata file: {obj.object_name}")
    
    # Merge only new rows 
    new_rows = metadata_df[~metadata_df["uuid"].isin(existing_trusted_df["uuid"])]
    if not new_rows.empty:
        metadata_df = pd.concat([existing_trusted_df, new_rows], ignore_index=True)
        print(f" Added {len(new_rows)} new rows to trusted metadata, total now: {len(metadata_df)}")
    else:
        print(" No new rows to add; trusted metadata is up to date.")
        metadata_df = existing_trusted_df
    
    # Cleanup local temp
    os.remove(local_existing)
    return metadata_df

def _save_and_cleanup(client, trusted_zone, meta_prefix, metadata_df):
    """Save cleaned metadata to Trusted Zone and cleanup temp files."""
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    local_file = f"trusted_metadata_{timestamp}.csv"
    os.makedirs("temp_trusted", exist_ok=True)
    local_path = os.path.join("temp_trusted", local_file)
    metadata_df.to_csv(local_path, index=False)
    
    # Upload to MinIO trusted-zone/metadata/
    client.fput_object(trusted_zone, f"{meta_prefix}{local_file}", local_path, content_type="text/csv")
    print(f" Trusted metadata uploaded: {meta_prefix}{local_file}")
    
    # Cleanup
    os.remove(local_path)
    shutil.rmtree("temp_trusted")

def _get_configuration():
    """Get configuration constants."""
    config = get_trusted_zone_config()
    target_columns = [
        "uuid", "kingdom", "phylum", "class", "order", "family",
        "genus", "species", "scientific_name", "common",
        "persistent_path", "formatted_path", "image_url"
    ]
    return config['formatted_zone'], config['trusted_zone'], config['meta_prefix'], target_columns

def _process_metadata_pipeline(client, formatted_zone, trusted_zone, meta_prefix, target_columns):
    """Execute the full metadata processing pipeline."""
    metadata_df = _load_formatted_metadata(client, formatted_zone, meta_prefix)
    metadata_df = _clean_metadata(metadata_df, target_columns)
    metadata_df = _merge_with_existing_trusted(client, trusted_zone, meta_prefix, metadata_df)
    return metadata_df

# -----------------------
#    Configuration
# -----------------------
def process_trusted_metadata():

    # Get MinIO configuration from environment variables (set by orchestrator)
    minio, access_key, secret_key = get_minio_config()

    # Get configuration constants
    formatted_zone, trusted_zone, meta_prefix, target_columns = _get_configuration()
    
    # Setup MinIO client and bucket
    client = setup_minio_client_and_bucket(minio, access_key, secret_key, trusted_zone)
    
    # Process metadata pipeline
    metadata_df = _process_metadata_pipeline(client, formatted_zone, trusted_zone, meta_prefix, target_columns)
    
    # Save and cleanup
    _save_and_cleanup(client, trusted_zone, meta_prefix, metadata_df)
    print(" Trusted metadata processing complete.")

process_trusted_metadata();
