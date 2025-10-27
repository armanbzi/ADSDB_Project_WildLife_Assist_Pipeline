#!/usr/bin/env python
# coding: utf-8

# In[1]:


"""
-Arman Bazarchi-
Trusted Zone — Metadata notebook

   Here we apply some cleaning on text data and move from fomatted-zone to trusted-zone
   connects to minIO, creates trusted-zone bucket, raises an error if formatted-zone does not exist.
   removes rows with missing id or a correct image path, makes sure of normality of all columns.
   text data gets normalized for another time here to ensure it completely.
   replaces missing valaues with 'none',
   it avoids storing duplicate data in formatted-zone, also removes any temporal file in local storage.


"""

from minio import Minio
import pandas as pd
import io, os, shutil
from datetime import datetime

# -----------------------
# 1. Configuration
# -----------------------
def process_trusted_metadata(
    minio_endpoint = "localhost:9000",
    access_key = "admin",
    secret_key = "password123"):
    """Main function to process trusted metadata"""
    
    # Setup configuration
    config = _setup_trusted_metadata_config()
    
    # Connect to MinIO and setup buckets
    client = _connect_to_minio_trusted_metadata(minio_endpoint, access_key, secret_key)
    
    # Load formatted metadata
    metadata_df = _load_formatted_metadata(client, config)
    
    # Clean metadata
    cleaned_metadata_df = _clean_metadata(metadata_df, config)
    
    # Merge with existing trusted metadata
    final_metadata_df = _merge_with_existing_trusted_metadata(client, cleaned_metadata_df, config)
    
    # Save to trusted zone
    _save_trusted_metadata(client, final_metadata_df, config)

def _setup_trusted_metadata_config():
    """Setup configuration for trusted metadata"""
    return {
        'FORMATTED_ZONE': "formatted-zone",
        'TRUSTED_ZONE': "trusted-zone",
        'META_PREFIX': "metadata/",
        'TARGET_COLUMNS': [
            "uuid", "kingdom", "phylum", "class", "order", "family",
            "genus", "species", "scientific_name", "common",
            "persistent_path", "formatted_path", "image_url"
        ]
    }

def _connect_to_minio_trusted_metadata(minio_endpoint, access_key, secret_key):
    """Connect to MinIO and setup buckets"""
    # Connect to MinIO
    client = Minio(minio_endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    if not client.bucket_exists("trusted-zone"):
        client.make_bucket("trusted-zone")
        print(" Created trusted zone bucket: trusted-zone")
    return client

def _load_formatted_metadata(client, config):
    """Load all formatted metadata files"""
    # Read all formatted metadata files
    metadata_objs = [
        obj.object_name for obj in client.list_objects(config['FORMATTED_ZONE'], prefix=config['META_PREFIX'], recursive=True)
        if obj.object_name.lower().endswith(".csv")
    ]

    if not metadata_objs:
        raise SystemExit(" No formatted metadata files found.")

    all_dfs = []
    for obj_name in metadata_objs:
        resp = client.get_object(config['FORMATTED_ZONE'], obj_name)
        data = resp.read()
        resp.close()
        resp.release_conn()
        df = pd.read_csv(io.BytesIO(data))
        all_dfs.append(df)

    # Combine all formatted metadata
    metadata_df = pd.concat(all_dfs, ignore_index=True)
    return metadata_df

def _clean_metadata(metadata_df, config):
    """Clean metadata: normalize columns, remove duplicates, filter missing data"""
    # Keep only target columns
    for col in config['TARGET_COLUMNS']:
        if col not in metadata_df.columns:
            metadata_df[col] = pd.NA
    metadata_df = metadata_df[config['TARGET_COLUMNS']]

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

def _merge_with_existing_trusted_metadata(client, metadata_df, config):
    """Merge with existing trusted metadata to avoid duplicates"""
    trusted_metadata_files = [
        obj.object_name for obj in client.list_objects(config['TRUSTED_ZONE'], prefix=config['META_PREFIX'], recursive=True)
        if obj.object_name.lower().endswith(".csv") and "trusted_metadata_" in obj.object_name
    ]

    if trusted_metadata_files:
        # Take the latest trusted metadata CSV
        trusted_metadata_files.sort(reverse=True)
        latest_file = trusted_metadata_files[0]
        local_existing = "temp_existing_trusted_metadata.csv"

        # Download existing trusted metadata
        client.fget_object(config['TRUSTED_ZONE'], latest_file, local_existing)
        existing_trusted_df = pd.read_csv(local_existing)

        # Remove old trusted metadata files 
        _remove_old_trusted_metadata_files(client, config)

        # Merge only new rows 
        new_rows = metadata_df[~metadata_df["uuid"].isin(existing_trusted_df["uuid"])]
        if not new_rows.empty:
            metadata_df = pd.concat([existing_trusted_df, new_rows], ignore_index=True)
            print(f" Added {len(new_rows)} new rows to trusted metadata, total now: {len(metadata_df)}")
        else:
            print(" No new rows to add; trusted metadata is up to date.")

        # Cleanup local temp
        os.remove(local_existing)
    else:
        print(" No existing trusted metadata found; creating new one.")

    return metadata_df

def _remove_old_trusted_metadata_files(client, config):
    """Remove old trusted metadata files"""
    for obj in client.list_objects(config['TRUSTED_ZONE'], prefix=config['META_PREFIX'], recursive=True):
        if obj.object_name.startswith("metadata/trusted_metadata_"):
            client.remove_object(config['TRUSTED_ZONE'], obj.object_name)
            print(f" Removed old trusted metadata file: {obj.object_name}")

def _save_trusted_metadata(client, metadata_df, config):
    """Save cleaned metadata to Trusted Zone"""
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    local_file = f"trusted_metadata_{timestamp}.csv"
    os.makedirs("temp_trusted", exist_ok=True)
    local_path = os.path.join("temp_trusted", local_file)
    metadata_df.to_csv(local_path, index=False)

    # Upload to MinIO
    client.fput_object(
        config['TRUSTED_ZONE'],
        f"{config['META_PREFIX']}trusted_metadata_{timestamp}.csv",
        local_path,
        content_type="text/csv"
    )

    # Cleanup
    os.remove(local_path)
    shutil.rmtree("temp_trusted")

    print("✅ Trusted metadata processing completed successfully!")

if __name__ == "__main__":
    process_trusted_metadata()


# In[ ]: