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

# -----------------------
#    Functions
# -----------------------
def get_minio_config():
    # Load MinIO configuration from environment variables (set by orchestrator).
    
    import os
    
    # Get configuration from environment variables (set by orchestrator)
    endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'admin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'admin123')
    
    print(f"Using MinIO configuration from environment variables: endpoint={endpoint}, access_key={access_key[:3]}***")
    return endpoint, access_key, secret_key

# -----------------------
#    Configuration
# -----------------------
def process_trusted_metadata(
    MINIO = "localhost:9000",
    ACCESS_KEY = "admin",
    SECRET_KEY = "password123"):

    # Get MinIO configuration from environment variables (set by orchestrator)
    MINIO, ACCESS_KEY, SECRET_KEY = get_minio_config()

    FORMATTED_ZONE = "formatted-zone"
    TRUSTED_ZONE = "trusted-zone"
    
    META_PREFIX = "metadata/"
    
    TARGET_COLUMNS = [
        "uuid", "kingdom", "phylum", "class", "order", "family",
        "genus", "species", "scientific_name", "common",
        "persistent_path", "formatted_path", "image_url"
    ]
    
    
    #  Connect to MinIO
    client = Minio(MINIO, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
    if not client.bucket_exists(TRUSTED_ZONE):
        client.make_bucket(TRUSTED_ZONE)
        print(f" Created trusted zone bucket: {TRUSTED_ZONE}")
    
    # -------------------------------------
    #   Read all formatted metadata files
    # -------------------------------------
    metadata_objs = [
        obj.object_name for obj in client.list_objects(FORMATTED_ZONE, prefix=META_PREFIX, recursive=True)
        if obj.object_name.lower().endswith(".csv")
    ]
    
    if not metadata_objs:
        raise SystemExit(" No formatted metadata files found.")
    
    all_dfs = []
    for obj_name in metadata_objs:
        resp = client.get_object(FORMATTED_ZONE, obj_name)
        data = resp.read()
        resp.close()
        resp.release_conn()
        df = pd.read_csv(io.BytesIO(data))
        all_dfs.append(df)
    
    #  all formatted metadata
    metadata_df = pd.concat(all_dfs, ignore_index=True)
    
    # --------------------------
    #    Trusted Zone cleaning
    # --------------------------
    
    # Keep only target columns
    for col in TARGET_COLUMNS:
        if col not in metadata_df.columns:
            metadata_df[col] = pd.NA
    metadata_df = metadata_df[TARGET_COLUMNS]
    
    #  Remove duplicates by uuid
    before_dupe = len(metadata_df)
    metadata_df = metadata_df.drop_duplicates(subset=["uuid"], keep="first").reset_index(drop=True)
    after_dupe = len(metadata_df)
    print(f"Removed {before_dupe - after_dupe} duplicate rows by uuid.")
    
    #  Remove rows missing uuid or formatted_path
    before_missing = len(metadata_df)
    metadata_df = metadata_df[metadata_df["uuid"].notna()]
    metadata_df = metadata_df[metadata_df["formatted_path"].notna()]
    after_missing = len(metadata_df)
    print(f"Removed {before_missing - after_missing} rows missing uuid or formatted_path.")
    
    #  Normalize strings
    str_cols = ["kingdom","phylum","class","order","family","genus","species","scientific_name","common","persistent_path","formatted_path","image_url"]
    for col in str_cols:
        metadata_df[col] = metadata_df[col].astype("string").str.strip().replace({"": pd.NA})
    
    #  "None" strings with pd.NA
    metadata_df.replace({"NA": pd.NA, "None": pd.NA}, inplace=True)
    
    
    # -----------------------
    #  Merge with existing trusted metadata (avoid duplicates)
    # -----------------------
    trusted_metadata_files = [
        obj.object_name for obj in client.list_objects(TRUSTED_ZONE, prefix=META_PREFIX, recursive=True)
        if obj.object_name.lower().endswith(".csv") and "trusted_metadata_" in obj.object_name
    ]
    
    if trusted_metadata_files:
        # Take the latest trusted metadata CSV
        trusted_metadata_files.sort(reverse=True)
        latest_file = trusted_metadata_files[0]
        local_existing = "temp_existing_trusted_metadata.csv"
    
        # Download existing trusted metadata
        client.fget_object(TRUSTED_ZONE, latest_file, local_existing)
        existing_trusted_df = pd.read_csv(local_existing)
    
        # Remove old trusted metadata files 
        for obj in client.list_objects(TRUSTED_ZONE, prefix=META_PREFIX, recursive=True):
            if obj.object_name.startswith("metadata/trusted_metadata_"):
                client.remove_object(TRUSTED_ZONE, obj.object_name)
                print(f" Removed old trusted metadata file: {obj.object_name}")
    
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
    
    # -----------------------
    #    Save cleaned metadata to Trusted Zone
    # -----------------------
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    local_file = f"trusted_metadata_{timestamp}.csv"
    os.makedirs("temp_trusted", exist_ok=True)
    local_path = os.path.join("temp_trusted", local_file)
    metadata_df.to_csv(local_path, index=False)
    
    # Upload to MinIO trusted-zone/metadata/
    client.fput_object(TRUSTED_ZONE, f"{META_PREFIX}{local_file}", local_path, content_type="text/csv")
    print(f" Trusted metadata uploaded: {META_PREFIX}{local_file}")
    
    # Cleanup
    os.remove(local_path)
    shutil.rmtree("temp_trusted")
    print(" Trusted metadata processing complete.")

process_trusted_metadata();
