"""
-Arman Bazarchi-
Formatted_Metadata
Here we ensure the format of only text metadata we had in persistent_landing and move to formatted-zone bucket.
we normalize the data and remove any incomplete data.
connects to minIO, creates formatted-zone bucket, raises an error if pesistant_landing or temporal-zone does not exist.
read data from persistent and normilize then store in formatted-zone, 
here we save a csv file, a .parquet file, a .json file, a schema summary of our text metadata for different uses in future.
it avoids storing duplicate data in formatted-zone, also removes any temporal file in local storage.

"""

from minio import Minio
import pandas as pd
import io, json, os, re, shutil
from datetime import datetime

# ==============================
#          Functions
# ==============================

def setup_minio_client_and_buckets(minio_endpoint, access_key, secret_key, landing_zone, persist_prefix, formatted_zone):
    # Setup MinIO client and validate/create buckets.
    client = Minio(
        minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    
    # Break if temporal landing or temporal-zone does not exist
    if not client.bucket_exists(landing_zone):
        sys.exit(" ERROR: Root bucket 'Landing' does not exist in MinIO.")
    
    persistent_objects = list(client.list_objects(landing_zone, prefix=f"{persist_prefix}/", recursive=False))
    if not persistent_objects:
        raise FileNotFoundError(f" Required prefix '{persist_prefix}/' not found inside '{landing_zone}' bucket.")
    
    # Ensure formatted-zone bucket exists
    if not client.bucket_exists(formatted_zone):
        client.make_bucket(formatted_zone)
        print(f" Created Formatted Zone bucket: {formatted_zone}")
    else:
        print(f" Formatted Zone bucket already exists: {formatted_zone}")
    
    return client

def load_metadata_files(client, landing_zone, persist_prefix):
    # Load all metadata files from Persistent Landing.
    
    metadata_objects = [
        obj.object_name for obj in client.list_objects(landing_zone, prefix=f"{persist_prefix}/metadata/", recursive=True)
        if obj.object_name.endswith(".csv") or obj.object_name.endswith(".json")
    ]
    
    if not metadata_objects:
        raise FileNotFoundError(" No metadata files found in Persistent Landing Zone.")
    
    print(f" Found {len(metadata_objects)} metadata files to process.")

    # search for metadata csv or json in temporal-zone
    all_dfs = []
    for obj_name in metadata_objects:
        print(f" Reading: {obj_name}")
        response = client.get_object(landing_zone, obj_name)
        data = response.read()
        response.close()
        response.release_conn()
        
        try:
            if obj_name.endswith(".csv"):
                df = pd.read_csv(io.BytesIO(data))
            elif obj_name.endswith(".json"):
                json_data = json.load(io.BytesIO(data))
                df = pd.json_normalize(json_data)
            else:
                continue
            
            all_dfs.append(df)
        except Exception as e:
            print(f" Error reading {obj_name}: {e}")
            continue
    
    if not all_dfs:
        raise ValueError(" No valid metadata could be loaded.")
    
    return all_dfs

def normalize_and_combine_metadata(all_dfs, persist_prefix):
    # Normalize schema and combine all metadata.
    target_columns = [
        "uuid", "kingdom", "phylum", "class", "order", "family",
        "genus", "species", "scientific_name", "common",
        "persistent_path","formatted_path", "image_url"
    ]
    
    for i, df in enumerate(all_dfs):
        for col in target_columns:
            if col not in df.columns:
                df[col] = None
        all_dfs[i] = df[target_columns]
    
    # Combine all Persistent metadata into one DataFrame
    persistent_df = pd.concat(all_dfs, ignore_index=True)
    
    # Generate formatted_path same as persistent_path
    persistent_df["formatted_path"] = persistent_df["persistent_path"].str.replace(
        f"{persist_prefix}/images", "images", regex=False
    )
    
    return persistent_df

def load_existing_general_metadata(client, formatted_zone, target_columns):
    # Load existing general metadata from Formatted Zone. to skip duplicates.
    
    general_metadata_files = [
        obj.object_name for obj in client.list_objects(formatted_zone, prefix="metadata/", recursive=True)
        if re.match(r"metadata/all_metadata_.*\.csv", obj.object_name)
    ]
    
    if general_metadata_files:
        # Take the latest general metadata CSV
        general_metadata_files.sort(reverse=True)
        latest_metadata_file = general_metadata_files[0]
        local_existing = "temp_existing_metadata.csv"
        
        # Download existing metadata
        client.fget_object(formatted_zone, latest_metadata_file, local_existing)
        general_df = pd.read_csv(local_existing)
        
        # Delete the old file from formatted-zone
        client.remove_object(formatted_zone, latest_metadata_file)
        # Remove old general formatted files
        general_files_prefix = "metadata/all_metadata"
        schema_files_prefix = "metadata/schema_summary"
        # List all objects in formatted-zone/metadata
        for obj in client.list_objects(formatted_zone, prefix="metadata/", recursive=True):
            if obj.object_name.startswith(general_files_prefix) or obj.object_name.startswith(schema_files_prefix):
                client.remove_object(formatted_zone, obj.object_name)
                print(f" Removed old file: {obj.object_name}")
        
        # Cleanup local temp
        os.remove(local_existing)
        print(f" Loaded and removed existing general metadata with {len(general_df)} rows")
    else:
        # No general metadata exists yet
        general_df = pd.DataFrame(columns=target_columns)
        print(" No existing general metadata found, creating new one")
    
    return general_df

def merge_and_prepare_data(persistent_df, general_df):
    # Merge new rows and prepare final dataset.
    
    new_rows = persistent_df[~persistent_df["uuid"].isin(general_df["uuid"])]
    if not new_rows.empty:
        updated_df = pd.concat([general_df, new_rows], ignore_index=True)
        print(f" Adding {len(new_rows)} new rows to general metadata, total now: {len(updated_df)}")
    else:
        updated_df = general_df
        print(" No new rows to add; general metadata is up to date")
    
    # Schema summary
    schema_summary = pd.DataFrame({
        "column_name": updated_df.columns,
        "dtype": [str(updated_df[c].dtype) for c in updated_df.columns],
        "missing_values": [updated_df[c].isna().sum() for c in updated_df.columns]
    })
    
    return updated_df, schema_summary

def save_unified_outputs(client, formatted_zone, updated_df, schema_summary):
    # Save unified outputs in multiple formats.
    timestamp = datetime.now().strftime("%Y_%m_%d_%H:%M")
    os.makedirs("temp_formatted", exist_ok=True)
    
    local_csv = "temp_formatted/all_metadata.csv"
    local_parquet = "temp_formatted/all_metadata.parquet"
    local_json = "temp_formatted/all_metadata.json"
    local_schema = "temp_formatted/schema_summary.csv"
    
    updated_df.to_csv(local_csv, index=False)
    updated_df.to_parquet(local_parquet, index=False)
    updated_df.to_json(local_json, orient="records", lines=True)
    schema_summary.to_csv(local_schema, index=False)
    
    # Upload to MinIO
    client.fput_object(formatted_zone, f"metadata/all_metadata_{timestamp}.csv", local_csv, content_type="text/csv")
    client.fput_object(formatted_zone, f"metadata/all_metadata_{timestamp}.parquet", local_parquet, content_type="application/octet-stream")
    client.fput_object(formatted_zone, f"metadata/all_metadata_{timestamp}.json", local_json, content_type="application/json")
    client.fput_object(formatted_zone, f"metadata/schema_summary_{timestamp}.csv", local_schema, content_type="text/csv")
    
    # Cleanup
    os.remove(local_csv)
    os.remove(local_parquet)
    os.remove(local_json)
    os.remove(local_schema)
    shutil.rmtree("temp_formatted")

# ==============================
#        Configuration
# ==============================
def process_formatted_metadata(
    minio_endpoint = "localhost:9000",
    access_key = "admin",
    secret_key = "password123"):

    landing_zone = "temporal-zone"
    persist_prefix = "persistent_landing"
    formatted_zone = "formatted-zone"
    
    # Setup MinIO client and buckets
    client = setup_minio_client_and_buckets(minio_endpoint, access_key, secret_key, landing_zone, persist_prefix, formatted_zone)
    
    # Load all metadata files from Persistent Landing
    all_dfs = load_metadata_files(client, landing_zone, persist_prefix)
    
    # Normalize schema and combine all metadata
    persistent_df = normalize_and_combine_metadata(all_dfs, persist_prefix)
    
    # Check for existing general metadata in Formatted Zone
    target_columns = [
        "uuid", "kingdom", "phylum", "class", "order", "family",
        "genus", "species", "scientific_name", "common",
        "persistent_path","formatted_path", "image_url"
    ]
    general_df = load_existing_general_metadata(client, formatted_zone, target_columns)
    
    # Merge new rows and prepare final dataset
    updated_df, schema_summary = merge_and_prepare_data(persistent_df, general_df)
    
    # Save unified outputs
    save_unified_outputs(client, formatted_zone, updated_df, schema_summary)
    
    print(" Formatted metadata updated successfully")

process_formatted_metadata();
