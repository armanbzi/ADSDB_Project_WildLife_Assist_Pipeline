"""
-Arman Bazarchi-
Temporal_Landing
here we get the raw data(image and metadata about each image) 
from source (imageomics/TreeOfLife-200M) and store in our temporal-zone.
miniIO user must be "admin", and password "password123".
as data is huge 200m, we get the data by chunks of 1% out of 100% of all data.
so we limit the amount of getting data on each run.
user is asked for a maximum number of species per family, limiting data to store up to a defined  
number of species for each stored family.
user also is asked for a maximum number of observations(images) per each stored species.
also is asked for the chunks interval for the run, for example 5-10 will get from 5th% up to 10th% of the whole 100% of data.
for now only retrieves images of snake families only because of not being able to store a lot of data at the moment.

then code connects to minIO create a temporal-zone bucket and a subbucket 'temporal-landing', 
gets defined interval % of the train split of data,
stores images in a folder 'images' and a csv file of the needed metadata in a folder 'metadata',
it checks and avoids storing duplicates in temporal-zone, also deletes any temporary file and cache
and huggingface heavy cach from local storage.


# Login using e.g. `huggingface-cli login` to access this dataset
"""

from datasets import load_dataset
from datasets import config
import shutil
from minio import Minio
from PIL import Image
from tqdm import tqdm
import io
import pandas as pd
import os
import time
import requests
import re

# ==============================
# Helper Functions
# ==============================

def get_user_parameters():
    """Get user input parameters for data processing with validation."""
    print("Please enter the following parameters:")
    
    # Get MAX_PER_SPECIES with validation
    while True:
        try:
            max_per_species = int(input("MAX_PER_SPECIES (e.g., 30): "))
            if max_per_species > 0:
                break
            else:
                print(" Please enter a positive number greater than 0.")
        except ValueError:
            print(" Please enter a valid number.")
    
    # Get MAX_SPECIES_PER_FAMILY with validation
    while True:
        try:
            max_species_per_family = int(input("MAX_SPECIES_PER_FAMILY (e.g., 11): "))
            if max_species_per_family > 0:
                break
            else:
                print(" Please enter a positive number greater than 0.")
        except ValueError:
            print(" Please enter a valid number.")
    
    return max_per_species, max_species_per_family

def setup_minio_buckets(client, root_bucket, temp_prefix):
    """Setup MinIO buckets and subfolders."""
    # Ensure temporal-landing and temporal-zone exist
    if not client.bucket_exists(root_bucket):
        client.make_bucket(root_bucket)
        print(f" Created top-level bucket: {root_bucket}")
    
    # Create temporal-landing subfolder if missing
    temporal_exists = any(
        obj.object_name.startswith(f"{temp_prefix}/")
        for obj in client.list_objects(root_bucket, recursive=False)
    )
    if not temporal_exists:
        client.put_object(
            root_bucket,
            f"{temp_prefix}/.init",
            data=io.BytesIO(b"init"),
            length=4,
            content_type="text/plain"
        )
        print(f"✅ Created subfolder: {temp_prefix}/ inside {root_bucket}")

def load_existing_metadata(client, root_bucket, metadata_remote_path, metadata_local):
    """Load existing metadata or create empty DataFrame."""
    try:
        client.fget_object(root_bucket, metadata_remote_path, metadata_local)
        metadata_df = pd.read_csv(metadata_local)
        print(f" Loaded metadata from temporal-landing with {len(metadata_df)} records.")
    except Exception:
        metadata_df = pd.DataFrame(columns=[
            "uuid","temporal_path", "kingdom", "phylum", "class", "order", "family",
            "genus", "species", "scientific_name", "common", "image_url"
        ])
        print(" No metadata found in temporal-landing — starting fresh.")
    
    # Maintain fast lookup sets
    existing_uuids = set(metadata_df["uuid"].dropna().unique()) if "uuid" in metadata_df.columns else set()
    return metadata_df, existing_uuids

def scan_existing_images(client, root_bucket, temp_prefix):
    """Scan existing images in temporal-landing."""
    print(" Scanning existing images in temporal-landing...")
    
    existing_image_ids = set()
    for obj in client.list_objects(root_bucket, prefix=f"{temp_prefix}/images/", recursive=True):
        # Extract UUID from filename: temporal-landing/images/<uuid>.jpg
        match = re.match(rf"{temp_prefix}/images/([a-f0-9\-]+)\.jpg", obj.object_name, re.IGNORECASE)
        if match:
            existing_image_ids.add(match.group(1))
    
    print(f" Found {len(existing_image_ids)} existing image files in temporal-landing.")
    return existing_image_ids

def build_family_species_map(metadata_df):
    """Build family -> species map from metadata."""
    family_species = {}
    species_counts = {}
    
    if not metadata_df.empty:
        for _, row in metadata_df.iterrows():
            fam = row.get("family")
            sp = row.get("species")
            if pd.notna(fam) and pd.notna(sp):
                family_species.setdefault(fam, set()).add(sp)
                species_counts[sp] = species_counts.get(sp, 0) + 1
    
    print(f" Tracking {len(family_species)} families from existing metadata.")
    return family_species, species_counts

def is_snake_family(sample, snake_families):
    """Check if sample belongs to snake families."""
    kingdom = sample.get("kingdom")
    cls = sample.get("class")
    family = sample.get("family") or "unknown"
    
    return (
        kingdom and kingdom.strip().lower() == "animalia"
        and cls and cls.strip().lower() == "squamata"
        and family in snake_families
    )

def should_skip_species(species, family, species_counts, family_species, max_per_species, max_species_per_family):
    """Check if species should be skipped based on limits."""
    # Skip if already have enough per this species
    count = species_counts.get(species, 0)
    if count >= max_per_species:
        return True
    
    # Skip if already have enough species for this family
    current_species_in_family = family_species.get(family, set())
    if len(current_species_in_family) >= max_species_per_family and species not in current_species_in_family:
        return True
    
    return False

def download_and_save_image(image_url, img_id, client, root_bucket, temp_prefix, existing_image_ids):
    """Download and save image to MinIO."""
    try:
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        image_bytes = response.content
        
        img = Image.open(io.BytesIO(image_bytes))
        img.verify()
        buffer = io.BytesIO(image_bytes)
        
        # Save to MinIO 
        object_name = f"{temp_prefix}/images/{img_id}.jpg"
        client.put_object(
            root_bucket,
            object_name,
            data=buffer,
            length=len(image_bytes),
            content_type="image/jpeg"
        )
        existing_image_ids.add(img_id)
        return object_name
    except Exception as e:
        raise RuntimeError(f"Error downloading image: {e}")

def save_metadata_record(sample, img_id, object_name, metadata_df, metadata_local, minio_config, existing_uuids, species_counts, family_species, species, family, limits):
    """Save metadata record to CSV and MinIO."""
    if img_id not in existing_uuids:
        # Save needed metadata
        record = {
            "uuid": img_id,
            "temporal_path": object_name,
            "kingdom": sample.get("kingdom"),
            "phylum": sample.get("phylum"),
            "class": sample.get("class"),
            "order": sample.get("order"),
            "family": family,
            "genus": sample.get("genus"),
            "species": species,
            "scientific_name": sample.get("scientific_name"),
            "common": sample.get("common"),
            "image_url": sample.get("source_url")
        }
        
        metadata_df = pd.concat([metadata_df, pd.DataFrame([record])], ignore_index=True)
        metadata_df.to_csv(metadata_local, index=False)
        minio_config['client'].fput_object(minio_config['root_bucket'], minio_config['metadata_remote_path'], metadata_local, content_type="text/csv")
        
        species_counts[species] = species_counts.get(species, 0) + 1
        family_species.setdefault(family, set()).add(species)
        existing_uuids.add(img_id)
        
        print(f" Saved {species} ({species_counts[species]}/{limits['max_per_species']}) "
              f"in family {family} ({len(family_species.get(family, []))}/{limits['max_species_per_family']})")
    
    return metadata_df

def process_sample(sample, snake_families, limits, client, root_bucket, temp_prefix, existing_image_ids, existing_uuids, metadata_df, species_counts, family_species, metadata_local, metadata_remote_path):
    """Process a single sample and return updated metadata_df."""
    species = sample.get("species") or "unknown"
    family = sample.get("family") or "unknown"
    img_id = sample.get("uuid")
    image_url = sample.get("source_url")
    
    # Only save snake images for now
    if not is_snake_family(sample, snake_families):
        return metadata_df
    
    # Skip if limits exceeded
    if should_skip_species(species, family, species_counts, family_species, limits['max_per_species'], limits['max_species_per_family']):
        return metadata_df
    
    try:
        object_name = None
        if img_id not in existing_image_ids:
            object_name = download_and_save_image(image_url, img_id, client, root_bucket, temp_prefix, existing_image_ids)
        
        # Save metadata
        minio_config = {
            'client': client,
            'root_bucket': root_bucket,
            'metadata_remote_path': metadata_remote_path
        }
        metadata_df = save_metadata_record(
            sample, img_id, object_name, metadata_df, metadata_local, 
            minio_config, existing_uuids, species_counts, family_species, 
            species, family, limits)
        
    except Exception as e:
        print(f" Error saving {species}: {e}")
    
    return metadata_df

def process_streaming_data(snake_families, limits, max_total_images, client, root_bucket, temp_prefix, existing_image_ids, existing_uuids, metadata_df, species_counts, family_species, metadata_local, metadata_remote_path):
    """Process streaming data with fixed limit."""
    print(" Processing streaming data...")
    
    try:
        # Stream the dataset (no full download)
        dataset = load_dataset(
            "imageomics/TreeOfLife-200M",
            split="train",
            streaming=True
        )
        
        # Stop after 15% of the dataset (50M samples out of 200M)
        max_samples = 30_000
        
    except Exception as e:
        print(f" Error loading dataset: {e}")
        return metadata_df, False
    
    # Process samples in stream
    for i, sample in enumerate(tqdm(dataset)):
        # Stop after processing max_samples
        if i >= max_samples:
            break
        
        metadata_df = process_sample(
            sample, snake_families, limits, client, root_bucket, temp_prefix, 
            existing_image_ids, existing_uuids, metadata_df, species_counts, 
            family_species, metadata_local, metadata_remote_path
        )
        
        if len(metadata_df) >= max_total_images:
            print(" Reached global image limit.")
            return metadata_df, True
    
    return metadata_df, False

def cleanup_local_files(metadata_local):
    """Clean up local CSV file."""
    if os.path.exists(metadata_local):
        os.remove(metadata_local)
        print(f" Removed local metadata file: {metadata_local}")

# ==============================
# 1 Configuration
# ==============================
def process_landing_zone(
    minio_endpoint = "localhost:9000",
    access_key = "admin",
    secret_key = "password123"):

    ROOT_BUCKET = "temporal-zone"         
    TEMP_PREFIX = "temporal-landing"     # Subbucket for temporal
    
    # Metadata config
    METADATA_REMOTE_PATH = f"{TEMP_PREFIX}/metadata/metadata_final.csv"
    METADATA_LOCAL = "metadata_final.csv"
    
    # Get user parameters
    MAX_PER_SPECIES, MAX_SPECIES_PER_FAMILY = get_user_parameters()
    
    # Global Limit
    MAX_TOTAL_IMAGES = 100000
    
    # Known snake families
    SNAKE_FAMILIES = {
        "Viperidae", "Elapidae", "Colubridae", "Pythonidae", "Boidae",
        "Typhlopidae", "Lamprophiidae", "Natricidae", "Dipsadidae",
        "Leptotyphlopidae", "Xenopeltidae", "Anomalepididae", "Loxocemidae",
        "Uropeltidae", "Cylindrophiidae", "Aniliidae", "Acrochhordidae",
        "Anomochilidae", "Atractaspididae", "Bolyeridae"
    }
    
    # Initialize MinIO client
    client = Minio(minio_endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    
    # Setup buckets
    setup_minio_buckets(client, ROOT_BUCKET, TEMP_PREFIX)
    
    # Load existing metadata
    metadata_df, existing_uuids = load_existing_metadata(client, ROOT_BUCKET, METADATA_REMOTE_PATH, METADATA_LOCAL)
    
    # Scan existing images
    existing_image_ids = scan_existing_images(client, ROOT_BUCKET, TEMP_PREFIX)
    
    # Build family species map
    family_species, species_counts = build_family_species_map(metadata_df)
    
    # Process streaming data
    limits = {'max_per_species': MAX_PER_SPECIES, 'max_species_per_family': MAX_SPECIES_PER_FAMILY}
    metadata_df, _ = process_streaming_data(
        SNAKE_FAMILIES, limits, MAX_TOTAL_IMAGES,
        client, ROOT_BUCKET, TEMP_PREFIX, existing_image_ids, existing_uuids,
        metadata_df, species_counts, family_species, METADATA_LOCAL, METADATA_REMOTE_PATH)
    
    print(f" Finished progressive sampling. Total images: {len(metadata_df)}")
    
    # Cleanup
    cleanup_local_files(METADATA_LOCAL)

process_landing_zone();

import os
import shutil
import tempfile
import subprocess
import time
from datasets import config

"""
  Here we notice that getting data from huggingface stores heavy size of cache of that dataset on local storage.
  so we try to safely remove this cache and free up space.
"""


# However some binary files from online huggingface hub stay in locat storage
