"""
-Arman Bazarchi-
Temporal_Landing
here we get the raw data(image and metadata about each image) 
from source (imageomics/TreeOfLife-200M) and store in our temporal-zone.
asks user for minIO configurations.
as data is huge 200m, we choose streaming it rather than storing heavy data in local storage and then process.
and we define a maximum samples for each run to define how many samples of the main dataset(200m) to iterate.
user is asked for a max samples, maximum number of species per family, limiting data to store up to a defined  
number of species for each stored family.
user also is asked for a maximum number of observations(images) per each stored species.
for now only retrieves images of snake families only because of not being able to store a lot of data at the moment.

then code connects to minIO create a temporal-zone bucket and a subbucket 'temporal-landing', 
stores images in a folder 'images' and a csv file of the needed metadata in a folder 'metadata',
it checks and avoids storing duplicates in temporal-zone, 

we use streaming mode to retrieve data we need, because data is huge and is not best option
to store in local storage and then filter to store

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
import threading
import atexit
import signal
import sys

# ==============================
#          Functions
# ==============================

def get_user_parameters():
    # Get user input parameters for data processing with validation control.
    # Supports both interactive and non-interactive (CI/CD) modes.
    
    import os
    import sys
    
    # Check if running in non-interactive mode (CI/CD)
    # When TEMPORAL_PARAMS is set, use those parameters (orchestrator mode)
    # When CI/GITHUB_ACTIONS/GITLAB_CI is set, use defaults (true CI/CD mode)
    temporal_params = os.getenv('TEMPORAL_PARAMS')
    
    is_non_interactive = (
        temporal_params is not None or  # Orchestrator passed parameters
        os.getenv('CI') == 'true' or  # GitHub Actions
        os.getenv('GITHUB_ACTIONS') == 'true' or  # GitHub Actions
        os.getenv('GITLAB_CI') == 'true' or  # GitLab CI
        '--non-interactive' in sys.argv  # Command line flag
    )
    
    # Don't check sys.stdin.isatty() as it can cause issues in some environments
    
    if is_non_interactive:
        # Use environment variables or defaults for CI/CD
        if temporal_params:
            print("Running in orchestrator mode - using provided parameters")
            try:
                params = temporal_params.split(',')
                max_per_species = int(params[0].strip())
                max_species_per_family = int(params[1].strip())
                max_samples = int(params[2].strip())
            except (ValueError, IndexError):
                print(f"Warning: Invalid TEMPORAL_PARAMS format '{temporal_params}', using defaults")
                max_per_species = 30
                max_species_per_family = 11
                max_samples = 300000
        else:
            print("Running in CI/CD mode - using environment variables or defaults")
            # Parse consolidated temporal parameters
            temporal_params_str = os.getenv('TEMPORAL_PARAMS', '30,11,3000000')
            try:
                params = temporal_params_str.split(',')
                max_per_species = int(params[0].strip())
                max_species_per_family = int(params[1].strip())
                max_samples = int(params[2].strip())
            except (ValueError, IndexError):
                print(f"Warning: Invalid TEMPORAL_PARAMS format '{temporal_params_str}', using defaults")
                max_per_species = 30
                max_species_per_family = 11
                max_samples = 3000000
        
        print(f"Using parameters: MAX_PER_SPECIES={max_per_species}, MAX_SPECIES_PER_FAMILY={max_species_per_family}, MAX_SAMPLES={max_samples}")
        
        return max_per_species, max_species_per_family, max_samples
    
    # Interactive mode - get user input
    print("Running in interactive mode - please enter the following parameters:")
    
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
    
    # Get MAX_SAMPLES with validation
    while True:
        try:
            max_samples = int(input("MAX_SAMPLES (e.g., 300000): "))
            if max_samples > 0:
                break
            else:
                print(" Please enter a positive number greater than 0.")
        except ValueError:
            print(" Please enter a valid number.")
    
    return max_per_species, max_species_per_family, max_samples

def setup_minio_buckets(client, root_bucket, temp_prefix):
    # Setup MinIO buckets and subfolders.
    
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
        print(f" Created subfolder: {temp_prefix}/ inside {root_bucket}")

def load_existing_metadata(client, root_bucket, metadata_remote_path, metadata_local):
    # Load existing metadata or create empty DataFrame if no existing available.
    
    try:
        client.fget_object(root_bucket, metadata_remote_path, metadata_local)
        metadata_df = pd.read_csv(metadata_local)
        print(f" Loaded metadata from temporal-landing with {len(metadata_df)} records.")
    except Exception:
        metadata_df = pd.DataFrame(columns=[
            "uuid","temporal_path", "kingdom", "phylum", "class", "order", "family",
            "genus", "species", "scientific_name", "common", "image_url"
        ])
        print(" No metadata found in temporal-landing - starting fresh.")
    
    # Maintain fast lookup sets
    existing_uuids = set(metadata_df["uuid"].dropna().unique()) if "uuid" in metadata_df.columns else set()
    return metadata_df, existing_uuids

def scan_existing_images(client, root_bucket, temp_prefix):
    # Scan existing images in temporal-landing.
    
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
    # Build family -> species map from metadata.
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
    # Check if sample belongs to snake families.
    
    kingdom = sample.get("kingdom")
    cls = sample.get("class")
    family = sample.get("family") or "unknown"
    
    return (
        kingdom and kingdom.strip().lower() == "animalia"
        and cls and cls.strip().lower() == "squamata"
        and family in snake_families
    )

def skip_species(species, family, species_counts, family_species, max_per_species, max_species_per_family):
    # Check if species should be skipped based on limits.
    
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
    # Download and save image to MinIO.
    
    try:
        # Configure requests to handle SSL certificate issues
        import ssl
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        response = requests.get(
            image_url, 
            timeout=10,
            verify=False,  # Disable SSL verification to handle certificate issues
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        )
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
    # Save metadata record to CSV and MinIO.
    
    if img_id not in existing_uuids:
        # Save needed columns
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

def process_sample(sample, snake_families, limits, client, root_bucket, temp_prefix, 
                   existing_image_ids, existing_uuids, metadata_df, species_counts, family_species, 
                   metadata_local, metadata_remote_path):
    
    # Process a single sample and return updated metadata_df.
    species = sample.get("species") or "unknown"
    family = sample.get("family") or "unknown"
    img_id = sample.get("uuid")
    image_url = sample.get("source_url")
    
    # Only save snake images for now
    if not is_snake_family(sample, snake_families):
        return metadata_df
    
    # Skip if limits exceeded
    if skip_species(species, family, species_counts, family_species, limits['max_per_species'], limits['max_species_per_family']):
        return metadata_df
    
    try:
        object_name = None
        if img_id not in existing_image_ids:
            # Download and save image - skip if fails, don't crash
            try:
                object_name = download_and_save_image(image_url, img_id, client, root_bucket, temp_prefix, existing_image_ids)
            except Exception as download_error:
                print(f"Skipping image download for {species} (failed): {download_error}")
                # Continue without the image - don't crash the entire process
                return metadata_df
        
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
        print(f"Skipping sample {species} due to error: {e}")
        # Continue processing other samples instead of crashing
        return metadata_df
    
    return metadata_df

def process_streaming_data(snake_families, limits, max_samples, client, root_bucket, temp_prefix, existing_image_ids, existing_uuids, metadata_df, species_counts, family_species, metadata_local, metadata_remote_path):
    # Process streaming data with fixed limit.
    # connecting to dataset through imagomics to get raw data.
    
    print(" Processing streaming data...")
    
    try:
        # Stream the dataset (no full download)
        dataset = load_dataset(
            "imageomics/TreeOfLife-200M",
            split="train",
            streaming=True,
            trust_remote_code=False  # Disable remote code execution
        )
        
        # Use user-provided max_samples limit
        print(f" Processing up to {max_samples} samples from the dataset...")
        
    except Exception as e:
        print(f" Error loading dataset: {e}")
        return metadata_df, False
    
    # Process samples in stream with error handling
    processed_count = 0
    
    try:
        for i, sample in enumerate(tqdm(dataset)):
            # Stop after processing max_samples
            if i >= max_samples:
                break
            
            try:
                metadata_df = process_sample(
                    sample, snake_families, limits, client, root_bucket, temp_prefix, 
                    existing_image_ids, existing_uuids, metadata_df, species_counts, 
                    family_species, metadata_local, metadata_remote_path
                )
                processed_count += 1
            except Exception as e:
                print(f"Skipping sample {i} due to error: {e}")
                # Continue processing other samples instead of crashing
                continue
    except Exception as e:
        print(f"Error during dataset iteration: {e}")
        print("Continuing with partial results...")
        # Don't crash - return what we have processed so far
    finally:
        # Ensure dataset cleanup happens
        try:
            del dataset
        except:
            pass
    
    print(f"Processing completed: {processed_count} samples processed")
    
    return metadata_df, False

def cleanup_local_files(metadata_local):
    """Clean up local CSV file."""
    if os.path.exists(metadata_local):
        os.remove(metadata_local)
        print(f" Removed local metadata file: {metadata_local}")

def get_minio_config():
    # Load MinIO configuration from environment variables (set by orchestrator).
    
    import os
    
    # Get configuration from environment variables (set by orchestrator)
    endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'admin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'admin123')
    
    print(f"Using MinIO configuration from environment variables: endpoint={endpoint}, access_key={access_key[:3]}***")
    return endpoint, access_key, secret_key

def cleanup_threads():
    """Clean up any remaining threads to prevent GIL errors."""
    try:
        # Wait for all non-daemon threads to complete
        for thread in threading.enumerate():
            if thread != threading.current_thread() and thread.is_alive():
                if not thread.daemon:
                    thread.join(timeout=1.0)
        
        # Force garbage collection to clean up any remaining references
        import gc
        gc.collect()
        
        # Additional cleanup for libraries that might cause GIL issues
        try:
            # Clean up any remaining requests sessions
            import requests
            requests.Session().close()
        except:
            pass
        
        try:
            # Clean up PIL/Pillow resources
            from PIL import Image
            Image._show = lambda *args, **kwargs: None  # Disable image display
        except:
            pass
        
        print(" Thread cleanup completed successfully")
    except Exception as e:
        print(f" Warning during thread cleanup: {e}")

# ==============================
#        Configuration
# ==============================
def process_temporal(
    minio_endpoint = "localhost:9000",
    access_key = "admin",
    secret_key = "password123"):

    print("Starting Temporal Landing Process...")
    
    # Get MinIO configuration from environment variables (set by orchestrator)
    minio_endpoint, access_key, secret_key = get_minio_config()
    
    ROOT_BUCKET = "temporal-zone"         
    TEMP_PREFIX = "temporal-landing"     # Subbucket for temporal
    
    # Metadata config
    METADATA_REMOTE_PATH = f"{TEMP_PREFIX}/metadata/metadata_final.csv"
    METADATA_LOCAL = "metadata_final.csv"
    
    print("Getting user parameters...")
    # Get user parameters
    MAX_PER_SPECIES, MAX_SPECIES_PER_FAMILY, MAX_SAMPLES = get_user_parameters()
    print(f"Parameters received: MAX_PER_SPECIES={MAX_PER_SPECIES}, MAX_SPECIES_PER_FAMILY={MAX_SPECIES_PER_FAMILY}, MAX_SAMPLES={MAX_SAMPLES}")
    
    print("Initializing snake families...")
    # Known snake families
    SNAKE_FAMILIES = {
        "Viperidae", "Elapidae", "Colubridae", "Pythonidae", "Boidae",
        "Typhlopidae", "Lamprophiidae", "Natricidae", "Dipsadidae",
        "Leptotyphlopidae", "Xenopeltidae", "Anomalepididae", "Loxocemidae",
        "Uropeltidae", "Cylindrophiidae", "Aniliidae", "Acrochhordidae",
        "Anomochilidae", "Atractaspididae", "Bolyeridae"
    }
    
    print("Initializing MinIO connection...")
    # Initialize MinIO client (lazy connection - only connect when needed)
    print(f"Preparing MinIO client for {minio_endpoint}...")
    
    client = Minio(minio_endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    print("MinIO client prepared (connection will be tested when needed)")
    
    print("Setting up MinIO buckets...")
    # Setup buckets
    setup_minio_buckets(client, ROOT_BUCKET, TEMP_PREFIX)
    
    print("Loading existing metadata...")
    # Load existing metadata
    metadata_df, existing_uuids = load_existing_metadata(client, ROOT_BUCKET, METADATA_REMOTE_PATH, METADATA_LOCAL)
    
    print("Scanning existing images...")
    # Scan existing images
    existing_image_ids = scan_existing_images(client, ROOT_BUCKET, TEMP_PREFIX)
    
    print("Building family species map...")
    # Build family species map
    family_species, species_counts = build_family_species_map(metadata_df)
    
    print("Starting data processing...")
    # Process streaming data
    limits = {'max_per_species': MAX_PER_SPECIES, 'max_species_per_family': MAX_SPECIES_PER_FAMILY}
    metadata_df, _ = process_streaming_data(
        SNAKE_FAMILIES, limits, MAX_SAMPLES,
        client, ROOT_BUCKET, TEMP_PREFIX, existing_image_ids, existing_uuids,
        metadata_df, species_counts, family_species, METADATA_LOCAL, METADATA_REMOTE_PATH)
    
    print(f" Finished progressive sampling. Total images: {len(metadata_df)}")
    
    print("Cleaning up local files...")
    # Cleanup
    cleanup_local_files(METADATA_LOCAL)
    
    print("Cleaning up...")
    # Cleanup
    cleanup_threads()
    
    print("Temporal Landing Process completed successfully!")

if __name__ == "__main__":
    try:
        process_temporal()
        print(" Temporal processing completed successfully")
    except KeyboardInterrupt:
        print("\n Process interrupted by user")
    except Exception as e:
        print(f"\n Error in temporal processing: {e}")
        raise  # Re-raise the exception so orchestrator can handle it
    finally:
        # Ensure cleanup happens even if there's an error
        cleanup_threads()


