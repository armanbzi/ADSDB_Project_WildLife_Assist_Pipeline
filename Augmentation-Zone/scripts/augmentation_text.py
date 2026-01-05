"""
-Arman Bazarchi-
Fine-Tuning Zone — Text Augmentation


This module implements text data augmentation for CLIP model fine-tuning,
directly addressing Constraint 3 (Data Augmentation) from the ADSDB project.

Text augmentation is crucial for multimodal contrastive learning because:

1. **Query Diversity**: Users search using various phrasings ("show me a cobra",
   "what is Naja naja", "cobra snake photo"). The model must learn that these
   semantically equivalent queries should retrieve similar images.

2. **Robustness**: CLIP's zero-shot capabilities depend on understanding natural
   language variations. Augmenting training text improves generalization.

3. **Domain Adaptation**: Scientific nomenclature (genus, species) differs from
   common names. Template variations help bridge this gap.

METHODOLOGY: TEMPLATE-BASED AUGMENTATION
--------------------------------------------------------------------------------
We employ template-based text augmentation rather than neural paraphrasing:

**Rationale for Template Approach:**
- Deterministic and reproducible (Constraint 9: Reproducibility)
- No additional model dependencies or computational overhead
- Controlled vocabulary ensures domain relevance
- Mimics realistic user search patterns

**Template Categories:**
1. Photo/Image queries: "A photo of {species}"
2. Question queries: "What is {species}?"
3. Command queries: "Show me {species}", "Find {species}"
4. Information queries: "Information about {species}"
5. Direct name queries: Just common name or scientific name

"""

import os
import json
import io
import random
from datetime import datetime
from typing import List, Dict
import pandas as pd
from minio import Minio
from minio.error import S3Error


def process_text_augmentation(
    minio_host="localhost:9000",
    access_key="admin",
    secret_key="password123",
    num_rows=None,
    num_variations_per_row=None):
    
    # Main entry point for text augmentation processing.
    
    # WORKFLOW
    # 1. Connect to MinIO and validate zones exist
    # 2. Clear existing augmented metadata (ensures idempotent execution)
    # 3. Load metadata from trusted-zone
    # 4. Randomly sample specified number of rows
    # 5. Apply template variations to each row
    # 6. Save augmented metadata to augmentation-zone
    
    # DATA MANAGEMENT STRATEGY
    
    # Per Constraint 4 (New Zones), augmented data is stored separately from
    # original trusted-zone data. This separation enables:
    #- Clear data lineage tracking
    # - Easy rollback if augmentation parameters change
    # - Flexible mixing ratios during training
    
    # Args:
    #    minio_host: MinIO endpoint address
    #     access_key: MinIO access credentials
    #     secret_key: MinIO secret credentials
    #     num_rows: Number of metadata rows to sample (None = interactive prompt)
    #     num_variations_per_row: Variations per row (None = interactive prompt)
    
    
    print("=" * 60)
    print(" Text Augmentation Processing")
    print("=" * 60)
    
    # Initialize MinIO client and validate bucket structure
    client = _initialize_minio_client(minio_host, access_key, secret_key)
    
    # Clear previous augmentation to ensure clean state (idempotency)
    print("\n Cleaning augmentation-zone...")
    _clear_augmentation_zone_metadata(client)
    
    # Interactive configuration if parameters not provided
    if num_rows is None:
        print("\n Configuration:")
        num_rows_input = input("  How many rows to retrieve from trusted-zone? [100]: ").strip()
        try:
            num_rows = int(num_rows_input) if num_rows_input else 100
        except ValueError:
            print("  Invalid input, using default: 100")
            num_rows = 100
    
    if num_variations_per_row is None:
        num_variations_input = input("  How many template variations per row? [3]: ").strip()
        try:
            num_variations_per_row = int(num_variations_input) if num_variations_input else 3
        except ValueError:
            print("  Invalid input, using default: 3")
            num_variations_per_row = 3
    
    print(f"\n Selected: {num_rows} rows, {num_variations_per_row} variations per row")
    print(f" Expected output: {num_rows * num_variations_per_row} augmented metadata records")
    
    # Load source metadata from trusted-zone
    print("\n Loading metadata from trusted-zone...")
    metadata_df = _load_metadata_from_trusted(client)
    
    if metadata_df is None or len(metadata_df) == 0:
        raise SystemExit(" No metadata found in trusted-zone. Cannot proceed.")
    
    print(f" Loaded {len(metadata_df)} total metadata records")
    
    # Random sampling for diversity
    # This ensures augmentation covers the full data distribution
    if len(metadata_df) > num_rows:
        selected_indices = random.sample(range(len(metadata_df)), num_rows)
        selected_df = metadata_df.iloc[selected_indices].copy()
        print(f" Randomly selected {num_rows} rows for augmentation")
    else:
        selected_df = metadata_df.copy()
        print(f" Using all {len(selected_df)} available rows")
        num_rows = len(selected_df)
    
    # Apply text template variations
    print(f"\n Applying {num_variations_per_row} random template variations to each row...")
    augmented_metadata = _apply_text_augmentations(selected_df, num_variations_per_row)
    
    print(f" Generated {len(augmented_metadata)} augmented metadata records")
    
    # Persist augmented data to augmentation-zone
    print("\n Saving augmented metadata to augmentation-zone...")
    _save_metadata_to_augmentation(client, augmented_metadata)
    
    print("\n" + "=" * 60)
    print(" Text augmentation completed successfully!")
    print(f" Saved {len(augmented_metadata)} augmented metadata records to augmentation-zone")
    print("=" * 60)


def _initialize_minio_client(minio_host, access_key, secret_key):
    # Initialize MinIO client and ensure required buckets exist.
    
    # ZONE ARCHITECTURE (Constraint 4)
    
    # - trusted-zone: Source of curated, validated data (required)
    # - augmentation-zone: Destination for augmented data (created if missing)
    
    # This follows the data lake zone pattern where each zone has a specific
    # purpose and data flows in one direction through the pipeline.
    
    # Returns:
    #     Minio: Configured MinIO client instance

    client = Minio(minio_host, access_key=access_key, secret_key=secret_key, secure=False)
    
    # Validate trusted-zone exists (prerequisite)
    trusted_zone = "trusted-zone"
    if not client.bucket_exists(trusted_zone):
        raise SystemExit(f" Trusted zone bucket '{trusted_zone}' does not exist. Cannot continue.")
    
    # Create augmentation-zone if needed
    augmentation_zone = "augmentation-zone"
    if not client.bucket_exists(augmentation_zone):
        print(f" Creating augmentation-zone bucket...")
        client.make_bucket(augmentation_zone)
        print(f" ✓ Created augmentation-zone bucket")
    else:
        print(f" ✓ Augmentation-zone bucket exists")
    
    print(f" Connected to MinIO: {minio_host}")
    return client


def _load_metadata_from_trusted(client):

    # Load metadata CSV from trusted-zone.
    
    # DATA LINEAGE
    # The trusted-zone contains validated, curated metadata that has passed
    # through the Temporal → Persistent → Formatted → Trusted pipeline.
    # This ensures augmentation operates on high-quality source data.
    
    # Returns:
    #     pd.DataFrame: Metadata DataFrame or None if not found

    trusted_zone = "trusted-zone"
    
    # Find all metadata CSV files
    metadata_objs = [
        obj.object_name for obj in client.list_objects(trusted_zone, prefix="metadata/", recursive=True)
        if obj.object_name.lower().endswith(".csv")
    ]
    
    if not metadata_objs:
        return None
    
    # Use latest metadata file (sorted by name, assumes timestamp suffix)
    metadata_objs.sort(reverse=True)
    latest_meta = metadata_objs[0]
    
    print(f" Loading metadata from: {latest_meta}")
    
    # Download and parse CSV
    resp = client.get_object(trusted_zone, latest_meta)
    data = resp.read()
    resp.close()
    resp.release_conn()
    
    metadata_df = pd.read_csv(io.BytesIO(data))
    
    return metadata_df


def _get_available_template_variations():

    # Return list of available text template variation identifiers.
    
    # TEMPLATE DESIGN RATIONALE

    # Templates are designed to mimic realistic user query patterns:
    
    # 1. **Photo/Image Templates**: Common for image search
       #- "A photo of {X}" - Natural phrasing
    
    # 2. **Question Templates**: Information-seeking queries
       #- "What is {X}?" - Identification queries
    
    # 3. **Command Templates**: Action-oriented queries
       #- "Show me {X}", "Find {X}" - Direct commands
    
    # 4. **Direct Name Templates**: Simple lookups
       #- Just the common name or scientific name
    
    # This diversity helps CLIP learn that semantically equivalent queries
    # (e.g., "Show me a cobra" vs "Naja naja") should map to similar embeddings.
    
    # Returns:
    #    List[str]: Template function identifiers
    
    return [
        'template_photo_of',
        'template_what_is',
        'template_show_me',
        'template_i_need',
        'template_looking_for',
        'template_find',
        'template_search',
        'template_identify',
        'template_information_about',
        'template_details_of',
        'template_common_name_only',
        'template_scientific_name_only'
    ]


def _apply_text_augmentations(metadata_df, num_variations=3):
    
    # Apply text template variations to metadata.
    
    # AUGMENTATION STRATEGY
    # ---------------------
    # For each metadata row:
    # 1. Extract taxonomic information (common name, scientific name, etc.)
    # 2. Randomly select N template variations (without replacement)
    # 3. Generate augmented text for each variation
    # 4. Preserve all original metadata columns for training
    
    # RANDOM SEED (Constraint 9: Reproducibility)
    # -------------------------------------------
    # Fixed seed (42) ensures:
    # - Same rows get same template assignments across runs
    # - Experiments can be reproduced exactly
    # - Debugging is deterministic
    
    # Args:
    #    metadata_df: DataFrame with original metadata
    #   num_variations: Number of template variations per row
        
    # Returns:
    #    List[Dict]: Augmented metadata records with 'text' field added
    
    augmented_records = []
    available_variations = _get_available_template_variations()
    
    # Set random seed for reproducibility
    random.seed(42)
    
    for _, row in metadata_df.iterrows():
        # Extract taxonomic fields, handling missing values
        common_name = str(row.get('common', '')).strip() if pd.notna(row.get('common')) else ''
        scientific_name = str(row.get('scientific_name', '')).strip() if pd.notna(row.get('scientific_name')) else ''
        family = str(row.get('family', '')).strip() if pd.notna(row.get('family')) else ''
        genus = str(row.get('genus', '')).strip() if pd.notna(row.get('genus')) else ''
        species = str(row.get('species', '')).strip() if pd.notna(row.get('species')) else ''
        
        # Skip rows without usable name information
        if not common_name and not scientific_name:
            continue
        
        # Random template selection (without replacement for diversity)
        selected_variations = random.sample(available_variations, min(num_variations, len(available_variations)))
        
        # Generate augmented record for each variation
        for variation_name in selected_variations:
            augmented_text = _apply_template_variation(
                common_name, scientific_name, family, genus, species, variation_name
            )
            
            if augmented_text:
                # Create augmented record preserving all original columns
                augmented_record = row.to_dict()
                augmented_record['text'] = augmented_text
                augmented_record['_augmentation_type'] = variation_name
                augmented_records.append(augmented_record)
    
    return augmented_records


def _apply_template_variation(common_name, scientific_name, family, genus, species, variation_name):
    
    # Apply a single template variation to create user-like search text.
    
    # TEMPLATE IMPLEMENTATION
    # -----------------------
    # Each template generates text that mimics how users naturally phrase
    # queries when searching for species information. This is critical for
    # CLIP fine-tuning because:
    
    # 1. **Contrastive Learning**: CLIP learns by contrasting positive pairs
    #    (matching image-text) against negatives. Diverse text representations
    #    help the model learn robust text-image alignments.
    
    # 2. **Zero-Shot Transfer**: Better text understanding enables the model
    #    to handle novel queries at inference time.
    
    # 3. **Domain-Specific Language**: Templates include both scientific
    #    nomenclature and common names, bridging expert and casual user needs.
    
    # Args:
    #    common_name: Species common name (e.g., "Indian Cobra")
    #    scientific_name: Binomial nomenclature (e.g., "Naja naja")
    #    family: Taxonomic family (e.g., "Elapidae")
    #    genus: Genus name (e.g., "Naja")
    #    species: Species epithet (e.g., "naja")
    #    variation_name: Template identifier
        
    # Returns:
    #    str: Augmented text or None if template cannot be applied
    
    # Primary name: prefer common name for natural queries
    primary_name = common_name if common_name else scientific_name
    
    if not primary_name:
        return None
    
    # Template implementations
    if variation_name == 'template_photo_of':
        # Classic CLIP-style prompt: "A photo of {X}"
        return f"A photo of {primary_name}"
    
    elif variation_name == 'template_what_is':
        # Question format for identification
        return f"What is {primary_name}?"
    
    elif variation_name == 'template_show_me':
        # Command format common in search interfaces
        return f"Show me {primary_name}"
    
    
    elif variation_name == 'template_looking_for':
        # Natural search phrasing
        return f"Looking for {primary_name}"
    
    elif variation_name == 'template_find':
        # Direct command
        return f"Find {primary_name}"
    
    elif variation_name == 'template_search':
        # Explicit search intent
        return f"Search for {primary_name}"
    
    elif variation_name == 'template_identify':
        # Identification task phrasing
        return f"Identify {primary_name}"
    
    elif variation_name == 'template_information_about':
        # Information-seeking query
        return f"Information about {primary_name}"
    
    elif variation_name == 'template_details_of':
        # Detailed information request
        return f"Details of {primary_name}"
    
    elif variation_name == 'template_common_name_only':
        # Direct common name (most natural user input)
        if common_name:
            return common_name
        return None
    
    elif variation_name == 'template_scientific_name_only':
        # Direct scientific name (expert user input)
        if scientific_name:
            return scientific_name
        return None

    
    else:
        return None


def _clear_augmentation_zone_metadata(client):
    # Delete all existing augmented text metadata from augmentation-zone.
    
    # IDEMPOTENCY PRINCIPLE
    # Clearing previous augmentation before generating new data ensures:
    # 1. Consistent state regardless of prior runs
    # 2. No accumulation of stale data
    # 3. Clean experiments without manual cleanup
    
    # Only text augmentation files (containing 'augmentation_text') are removed,
    # preserving image augmentation data for independent control.
    augmentation_zone = "augmentation-zone"
    
    if not client.bucket_exists(augmentation_zone):
        print("  Augmentation-zone does not exist. Nothing to clear.")
        return
    
    try:
        # Target only text augmentation metadata
        metadata_prefix = "metadata/"
        objects_to_delete = [
            obj for obj in client.list_objects(augmentation_zone, prefix=metadata_prefix, recursive=True)
            if "augmentation_text" in obj.object_name.lower()
        ]
        
        if not objects_to_delete:
            print("  No existing augmented metadata found in augmentation-zone.")
            return
        
        print(f"  Found {len(objects_to_delete)} existing augmented metadata files to delete...")
        
        # Delete each object
        deleted_count = 0
        for obj in objects_to_delete:
            try:
                client.remove_object(augmentation_zone, obj.object_name)
                deleted_count += 1
            except Exception as e:
                print(f"  Warning: Could not delete {obj.object_name}: {e}")
        
        print(f"  ✓ Deleted {deleted_count} existing augmented metadata files")
        
    except Exception as e:
        print(f"  Warning: Error clearing augmentation-zone: {e}")


def _save_metadata_to_augmentation(client, augmented_metadata):

    # Save augmented metadata to augmentation-zone.
    
    # FILE NAMING CONVENTION 
    # Format: metadata/augmentation_text_{timestamp}.csv
    
    #- 'augmentation_text' prefix identifies the data type
    # - Timestamp ensures uniqueness and enables temporal tracking
    # - CSV format for interoperability with pandas/other tools
    
    # PRESERVED COLUMNS

    # All original metadata columns are preserved alongside the new 'text' field.
    # This enables the fine-tuning script to:
    # - Link augmented text to original images via UUID
    # - Use taxonomic info for stratified sampling
    # - Track data lineage back to trusted-zone
    
    # Args:
    #   client: MinIO client instance
    #    augmented_metadata: List of augmented record dictionaries
    
    augmentation_zone = "augmentation-zone"
    
    # Convert to DataFrame for CSV serialization
    augmented_df = pd.DataFrame(augmented_metadata)
    
    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"metadata/augmentation_text_{timestamp}.csv"
    
    # Serialize to CSV bytes
    csv_buffer = io.BytesIO()
    augmented_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Upload to MinIO
    try:
        client.put_object(
            augmentation_zone,
            filename,
            csv_buffer,
            length=csv_buffer.getbuffer().nbytes,
            content_type="text/csv"
        )
        print(f" ✓ Saved augmented metadata to: {filename}")
        print(f"   Total records: {len(augmented_metadata)}")
    except S3Error as e:
        raise SystemExit(f" Error saving augmented metadata: {e}")


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    # Configuration from environment variables (supports containerized execution)
    minio_host = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'admin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'password123')
    
    # Environment variables for non-interactive execution (e.g., CI/CD pipelines)
    num_rows = None
    num_variations = None
    
    if 'AUGMENTATION_NUM_ROWS' in os.environ:
        num_rows = int(os.getenv('AUGMENTATION_NUM_ROWS', '100'))
    if 'AUGMENTATION_NUM_VARIATIONS' in os.environ:
        num_variations = int(os.getenv('AUGMENTATION_NUM_VARIATIONS', '3'))
    
    process_text_augmentation(
        minio_host=minio_host,
        access_key=access_key,
        secret_key=secret_key,
        num_rows=num_rows,
        num_variations_per_row=num_variations
    )
