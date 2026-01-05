"""
-Arman Bazarchi-
Fine-Tuning Zone — CLIP Fine-Tuning with LoRA/QLoRA

This script implements parameter-efficient fine-tuning of CLIP using LoRA 
(Low-Rank Adaptation) and QLoRA (Quantized LoRA). Addresses Constraint 1 
(Fine-Tuning) from the ADSDB project guidelines.


FINE-TUNING METHODOLOGY 
================================================================================
LoRA (Low-Rank Adaptation):
- Instead of updating all model parameters W, we learn low-rank matrices A and B
- The update is: W' = W + BA where B∈R^(d×r), A∈R^(r×k), and r << min(d,k)
- This reduces trainable parameters from d×k to r×(d+k), enabling efficient training

QLoRA (Quantized LoRA):
- Combines 4-bit quantization with LoRA for further memory reduction
- Base model stored in 4-bit (8x reduction), adapters trained in FP16
- Enables fine-tuning of large models on consumer hardware

Contrastive Loss (InfoNCE):
- CLIP uses contrastive learning to align image-text pairs in embedding space
- For a batch of N pairs, the loss maximizes similarity of matching pairs
- while minimizing similarity to N-1 negative pairs per sample


TRAINING DATA 
================================================================================
Training data is loaded from:
- trusted-zone: Original images and metadata
- augmentation-zone: Augmented images and text variations (Constraint 3)
"""

import os
import json
import io
import random
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from PIL import Image
from minio import Minio
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from tqdm import tqdm

# ============================================================================
# LIBRARY IMPORTS WITH COMPATIBILITY HANDLING
# ============================================================================

# Workaround for huggingface_hub compatibility
try:
    import huggingface_hub
    if not hasattr(huggingface_hub, 'HfFileSystem'):
        class HfFileSystem:
            def __init__(self, *args, **kwargs): pass
            def __getattr__(self, name): return lambda *args, **kwargs: None
        huggingface_hub.HfFileSystem = HfFileSystem
except Exception:
    pass

from peft import LoraConfig, get_peft_model, TaskType, prepare_model_for_kbit_training
from transformers import CLIPModel, CLIPProcessor, BitsAndBytesConfig

try:
    import bitsandbytes as bnb
except ImportError:
    bnb = None

# ============================================================================
# REPRODUCIBILITY
# ============================================================================
random.seed(42)
np.random.seed(42)
torch.manual_seed(42)

# ============================================================================
# TRUSTED ZONE METADATA COLUMNS (preserved in training-zone)
# ============================================================================
TRUSTED_ZONE_COLUMNS = [
    "uuid", "kingdom", "phylum", "class", "order", "family",
    "genus", "species", "scientific_name", "common",
    "persistent_path", "formatted_path", "image_url"
]

# ============================================================================
# AUGMENTATION TYPES
# ============================================================================
IMAGE_AUGMENTATION_TYPES = [
    'random_resized_crop', 'center_crop', 'rotation', 'translation',
    'brightness_jitter', 'contrast_jitter', 'saturation_jitter',
    'gaussian_noise', 'gaussian_blur', 'partial_crop'
]

TEXT_AUGMENTATION_TYPES = [
    'template_photo_of', 'template_what_is', 'template_show_me',
    'template_i_need', 'template_looking_for', 'template_find',
    'template_search', 'template_identify', 'template_information_about',
    'template_details_of', 'template_common_name_only', 'template_scientific_name_only'
]

# ============================================================================
# DATA ZONE VALIDATION
# ============================================================================
def _validate_data_zones(client):
    # Validate that trusted-zone and augmentation-zone exist and contain data.
    # Returns:
    #     Tuple of (is_valid, error_message, zone_stats)
    trusted_zone = "trusted-zone"
    augmentation_zone = "augmentation-zone"
    errors = []
    zone_stats = {}
    
    # Check trusted-zone
    if not client.bucket_exists(trusted_zone):
        errors.append(f"{trusted_zone} bucket does not exist")
    else:
        # Check for metadata
        metadata_objs = list(client.list_objects(trusted_zone, prefix="metadata/", recursive=True))
        metadata_csvs = [o for o in metadata_objs if o.object_name.lower().endswith(".csv")]
        
        # Check for images
        image_objs = list(client.list_objects(trusted_zone, prefix="images/", recursive=True))
        images = [o for o in image_objs if o.object_name.lower().endswith(('.jpg', '.jpeg', '.png'))]
        
        zone_stats['trusted_metadata'] = len(metadata_csvs)
        zone_stats['trusted_images'] = len(images)
        
        if not metadata_csvs:
            errors.append(f"{trusted_zone}/metadata/ is empty (no CSV files found)")
        if not images:
            errors.append(f"{trusted_zone}/images/ is empty (no images found)")
    
    # Check augmentation-zone
    if not client.bucket_exists(augmentation_zone):
        errors.append(f"{augmentation_zone} bucket does not exist")
    else:
        # Check for augmented images
        aug_image_objs = list(client.list_objects(augmentation_zone, prefix="images/", recursive=True))
        aug_images = [o for o in aug_image_objs if o.object_name.lower().endswith(('.jpg', '.jpeg', '.png'))]
        
        # Check for augmented metadata
        aug_meta_objs = list(client.list_objects(augmentation_zone, prefix="metadata/", recursive=True))
        aug_metadata = [o for o in aug_meta_objs if o.object_name.lower().endswith(".csv")]
        
        zone_stats['augmented_images'] = len(aug_images)
        zone_stats['augmented_metadata'] = len(aug_metadata)
        
        if not aug_images and not aug_metadata:
            errors.append(f"{augmentation_zone} is empty (no augmented images or metadata)")
    
    is_valid = len(errors) == 0
    error_message = "\n".join(errors) if errors else None
    
    return is_valid, error_message, zone_stats


def _validate_device_and_method(use_qlora, force_qlora=False):
    # Validate device availability and determine training method.
    # Args:
    #     use_qlora: Whether user wants to use QLoRA
    #     force_qlora: If True, abort if QLoRA not available instead of falling back
    # Returns:
    #     Tuple of (device, use_qlora, should_abort)
    cuda_available = torch.cuda.is_available()
    
    print("\n Device Detection:")
    print("=" * 40)
    
    if cuda_available:
        # Get GPU info
        gpu_name = torch.cuda.get_device_name(0)
        gpu_memory = torch.cuda.get_device_properties(0).total_memory / (1024**3)  # GB
        print(f" CUDA available: {gpu_name}")
        print(f"   GPU Memory: {gpu_memory:.1f} GB")
        device = torch.device("cuda")
        
        if use_qlora:
            print(f" QLoRA (4-bit quantization) will be used")
        else:
            print(f" LoRA (full precision) will be used")
        
        return device, use_qlora, False
    else:
        print(" CUDA not available")
        print("   GPU acceleration is not possible on this system")
        
        if use_qlora:
            if force_qlora:
                print("\n ERROR: QLoRA requires CUDA/GPU but none is available.")
                print("   Cannot proceed with QLoRA on CPU.")
                return None, use_qlora, True  # Abort
            else:
                print("\n WARNING: QLoRA requires CUDA. Falling back to LoRA on CPU.")
                print("   Training will be slower on CPU.")
                use_qlora = False
        else:
            print("\n LoRA will run on CPU (slower, but functional)")
        
        device = torch.device("cpu")
        return device, use_qlora, False


# ============================================================================
# MAIN FINE-TUNING FUNCTION
# ============================================================================
def process_fine_tuning_clip(
    minio_host="localhost:9000",
    access_key="admin",
    secret_key="password123",
    model_name="openai/clip-vit-base-patch32",
    use_qlora=True,
    force_qlora=False,  # If True, abort if GPU not available instead of fallback
    lora_r_text=16,     # LoRA rank for text encoder
    lora_r_vision=8,    # LoRA rank for vision (smaller, vision needs less adaptation)
    lora_alpha=32,      # LoRA scaling factor
    lora_dropout=0.1,
    batch_size=32,      # Larger batch = more in-batch negatives for contrastive learning
    num_epochs=3,
    learning_rate=1e-4,
    output_dir=None,
    num_original_pairs=100,
    num_augmented_pairs=30,  # Divided equally among 3 augmentation type categories
    num_negative_pairs=20,   # Divided equally between hard and normal negatives
    _skip_validation=False):  # Internal: skip validation if already done (e.g., from interactive mode)
    # Fine-tune CLIP with LoRA/QLoRA for domain-specific image-text alignment.
    # The LoRA approach (Hu et al., 2021) enables training <1% of parameters
    # while achieving comparable performance to full fine-tuning. This is
    # critical for efficient experimentation on consumer hardware.
    # Args:
    #     use_qlora: Use QLoRA (4-bit quantization) - requires GPU
    #     force_qlora: If True, abort if GPU not available instead of falling back to LoRA
    #     num_original_pairs: Number of original image-text pairs from trusted-zone
    #     num_augmented_pairs: Number of augmented pairs (divided equally among 3 types:
    #                         image augmentation, text augmentation, combined)
    #     num_negative_pairs: Number of explicit negative pairs (divided equally between
    #                        hard negatives from same family and normal negatives)
    print("\n" + "=" * 60)
    print(" Starting CLIP Fine-Tuning...")
    print("=" * 60)
    
    # =========================================================================
    # STEP 1: Validate Device and Method (skip if already validated)
    # =========================================================================
    if not _skip_validation:
        device, use_qlora, should_abort = _validate_device_and_method(use_qlora, force_qlora)
        
        if should_abort:
            raise SystemExit("\n Training aborted: QLoRA requires GPU but none is available.\n"
                            "   Either use LoRA instead or run on a machine with CUDA support.")
    else:
        # Device already validated in interactive mode
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    
    # =========================================================================
    # STEP 2: Initialize MinIO and Validate Data Zones (skip if already validated)
    # =========================================================================
    client = Minio(minio_host, access_key=access_key, secret_key=secret_key, secure=False)
    
    if not _skip_validation:
        print("\n Validating data zones...")
        is_valid, error_message, zone_stats = _validate_data_zones(client)
        
        if not is_valid:
            print("\n Data zone validation failed:")
            print(f"\n{error_message}")
            print("\n Please ensure:")
            print("   1. Run Trusted Zone processing first (Trusted_Metadata.py, Trusted_Images.py)")
            print("   2. Run Augmentation processing (augmentation_image.py, augmentation_text.py)")
            raise SystemExit("\n Training aborted: Required data zones are missing or empty.")
        
        print(" Data zones validated")
    
    # Load training data (from trusted-zone and augmentation-zone)
    print("\n Loading training data...")
    training_data = _load_training_data(
        client, 
        num_original_pairs=num_original_pairs,
        num_augmented_pairs=num_augmented_pairs,
        num_negative_pairs=num_negative_pairs
    )
    
    if not training_data:
        raise SystemExit(" No training data found.")
    
    # Initialize model with LoRA/QLoRA
    print("\n Initializing CLIP model...")
    model, processor = _initialize_clip_model(model_name, use_qlora, device)
    
    # Apply LoRA adapters to text and vision encoders
    print("\n Applying LoRA adapters...")
    model = _apply_lora_config(model, lora_r_text, lora_r_vision, lora_alpha, lora_dropout)
    
    # Create data loader
    print("\n Creating data loader...")
    train_loader = _create_data_loader(training_data, batch_size, processor, client)
    
    # Setup optimizer and scheduler
    optimizer, scheduler = _setup_training(model, learning_rate, num_epochs, len(train_loader), device)
    
    # Training loop
    print("\n Starting training...")
    training_history = []
    
    for epoch in range(num_epochs):
        print(f"\n Epoch {epoch + 1}/{num_epochs}")
        avg_loss = _train_epoch(model, train_loader, optimizer, scheduler, device)
        training_history.append({"epoch": epoch + 1, "loss": avg_loss})
        print(f"  Average Loss: {avg_loss:.4f}")
    
    # Save checkpoint
    print("\n Saving checkpoint...")
    if output_dir is None:
        # Handle both script execution and notebook execution
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            output_dir = os.path.join(script_dir, "../checkpoints")
        except NameError:
            # __file__ not defined (running in notebook)
            output_dir = os.path.join(os.getcwd(), "Fine-Tuning-Zone/checkpoints")
    
    checkpoint_path = _save_checkpoint(
        model, output_dir, model_name,
        use_qlora, lora_r_text, lora_r_vision, lora_alpha, training_history
    )
    
    # Save training data to MinIO for reproducibility
    _save_training_data_to_minio(client, training_data)
    
    print("\n" + "=" * 60)
    print(f" Fine-tuning complete!")
    print(f" Checkpoint saved to: {checkpoint_path}")
    print("=" * 60)

# ============================================================================
# DATA LOADING (Constraint 2 - Training Data)
# ============================================================================
def _load_training_data(client, num_original_pairs=100, num_augmented_pairs=30, num_negative_pairs=20):
    # Load training data from trusted-zone and augmentation-zone.
    # Args:
    #     num_original_pairs: Number of original pairs from trusted-zone
    #     num_augmented_pairs: Number of augmented pairs (divided equally among 3 types)
    #     num_negative_pairs: Number of negative pairs (half hard, half normal)
    # IMPORTANT: CLIP uses in-batch negatives, NOT explicit negative labels.
    # All pairs loaded here are POSITIVE (matching image-text).
    # Negatives are created implicitly: for each image in a batch, all OTHER
    # texts in that batch serve as negatives (and vice versa).
    # Hard negative mining is achieved by batching same-family species together,
    # letting the contrastive loss naturally push apart similar species.
    trusted_zone = "trusted-zone"
    augmentation_zone = "augmentation-zone"
    
    # Load metadata from trusted-zone (preserving all columns)
    metadata_df = None
    metadata_objs = [
        obj.object_name for obj in client.list_objects(trusted_zone, prefix="metadata/", recursive=True)
        if obj.object_name.lower().endswith(".csv")
    ]
    
    if metadata_objs:
        metadata_objs.sort(reverse=True)
        resp = client.get_object(trusted_zone, metadata_objs[0])
        metadata_df = pd.read_csv(io.BytesIO(resp.read()))
        resp.close()
        resp.release_conn()
    
    if metadata_df is None:
        return []
    
    # Create text descriptions
    metadata_df['text'] = metadata_df.apply(_create_text_description, axis=1)
    
    # Build image lookup from trusted-zone
    image_lookup = {}
    for obj in client.list_objects(trusted_zone, prefix="images/", recursive=True):
        if obj.object_name.lower().endswith(('.jpg', '.jpeg', '.png')):
            filename = os.path.basename(obj.object_name)
            uuid = filename.split('.')[0].split('_')[0]
            image_lookup[uuid] = (obj.object_name, trusted_zone)
    
    # Build augmented image lookup
    augmented_image_lookup = {}
    if client.bucket_exists(augmentation_zone):
        for obj in client.list_objects(augmentation_zone, prefix="images/", recursive=True):
            if obj.object_name.lower().endswith(('.jpg', '.jpeg', '.png')):
                filename = os.path.basename(obj.object_name)
                # Augmented images: uuid_augtype.jpg -> base uuid for metadata lookup
                base_uuid = filename.split('.')[0].split('_')[0]
                aug_key = filename.rsplit('.', 1)[0]
                # Extract augmentation type from filename
                parts = filename.rsplit('.', 1)[0].split('_')
                aug_type = parts[-1] if len(parts) > 1 else 'unknown'
                augmented_image_lookup[aug_key] = (obj.object_name, augmentation_zone, base_uuid, aug_type)
    
    # Load augmented text metadata
    augmented_text_df = None
    if client.bucket_exists(augmentation_zone):
        aug_meta_objs = [
            obj.object_name for obj in client.list_objects(augmentation_zone, prefix="metadata/", recursive=True)
            if obj.object_name.lower().endswith(".csv") and "augmentation_text" in obj.object_name.lower()
        ]
        if aug_meta_objs:
            aug_meta_objs.sort(reverse=True)
            resp = client.get_object(augmentation_zone, aug_meta_objs[0])
            augmented_text_df = pd.read_csv(io.BytesIO(resp.read()))
            resp.close()
            resp.release_conn()
    
    training_data = []
    
    # =========================================================================
    # 1. ORIGINAL PAIRS from trusted-zone
    # =========================================================================
    original_pairs = []
    
    for _, row in metadata_df.iterrows():
        uuid = str(row.get('uuid', ''))
        text = row.get('text', '')
        
        if not uuid or not text:
            continue
        
        image_info = image_lookup.get(uuid)
        if image_info:
            image_path, zone = image_info[0], image_info[1]
            
            # Preserve ALL trusted-zone metadata columns
            pair_data = {
                'image_path': image_path,
                'image_zone': zone,
                'text': text,
                'pair_type': 'original'
            }
            # Add all trusted zone columns
            for col in TRUSTED_ZONE_COLUMNS:
                pair_data[col] = row.get(col, '') if pd.notna(row.get(col, '')) else ''
            
            original_pairs.append(pair_data)
    
    # Sample requested number of original pairs
    if len(original_pairs) > num_original_pairs:
        original_pairs = random.sample(original_pairs, num_original_pairs)
    
    training_data.extend(original_pairs)
    
    # =========================================================================
    # 2. AUGMENTED PAIRS (divided equally among 3 types)
    # =========================================================================
    # Types: image_augmented, text_augmented, combined (image+text augmented)
    pairs_per_aug_type = num_augmented_pairs // 3
    remainder = num_augmented_pairs % 3
    
    # Type 1: Image Augmented (original text + augmented image)
    image_aug_pairs = []
    for key, info in augmented_image_lookup.items():
        if len(info) == 4:
            image_path, zone, base_uuid, aug_type = info
            match = metadata_df[metadata_df['uuid'].astype(str) == base_uuid]
            if len(match) > 0:
                row = match.iloc[0]
                text = row.get('text', '')
                if text:
                    pair_data = {
                        'image_path': image_path,
                        'image_zone': zone,
                        'text': text,
                        'pair_type': 'image_augmented',
                        'augmentation_type': aug_type
                    }
                    for col in TRUSTED_ZONE_COLUMNS:
                        pair_data[col] = row.get(col, '') if pd.notna(row.get(col, '')) else ''
                    image_aug_pairs.append(pair_data)
    
    if len(image_aug_pairs) > pairs_per_aug_type:
        image_aug_pairs = random.sample(image_aug_pairs, pairs_per_aug_type)
    training_data.extend(image_aug_pairs)
    
    # Type 2: Text Augmented (augmented text + original image)
    text_aug_pairs = []
    if augmented_text_df is not None:
        for _, row in augmented_text_df.iterrows():
            uuid = str(row.get('uuid', ''))
            text = row.get('text', '')
            aug_type = row.get('_augmentation_type', 'text_template')
            
            if not uuid or not text:
                continue
            
            image_info = image_lookup.get(uuid)
            if image_info:
                image_path, zone = image_info[0], image_info[1]
                pair_data = {
                    'image_path': image_path,
                    'image_zone': zone,
                    'text': text,
                    'pair_type': 'text_augmented',
                    'augmentation_type': aug_type
                }
                for col in TRUSTED_ZONE_COLUMNS:
                    pair_data[col] = row.get(col, '') if pd.notna(row.get(col, '')) else ''
                text_aug_pairs.append(pair_data)
    
    if len(text_aug_pairs) > pairs_per_aug_type:
        text_aug_pairs = random.sample(text_aug_pairs, pairs_per_aug_type)
    training_data.extend(text_aug_pairs)
    
    # Type 3: Combined (augmented text + augmented image)
    combined_aug_pairs = []
    if augmented_text_df is not None:
        for _, row in augmented_text_df.iterrows():
            uuid = str(row.get('uuid', ''))
            text = row.get('text', '')
            
            if not uuid or not text:
                continue
            
            # Find matching augmented image
            matching_aug_images = [
                (k, v) for k, v in augmented_image_lookup.items()
                if v[2] == uuid  # base_uuid matches
            ]
            
            if matching_aug_images:
                aug_key, aug_info = random.choice(matching_aug_images)
                image_path, zone, base_uuid, img_aug_type = aug_info
                
                pair_data = {
                    'image_path': image_path,
                    'image_zone': zone,
                    'text': text,
                    'pair_type': 'combined_augmented',
                    'augmentation_type': f"img:{img_aug_type}+txt:{row.get('_augmentation_type', 'template')}"
                }
                for col in TRUSTED_ZONE_COLUMNS:
                    pair_data[col] = row.get(col, '') if pd.notna(row.get(col, '')) else ''
                combined_aug_pairs.append(pair_data)
    
    target_combined = pairs_per_aug_type + remainder
    if len(combined_aug_pairs) > target_combined:
        combined_aug_pairs = random.sample(combined_aug_pairs, target_combined)
    training_data.extend(combined_aug_pairs)
    
    # =========================================================================
    # 3. NEGATIVE PAIRS (for hard negative mining during batching)
    # =========================================================================
    # These are still positive pairs but strategically selected:
    # - Hard negatives: Same family, different species (visually similar but different)
    # - Normal negatives: Different families (clearly different)
    
    hard_neg_count = num_negative_pairs // 2
    normal_neg_count = num_negative_pairs - hard_neg_count
    
    # Group by family for negative selection
    family_groups = {}
    for _, row in metadata_df.iterrows():
        family = str(row.get('family', 'Unknown')).strip()
        if family and family != 'Unknown':
            if family not in family_groups:
                family_groups[family] = []
            family_groups[family].append(row)
    
    # Collect already used UUIDs to avoid duplicates with original pairs
    used_uuids = set(item.get('uuid', '') for item in training_data)
    
    # Hard negatives: Select pairs from same family (visually similar species)
    # We want pairs from families with multiple species to create hard negatives
    hard_neg_pairs = []
    families_with_multiple = [f for f, items in family_groups.items() if len(items) >= 2]
    random.shuffle(families_with_multiple)
    
    # Collect all potential hard negative candidates
    hard_neg_candidates = []
    for family in families_with_multiple:
        members = family_groups[family]
        for row in members:
            uuid = str(row.get('uuid', ''))
            if uuid in used_uuids:
                continue
            text = row.get('text', '')
            image_info = image_lookup.get(uuid)
            
            if image_info and text:
                image_path, zone = image_info[0], image_info[1]
                pair_data = {
                    'image_path': image_path,
                    'image_zone': zone,
                    'text': text,
                    'pair_type': 'hard_negative_context'
                }
                for col in TRUSTED_ZONE_COLUMNS:
                    pair_data[col] = row.get(col, '') if pd.notna(row.get(col, '')) else ''
                hard_neg_candidates.append(pair_data)
    
    # Sample requested number of hard negatives
    if len(hard_neg_candidates) >= hard_neg_count:
        hard_neg_pairs = random.sample(hard_neg_candidates, hard_neg_count)
    else:
        hard_neg_pairs = hard_neg_candidates
    
    training_data.extend(hard_neg_pairs)
    for pair in hard_neg_pairs:
        used_uuids.add(pair.get('uuid', ''))
    
    # Normal negatives: Select pairs from all families (diversity across families)
    # Sample multiple from each family if needed to reach target count
    normal_neg_candidates = []
    for family, members in family_groups.items():
        for row in members:
            uuid = str(row.get('uuid', ''))
            if uuid in used_uuids:
                continue
            text = row.get('text', '')
            image_info = image_lookup.get(uuid)
            
            if image_info and text:
                image_path, zone = image_info[0], image_info[1]
                pair_data = {
                    'image_path': image_path,
                    'image_zone': zone,
                    'text': text,
                    'pair_type': 'normal_negative_context'
                }
                for col in TRUSTED_ZONE_COLUMNS:
                    pair_data[col] = row.get(col, '') if pd.notna(row.get(col, '')) else ''
                normal_neg_candidates.append(pair_data)
    
    # Sample requested number of normal negatives
    if len(normal_neg_candidates) >= normal_neg_count:
        normal_neg_pairs = random.sample(normal_neg_candidates, normal_neg_count)
    else:
        normal_neg_pairs = normal_neg_candidates
    
    training_data.extend(normal_neg_pairs)
    
    # Shuffle but keep all data - diversity is important for contrastive learning
    random.shuffle(training_data)
    
    # Summary
    print(f"\n  Training data loaded:")
    print(f"    Original pairs: {len(original_pairs)}")
    print(f"    Image-augmented pairs: {len(image_aug_pairs)}")
    print(f"    Text-augmented pairs: {len(text_aug_pairs)}")
    print(f"    Combined-augmented pairs: {len(combined_aug_pairs)}")
    print(f"    Hard negative context pairs: {len(hard_neg_pairs)}")
    print(f"    Normal negative context pairs: {len(normal_neg_pairs)}")
    print(f"    Total: {len(training_data)}")
    
    return training_data

def _create_text_description(row):
    # Create natural language description for contrastive learning.
    def clean(value):
        return value if value and str(value) not in ['nan', 'None', ''] else ''
    
    common = clean(str(row.get('common', '')).strip())
    scientific = clean(str(row.get('scientific_name', '')).strip())
    family = clean(str(row.get('family', '')).strip())
    
    parts = ["This wildlife is"]
    if common:
        parts.append(f"commonly known as {common}")
    if scientific:
        parts.append(f"scientifically named {scientific}")
    if species:
        parts.append(f"and specie of {species}")
    if family:
        parts.append(f"from the {family} family")
    if kingdom:
        parts.append(f"in kingdom {kingdom}")
    if phylum:
        parts.append(f"phylum {phylum}")
    if class_name:
        parts.append(f"in the {class_name} class")
    if order:
        parts.append(f"order of {order}")
    if genus:
        parts.append(f"genus {genus}")
    
    return ' '.join(parts) + '.'

# ============================================================================
# MODEL INITIALIZATION
# ============================================================================
def _initialize_clip_model(model_name, use_qlora, device):
    # Initialize CLIP model with optional QLoRA quantization.
    # QLoRA uses 4-bit quantization (nf4) which stores each weight in 4 bits
    # instead of 32 bits, reducing model size by 8x. Double quantization
    # further reduces memory overhead of quantization constants.
    print(f" Loading model: {model_name}")
    
    if use_qlora and torch.cuda.is_available():
        # BitsAndBytes config for 4-bit quantization
        bnb_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_quant_type="nf4",           # NormalFloat4 quantization
            bnb_4bit_compute_dtype=torch.float16, # FP16 for computation
            bnb_4bit_use_double_quant=True        # Nested quantization
        )
        model = CLIPModel.from_pretrained(model_name, quantization_config=bnb_config)
        model = prepare_model_for_kbit_training(model)
        print(" Loaded with 4-bit quantization (QLoRA)")
    else:
        model = CLIPModel.from_pretrained(model_name)
        print(" Loaded in full precision")
    
    processor = CLIPProcessor.from_pretrained(model_name)
    model = model.to(device)
    
    return model, processor

def _apply_lora_config(model, lora_r_text, lora_r_vision, lora_alpha, lora_dropout):
    # Apply LoRA adapters to CLIP's text and vision encoders.
    # LoRA injects trainable low-rank matrices into attention layers (q_proj, v_proj).
    # Text and vision encoders can use different ranks:
    # - Text encoder: higher rank (16) for semantic understanding
    # - Vision encoder: lower rank (8) as visual features need less adaptation
    # Apply LoRA to text encoder with higher rank
    if hasattr(model, 'text_model'):
        text_lora_config = LoraConfig(
            r=lora_r_text,
            lora_alpha=lora_alpha,
            lora_dropout=lora_dropout,
            target_modules=["q_proj", "v_proj"],
            task_type=TaskType.FEATURE_EXTRACTION
        )
        peft_text = get_peft_model(model.text_model, text_lora_config)
        model.text_model = _TextModelWrapper(peft_text)
        print(f" Applied LoRA to text encoder (r={lora_r_text})")
    
    # Apply LoRA to vision encoder with lower rank
    if hasattr(model, 'vision_model'):
        vision_lora_config = LoraConfig(
            r=lora_r_vision,
            lora_alpha=lora_alpha,
            lora_dropout=lora_dropout,
            target_modules=["q_proj", "v_proj"],
            task_type=TaskType.FEATURE_EXTRACTION
        )
        peft_vision = get_peft_model(model.vision_model, vision_lora_config)
        model.vision_model = _VisionModelWrapper(peft_vision)
        print(f" Applied LoRA to vision encoder (r={lora_r_vision})")
    
    # Freeze base model, only train LoRA adapters
    for name, param in model.named_parameters():
        if 'lora_' not in name and 'logit_scale' not in name:
            param.requires_grad = False
    
    # logit_scale is trainable (temperature parameter for contrastive loss)
    if hasattr(model, 'logit_scale'):
        model.logit_scale.requires_grad = True
    
    # Report parameter efficiency
    trainable = sum(p.numel() for p in model.parameters() if p.requires_grad)
    total = sum(p.numel() for p in model.parameters())
    print(f" Trainable: {trainable:,} / {total:,} ({100*trainable/total:.2f}%)")
    
    return model

class _VisionModelWrapper(torch.nn.Module):
    # Wrapper to filter kwargs for vision encoder.
    # IMPORTANT: We access self.peft_model.base_model.model to get the actual
    # CLIPVisionTransformer with LoRA layers injected. This bypasses PEFT's
    # forward signature which expects text-like inputs (input_ids).
    # The LoRA layers are already injected into the model's attention layers,
    # so they will be used when we call the base model directly.
    def __init__(self, peft_model):
        super().__init__()
        self.peft_model = peft_model
        if hasattr(peft_model, 'config'):
            self.config = peft_model.config
    
    def named_parameters(self, prefix='', recurse=True):
        return self.peft_model.named_parameters(prefix=prefix, recurse=recurse)
    
    def parameters(self, recurse=True):
        return self.peft_model.parameters(recurse=recurse)
    
    def forward(self, *args, **kwargs):
        # Forward pass through the vision encoder.
        # CRITICAL FIX: Access the underlying model with LoRA layers directly,
        # bypassing PEFT's forward method which incorrectly passes input_ids
        # to CLIPVisionTransformer.
        # peft_model structure:
        # - peft_model (PeftModelForFeatureExtraction)
        #   - base_model (LoraModel/BaseTuner)
        #     - model (CLIPVisionTransformer with LoRA layers injected)
        # Extract pixel_values - this is the only required input for vision
        pixel_values = kwargs.get("pixel_values", args[0] if args else None)
        
        # Build kwargs for CLIPVisionTransformer.forward()
        valid_kwargs = {}
        if pixel_values is not None:
            valid_kwargs['pixel_values'] = pixel_values
        if 'output_attentions' in kwargs:
            valid_kwargs['output_attentions'] = kwargs['output_attentions']
        if 'output_hidden_states' in kwargs:
            valid_kwargs['output_hidden_states'] = kwargs['output_hidden_states']
        if 'return_dict' in kwargs:
            valid_kwargs['return_dict'] = kwargs['return_dict']
        if 'interpolate_pos_encoding' in kwargs:
            valid_kwargs['interpolate_pos_encoding'] = kwargs['interpolate_pos_encoding']
        
        # Access the underlying model with LoRA layers injected
        # This bypasses PEFT's forward which has incompatible signature
        underlying_model = self.peft_model.base_model.model
        return underlying_model(**valid_kwargs)


class _TextModelWrapper(torch.nn.Module):
    
    # Wrapper to filter kwargs for text encoder.
    
    # IMPORTANT: We access self.peft_model.base_model.model to get the actual
    # CLIPTextTransformer with LoRA layers injected.
    
    def __init__(self, peft_model):
        super().__init__()
        self.peft_model = peft_model
        if hasattr(peft_model, 'config'):
            self.config = peft_model.config
    
    def named_parameters(self, prefix='', recurse=True):
        return self.peft_model.named_parameters(prefix=prefix, recurse=recurse)
    
    def parameters(self, recurse=True):
        return self.peft_model.parameters(recurse=recurse)
    
    def forward(self, *args, **kwargs):
        # Forward pass through the text encoder.
        # Access the underlying model with LoRA layers directly for consistency.
        # Build kwargs for CLIPTextTransformer.forward()
        valid_kwargs = {}
        
        if 'input_ids' in kwargs:
            valid_kwargs['input_ids'] = kwargs['input_ids']
        elif args:
            valid_kwargs['input_ids'] = args[0]
        
        if 'attention_mask' in kwargs:
            valid_kwargs['attention_mask'] = kwargs['attention_mask']
        if 'position_ids' in kwargs:
            valid_kwargs['position_ids'] = kwargs['position_ids']
        if 'output_attentions' in kwargs:
            valid_kwargs['output_attentions'] = kwargs['output_attentions']
        if 'output_hidden_states' in kwargs:
            valid_kwargs['output_hidden_states'] = kwargs['output_hidden_states']
        if 'return_dict' in kwargs:
            valid_kwargs['return_dict'] = kwargs['return_dict']
        
        # Access the underlying model with LoRA layers injected
        underlying_model = self.peft_model.base_model.model
        return underlying_model(**valid_kwargs)

# ============================================================================
# DATA LOADING
# ============================================================================
def _create_data_loader(training_data, batch_size, processor, client):
    
    # Create DataLoader for CLIP training.
    
    # All samples are positive (matching image-text pairs).
    # Negatives are created implicitly by CLIP's contrastive loss:
    # for each sample in a batch, all other samples serve as negatives.
    
    
    class CLIPDataset(Dataset):
        def __init__(self, data, processor, client):
            self.data = data
            self.processor = processor
            self.client = client
        
        def __len__(self):
            return len(self.data)
        
        def __getitem__(self, idx):
            item = self.data[idx]
            
            # Load image from appropriate zone
            try:
                zone = item.get('image_zone', 'trusted-zone')
                resp = self.client.get_object(zone, item['image_path'])
                image = Image.open(io.BytesIO(resp.read())).convert("RGB")
                resp.close()
                resp.release_conn()
            except Exception:
                # Placeholder on error (shouldn't happen often)
                image = Image.new('RGB', (224, 224), color=(128, 128, 128))
            
            # Process inputs using CLIP processor
            inputs = self.processor(
                text=item['text'],
                images=image,
                return_tensors="pt",
                padding='max_length',
                truncation=True,
                max_length=77
            )
            
            # No label needed - all pairs are positive, negatives are in-batch
            return {
                'pixel_values': inputs['pixel_values'].squeeze(0),
                'input_ids': inputs['input_ids'].squeeze(0),
                'attention_mask': inputs['attention_mask'].squeeze(0)
            }
    
    dataset = CLIPDataset(training_data, processor, client)
    # Shuffle is critical for diverse in-batch negatives
    return DataLoader(dataset, batch_size=batch_size, shuffle=True, num_workers=0, drop_last=True)

# ============================================================================
# TRAINING
# ============================================================================
def _setup_training(model, learning_rate, num_epochs, steps_per_epoch, device):
    # Setup optimizer and learning rate scheduler.
    # Uses AdamW with linear warmup and cosine decay, which is standard
    # for transformer fine-tuning. Warmup prevents early gradient explosion.
    optimizer = torch.optim.AdamW(
        [p for p in model.parameters() if p.requires_grad],
        lr=learning_rate,
        weight_decay=0.01
    )
    
    total_steps = num_epochs * steps_per_epoch
    warmup_steps = int(0.1 * total_steps)  # 10% warmup
    
    def lr_lambda(step):
        if step < warmup_steps:
            return step / warmup_steps  # Linear warmup
        progress = (step - warmup_steps) / (total_steps - warmup_steps)
        return 0.5 * (1 + np.cos(np.pi * progress))  # Cosine decay
    
    scheduler = torch.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda)
    
    return optimizer, scheduler

def _train_epoch(model, train_loader, optimizer, scheduler, device):
    # Train for one epoch using standard CLIP contrastive loss (InfoNCE).
    # CLIP Loss Mechanism:
    # - All samples in the batch are POSITIVE (matching image-text pairs)
    # - Negatives are IMPLICIT: for image_i, all text_j where j != i are negatives
    # - This is why batch size matters: batch=32 gives 31 negatives per sample
    # Loss = 0.5 * (CE(logits_per_image, targets) + CE(logits_per_text, targets))
    # where targets = [0, 1, 2, ..., batch_size-1] (diagonal is correct match)
    # The learnable logit_scale (temperature) controls softmax sharpness.
    model.train()
    total_loss = 0.0
    num_batches = 0
    
    for batch in tqdm(train_loader, desc="Training"):
        pixel_values = batch['pixel_values'].to(device)
        input_ids = batch['input_ids'].to(device)
        attention_mask = batch['attention_mask'].to(device)
        
        optimizer.zero_grad()
        
        # Forward pass through CLIP
        outputs = model(
            input_ids=input_ids,
            attention_mask=attention_mask,
            pixel_values=pixel_values,
            return_loss=False
        )
        
        # L2 normalize embeddings (standard for cosine similarity)
        image_embeds = F.normalize(outputs.image_embeds, dim=-1)
        text_embeds = F.normalize(outputs.text_embeds, dim=-1)
        
        # Compute scaled similarity matrix
        # logit_scale is learnable and controls temperature
        logit_scale = model.logit_scale.exp()
        logits_per_image = logit_scale * image_embeds @ text_embeds.t()
        logits_per_text = logits_per_image.t()
        
        # Standard CLIP loss: diagonal elements are correct matches
        # targets = [0, 1, 2, ..., N-1] means image_i should match text_i
        batch_size = logits_per_image.shape[0]
        targets = torch.arange(batch_size, device=device)
        
        # Symmetric cross-entropy loss
        loss_img = F.cross_entropy(logits_per_image, targets)
        loss_txt = F.cross_entropy(logits_per_text, targets)
        loss = (loss_img + loss_txt) / 2.0
        
        loss.backward()
        optimizer.step()
        scheduler.step()
        
        total_loss += loss.item()
        num_batches += 1
    
    return total_loss / num_batches if num_batches > 0 else 0.0

# ============================================================================
# CHECKPOINT SAVING
# ============================================================================
def _save_checkpoint(model, output_dir, model_name, use_qlora, lora_r_text, lora_r_vision, lora_alpha, training_history):
    # Save fine-tuned checkpoint with LoRA adapters.
    # Only the LoRA adapter weights are saved (not the full model),
    # resulting in a small checkpoint (~few MB vs ~1GB for full model).
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    method = "qlora" if use_qlora else "lora"
    checkpoint_name = f"clip_{method}_t{lora_r_text}_v{lora_r_vision}_{timestamp}"
    checkpoint_path = os.path.join(output_dir, checkpoint_name)
    os.makedirs(checkpoint_path, exist_ok=True)
    
    # Save LoRA adapters for each encoder
    if hasattr(model, 'text_model') and hasattr(model.text_model, 'peft_model'):
        text_path = os.path.join(checkpoint_path, "text_encoder")
        model.text_model.peft_model.save_pretrained(text_path)
        print(f" Saved text encoder adapters to: {text_path}")
    
    if hasattr(model, 'vision_model') and hasattr(model.vision_model, 'peft_model'):
        vision_path = os.path.join(checkpoint_path, "vision_encoder")
        model.vision_model.peft_model.save_pretrained(vision_path)
        print(f" Saved vision encoder adapters to: {vision_path}")
    
    # Save training configuration for reproducibility (Constraint 9)
    config = {
        "model_name": model_name,
        "use_qlora": use_qlora,
        "lora_r_text": lora_r_text,
        "lora_r_vision": lora_r_vision,
        "lora_alpha": lora_alpha,
        "training_history": training_history,
        "created_at": datetime.now().isoformat()
    }
    
    with open(os.path.join(checkpoint_path, "training_config.json"), "w") as f:
        json.dump(config, f, indent=2)
    
    return checkpoint_path

def _clear_training_zone(client):
    # Clear the training-zone metadata/ and images/ subfolders before saving new data.
    # This ensures a clean slate for each training run.
    training_zone = "training-zone"
    
    if not client.bucket_exists(training_zone):
        return  # Nothing to clear
    
    print("\n Clearing previous training data...")
    
    # Clear metadata/ subfolder
    metadata_deleted = 0
    try:
        metadata_objs = list(client.list_objects(training_zone, prefix="metadata/", recursive=True))
        for obj in metadata_objs:
            client.remove_object(training_zone, obj.object_name)
            metadata_deleted += 1
    except Exception as e:
        print(f"  Warning: Error clearing metadata/: {e}")
    
    # Clear images/ subfolder
    images_deleted = 0
    try:
        images_objs = list(client.list_objects(training_zone, prefix="images/", recursive=True))
        for obj in images_objs:
            client.remove_object(training_zone, obj.object_name)
            images_deleted += 1
    except Exception as e:
        print(f"  Warning: Error clearing images/: {e}")
    
    if metadata_deleted > 0 or images_deleted > 0:
        print(f"  Cleared {metadata_deleted} metadata files and {images_deleted} image files")
    else:
        print("  Training zone was already empty")


def _save_training_data_to_minio(client, training_data):
    # Save training data (metadata AND images) to MinIO for reproducibility (Constraint 9).
    # CLEARS metadata/ and images/ subfolders before saving new data.
    # COPIES all training images from their source zones to training-zone.
    # PRESERVES ALL TRUSTED-ZONE COLUMNS exactly as retrieved.
    # All training pairs are positive (matching image-text).
    training_zone = "training-zone"
    
    # Create bucket if it doesn't exist
    if not client.bucket_exists(training_zone):
        client.make_bucket(training_zone)
        print(f" Created {training_zone} bucket")
    
    # Clear previous training data (metadata/ and images/ only)
    _clear_training_zone(client)
    
    # =========================================================================
    # COPY TRAINING IMAGES to training-zone
    # =========================================================================
    print("\n Copying training images to training-zone...")
    copied_images = 0
    failed_images = 0
    copied_paths = {}  # Map original path to new path in training-zone
    
    for item in training_data:
        source_zone = item.get('image_zone', 'trusted-zone')
        source_path = item['image_path']
        
        # Create destination path: images/{pair_type}/{filename}
        pair_type = item.get('pair_type', 'original')
        filename = os.path.basename(source_path)
        dest_path = f"images/{pair_type}/{filename}"
        
        # Skip if already copied (avoid duplicates)
        cache_key = f"{source_zone}:{source_path}"
        if cache_key in copied_paths:
            continue
        
        try:
            # Copy image from source zone to training-zone
            resp = client.get_object(source_zone, source_path)
            image_data = resp.read()
            resp.close()
            resp.release_conn()
            
            # Upload to training-zone
            image_buffer = io.BytesIO(image_data)
            client.put_object(
                training_zone,
                dest_path,
                image_buffer,
                length=len(image_data),
                content_type="image/jpeg"
            )
            
            copied_paths[cache_key] = dest_path
            copied_images += 1
            
            if copied_images % 100 == 0:
                print(f"   Copied {copied_images} images...")
                
        except Exception as e:
            failed_images += 1
            if failed_images <= 3:  # Only show first few errors
                print(f"   Warning: Failed to copy {source_path}: {e}")
    
    print(f" Copied {copied_images} images to training-zone")
    if failed_images > 0:
        print(f"   Warning: {failed_images} images failed to copy")
    
    # =========================================================================
    # SAVE METADATA with updated paths pointing to training-zone
    # =========================================================================
    records = []
    for item in training_data:
        # Update image path to point to training-zone location
        pair_type = item.get('pair_type', 'original')
        filename = os.path.basename(item['image_path'])
        training_zone_path = f"images/{pair_type}/{filename}"
        
        record = {
            'text': item.get('text', ''),
            'image_path': training_zone_path,  # Updated path in training-zone
            'original_image_path': item['image_path'],  # Keep original for reference
            'original_image_zone': item.get('image_zone', 'trusted-zone'),
            'pair_type': pair_type,
            'augmentation_type': item.get('augmentation_type', '')
        }
        # Add all trusted zone columns (preserving original metadata)
        for col in TRUSTED_ZONE_COLUMNS:
            record[col] = item.get(col, '')
        records.append(record)
    
    df = pd.DataFrame(records)
    
    # Reorder columns: trusted zone columns first, then training-specific columns
    column_order = TRUSTED_ZONE_COLUMNS + ['text', 'image_path', 'original_image_path', 
                                            'original_image_zone', 'pair_type', 'augmentation_type']
    # Only include columns that exist
    column_order = [c for c in column_order if c in df.columns]
    df = df[column_order]
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    client.put_object(
        training_zone,
        f"metadata/training_data_{timestamp}.csv",
        csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type="text/csv"
    )
    print(f" Saved metadata for {len(training_data)} training samples")
    print(f"   Preserved columns: {', '.join(TRUSTED_ZONE_COLUMNS)}")
    
    # Print summary by pair type
    pair_type_counts = df['pair_type'].value_counts().to_dict()
    print(f"   Pair types: {pair_type_counts}")

# ============================================================================
# INTERACTIVE MODE
# ============================================================================
def run_interactive_mode():
    # Run interactive fine-tuning configuration.
    print("\n" + "=" * 60)
    print(" CLIP Fine-Tuning - Interactive Mode")
    print("=" * 60)
    
    minio_host = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'admin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'password123')
    
    # =========================================================================
    # PRE-training CHECK: Validate Data Zones
    # =========================================================================
    print("\n Pre-training Check:")
    print("-" * 40)
    print(" Checking data zones...")
    
    try:
        client = Minio(minio_host, access_key=access_key, secret_key=secret_key, secure=False)
        is_valid, error_message, zone_stats = _validate_data_zones(client)
        
        if not is_valid:
            print("\n Data zone validation FAILED:")
            print(f"\n{error_message}")
            print("\n Please ensure you have run the data pipeline first:")
            print("   1. Temporal Zone  → Persistent Landing")
            print("   2. Formatted Zone → Format metadata and images")
            print("   3. Trusted Zone   → Clean and validate data")
            print("   4. Augmentation   → Generate augmented samples")
            print("\n Cannot proceed without training data.")
            return
        
        print(f" trusted-zone: {zone_stats.get('trusted_images', 0)} images, {zone_stats.get('trusted_metadata', 0)} metadata files")
        print(f" augmentation-zone: {zone_stats.get('augmented_images', 0)} images, {zone_stats.get('augmented_metadata', 0)} metadata files")
        print(" Data zones validated successfully!")
    except Exception as e:
        print(f"\n Failed to connect to MinIO: {e}")
        print("   Please ensure MinIO is running and accessible.")
        return
    
    # =========================================================================
    # DEVICE CHECK
    # =========================================================================
    print("\n Device Check:")
    print("-" * 40)
    cuda_available = torch.cuda.is_available()
    
    if cuda_available:
        gpu_name = torch.cuda.get_device_name(0)
        gpu_memory = torch.cuda.get_device_properties(0).total_memory / (1024**3)
        print(f" CUDA available: {gpu_name} ({gpu_memory:.1f} GB)")
        print("   Both QLoRA and LoRA are available.")
    else:
        print(" CUDA not available (CPU only)")
        print("   Only LoRA is available (QLoRA requires GPU)")
        print("   Note: Training on CPU will be slower.")
    
    # =========================================================================
    # TRAINING DATA CONFIGURATION
    # =========================================================================
    print("\n Training Data Configuration:")
    print("  (Data will be sampled from trusted-zone and augmentation-zone)")
    
    # Original pairs from trusted-zone
    orig_input = input("\n  How many ORIGINAL pairs from trusted-zone? [100]: ").strip()
    num_original_pairs = int(orig_input) if orig_input else 100
    
    # Augmented pairs (divided among 3 types)
    print("\n  Augmented pairs will be divided equally among 3 types:")
    print("    1. Image-augmented (original text + augmented image)")
    print("    2. Text-augmented (augmented text + original image)")
    print("    3. Combined (augmented text + augmented image)")
    aug_input = input("  Total AUGMENTED pairs? (divided by 3 types) [30]: ").strip()
    num_augmented_pairs = int(aug_input) if aug_input else 30
    
    # Negative pairs (divided between hard and normal)
    print("\n  Negative context pairs for hard negative mining:")
    print("    - Hard negatives: Same family, different species (visually similar)")
    print("    - Normal negatives: Different families (diverse)")
    neg_input = input("  Total NEGATIVE context pairs? (half hard, half normal) [20]: ").strip()
    num_negative_pairs = int(neg_input) if neg_input else 20
    
    # =========================================================================
    # MODEL CONFIGURATION
    # =========================================================================
    print("\n Model Configuration:")
    
    # Handle QLoRA selection based on device availability
    force_qlora = False
    if cuda_available:
        print("  QLoRA (4-bit) - Memory efficient, GPU required")
        print("  LoRA (full precision) - Standard, works on CPU/GPU")
        qlora_input = input("  Use QLoRA (4-bit)? [y/n, default=y]: ").strip().lower()
        use_qlora = qlora_input != 'n'
    else:
        print("  WARNING: QLoRA requires GPU - not available on this system")
        print("  LoRA will be used instead (works on CPU)")
        lora_choice = input("  Continue with LoRA on CPU? [y/n, default=y]: ").strip().lower()
        if lora_choice == 'n':
            print("\n Training cancelled.")
            return
        use_qlora = False
    
    lora_r_text_input = input("  LoRA rank for text encoder [16]: ").strip()
    lora_r_text = int(lora_r_text_input) if lora_r_text_input else 16
    
    lora_r_vision_input = input("  LoRA rank for vision encoder [8]: ").strip()
    lora_r_vision = int(lora_r_vision_input) if lora_r_vision_input else 8
    
    # =========================================================================
    # TRAINING CONFIGURATION
    # =========================================================================
    print("\n Training Configuration:")
    epochs_input = input("  Number of epochs [3]: ").strip()
    num_epochs = int(epochs_input) if epochs_input else 3
    
    batch_input = input("  Batch size [32]: ").strip()
    batch_size = int(batch_input) if batch_input else 32
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 60)
    print(" Configuration Summary:")
    print("=" * 60)
    
    print(f"\n Device:")
    if cuda_available:
        gpu_name = torch.cuda.get_device_name(0)
        print(f"   GPU: {gpu_name}")
    else:
        print(f"   CPU only (training will be slower)")
    
    print(f"\n Data Zones:")
    print(f"   trusted-zone: {zone_stats.get('trusted_images', 0)} images")
    print(f"   augmentation-zone: {zone_stats.get('augmented_images', 0)} images")
    
    print(f"\n Training Data:")
    print(f"   Original pairs: {num_original_pairs}")
    print(f"   Augmented pairs: {num_augmented_pairs} ({num_augmented_pairs // 3} per type)")
    print(f"   Negative context pairs: {num_negative_pairs} ({num_negative_pairs // 2} hard, {num_negative_pairs - num_negative_pairs // 2} normal)")
    total_pairs = num_original_pairs + num_augmented_pairs + num_negative_pairs
    print(f"   Total training samples: ~{total_pairs}")
    
    print(f"\n Model:")
    print(f"   Method: {'QLoRA (4-bit)' if use_qlora else 'LoRA'}")
    print(f"   Text encoder LoRA rank: {lora_r_text}")
    print(f"   Vision encoder LoRA rank: {lora_r_vision}")
    
    print(f"\n Training:")
    print(f"   Epochs: {num_epochs}")
    print(f"   Batch size: {batch_size} ({batch_size - 1} in-batch negatives)")
    
    if input("\n Start training? (y/n): ").strip().lower() in ['y', 'yes']:
        process_fine_tuning_clip(
            minio_host=minio_host,
            access_key=access_key,
            secret_key=secret_key,
            use_qlora=use_qlora,
            force_qlora=False,  # Interactive mode always allows fallback
            lora_r_text=lora_r_text,
            lora_r_vision=lora_r_vision,
            lora_alpha=lora_r_text * 2,  # Based on text rank
            num_epochs=num_epochs,
            batch_size=batch_size,
            num_original_pairs=num_original_pairs,
            num_augmented_pairs=num_augmented_pairs,
            num_negative_pairs=num_negative_pairs,
            _skip_validation=True  # Already validated in interactive mode
        )


if __name__ == "__main__":
    run_interactive_mode()
