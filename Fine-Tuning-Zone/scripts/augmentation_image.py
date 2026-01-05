"""

-Arman Bazarchi-
Fine-Tuning Zone — Image Augmentation


This module implements image data augmentation for CLIP model fine-tuning,
directly addressing Constraint 3 (Data Augmentation) from the ADSDB project.

Image augmentation is a fundamental technique in computer vision that creates
synthetic variations of existing images to:

1. **Expand Training Data**: Increases effective dataset size without collecting
   new images, which is especially valuable when data is limited.

2. **Improve Generalization**: Exposes the model to variations it may encounter
   at inference time (different lighting, angles, crops, etc.).

3. **Reduce Overfitting**: By presenting slightly different views of the same
   semantic content, the model learns robust features rather than memorizing
   pixel-level details.

METHODOLOGY: GEOMETRIC & PHOTOMETRIC AUGMENTATION
We implement a diverse set of augmentations from two categories:

**Geometric Transformations** (Spatial variations):
- Random resized crop: Simulates zoom and framing differences
- Center crop: Stabilizes training with consistent centering
- Rotation: Orientation robustness (±10-15°)
- Translation: Camera shift tolerance
- Partial crop: Handles partially visible subjects

**Photometric Transformations** (Appearance variations):
- Brightness jitter: Lighting variability
- Contrast jitter: Environmental differences
- Saturation jitter: Camera/sensor color differences
- Gaussian noise: Sensor noise simulation
- Gaussian blur: Motion/focus variation

This dual approach is supported by "A Survey on Image Data Augmentation for
Deep Learning" (Shorten & Khoshgoftaar, 2019), which establishes these as
foundational augmentations for deep learning.

BALANCED SAMPLING STRATEGY
--------------------------------------------------------------------------------
To ensure fair representation across taxonomic families (per project requirements),
we implement stratified sampling:
- Target equal samples per family
- Redistribute when families have insufficient images
- Maintain taxonomic structure in augmented data

"""

import os
import io
import random
from datetime import datetime
from typing import List, Dict, Tuple
from PIL import Image, ImageFilter, ImageEnhance
import numpy as np
import pandas as pd
from minio import Minio
from minio.error import S3Error
import torchvision.transforms as transforms


def process_image_augmentation(
    minio_host="localhost:9000",
    access_key="admin",
    secret_key="password123",
    num_images=None,
    num_augmentations_per_image=None):
    # Main entry point for image augmentation processing.
    # WORKFLOW
    # --------
    # 1. Connect to MinIO and validate zones exist
    # 2. Clear existing augmented images (ensures idempotent execution)
    # 3. Load metadata for taxonomic information
    # 4. Load image list from trusted-zone
    # 5. Select images with balanced family representation
    # 6. Apply random augmentations to each selected image
    # 7. Save augmented images to augmentation-zone with structured paths
    # DATA MANAGEMENT STRATEGY (Constraint 4: New Zones)
    # --------------------------------------------------
    # Augmented images are stored in augmentation-zone with taxonomic folder
    # structure: images/{kingdom}/{class}/{family}/{species}/{uuid}_{augtype}.jpg
    # This structure enables:
    # - Taxonomic-aware sampling during training
    # - Easy visual inspection of augmentation quality
    # - Clear separation from original trusted-zone data
    # Args:
    #     minio_host: MinIO endpoint address
    #     access_key: MinIO access credentials
    #     secret_key: MinIO secret credentials
    #     num_images: Number of images to sample (None = interactive prompt)
    #     num_augmentations_per_image: Augmentations per image (None = prompt)
    
    print("=" * 60)
    print(" Image Augmentation Processing")
    print("=" * 60)
    
    # Initialize MinIO client and validate bucket structure
    client = _initialize_minio_client(minio_host, access_key, secret_key)
    
    # Clear previous augmentation to ensure clean state (idempotency)
    print("\n Cleaning augmentation-zone...")
    _clear_augmentation_zone_images(client)
    
    # Interactive configuration if parameters not provided
    if num_images is None:
        print("\n Configuration:")
        num_images_input = input("  How many images to retrieve from trusted-zone? [100]: ").strip()
        try:
            num_images = int(num_images_input) if num_images_input else 100
        except ValueError:
            print("  Invalid input, using default: 100")
            num_images = 100
    
    if num_augmentations_per_image is None:
        num_augmentations_input = input("  How many augmentations per image? [3]: ").strip()
        try:
            num_augmentations_per_image = int(num_augmentations_input) if num_augmentations_input else 3
        except ValueError:
            print("  Invalid input, using default: 3")
            num_augmentations_per_image = 3
    
    print(f"\n Selected: {num_images} images, {num_augmentations_per_image} augmentations per image")
    print(f" Expected output: {num_images * num_augmentations_per_image} augmented images")
    
    # Load metadata for taxonomic grouping
    print("\n Loading metadata from trusted-zone...")
    metadata_lookup = _load_metadata_lookup(client)
    
    # Load image list from trusted-zone
    print("\n Loading images from trusted-zone...")
    image_objects = _load_images_from_trusted(client)
    
    if not image_objects:
        raise SystemExit(" No images found in trusted-zone. Cannot proceed.")
    
    print(f" Found {len(image_objects)} total images in trusted-zone")
    
    # Stratified selection by family for balanced representation
    print(f"\n Selecting images equally from each family (target: {num_images} total)...")
    selected_images = _select_images_by_family(client, image_objects, num_images, metadata_lookup)
    
    if not selected_images:
        raise SystemExit(" No images could be selected. Cannot proceed.")
    
    print(f" Selected {len(selected_images)} images for augmentation")
    num_images = len(selected_images)
    
    # Apply augmentation transforms
    print(f"\n Applying {num_augmentations_per_image} random augmentations to each image...")
    print(f" Each image will generate {num_augmentations_per_image} separate augmented images")
    augmented_images = _apply_image_augmentations(client, selected_images, num_augmentations_per_image, metadata_lookup)
    
    expected_count = len(selected_images) * num_augmentations_per_image
    print(f" Generated {len(augmented_images)} augmented images (expected: {expected_count})")
    
    # Persist augmented images to augmentation-zone
    print("\n Saving augmented images to augmentation-zone...")
    _save_images_to_augmentation(client, augmented_images)
    
    print("\n" + "=" * 60)
    print(" Image augmentation completed successfully!")
    print(f" Saved {len(augmented_images)} augmented images to augmentation-zone")
    print("=" * 60)


def _initialize_minio_client(minio_host, access_key, secret_key):
    # Initialize MinIO client and ensure required buckets exist.
    # ZONE ARCHITECTURE (Constraint 4)
    # ---------------------------------
    # - trusted-zone: Source of curated, validated images (required)
    # - augmentation-zone: Destination for augmented images (created if missing)
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
        print(f" Created augmentation-zone bucket")
    else:
        print(f" Augmentation-zone bucket exists")
    
    print(f" Connected to MinIO: {minio_host}")
    return client


def _load_metadata_lookup(client):
    # Load metadata from trusted-zone and create UUID-to-taxonomy lookup.
    # PURPOSE
    # -------
    # Taxonomic information is used for:
    # 1. Stratified sampling across families
    # 2. Organizing augmented images in hierarchical folders
    # 3. Enabling family-balanced training datasets
    # Returns:
    #     Dict[str, Dict]: UUID -> {kingdom, class, family, species} mapping
    trusted_zone = "trusted-zone"
    metadata_prefix = "metadata/"
    
    # Find latest metadata CSV
    metadata_objs = [
        obj.object_name for obj in client.list_objects(trusted_zone, prefix=metadata_prefix, recursive=True)
        if obj.object_name.lower().endswith(".csv")
    ]
    
    if not metadata_objs:
        print("  Warning: No metadata found. Using 'Unknown' for taxonomic information.")
        return {}
    
    metadata_objs.sort(reverse=True)
    latest_meta = metadata_objs[0]
    
    # Load and parse metadata
    resp = client.get_object(trusted_zone, latest_meta)
    data = resp.read()
    resp.close()
    resp.release_conn()
    
    metadata_df = pd.read_csv(io.BytesIO(data))
    
    # Build lookup dictionary
    metadata_lookup = {}
    for _, row in metadata_df.iterrows():
        uuid_val = row.get('uuid')
        if pd.notna(uuid_val):
            def clean_value(value):
                # Normalize taxonomy values for folder names.
                val = str(value).strip() if pd.notna(value) else 'Unknown'
                return val.replace(" ", "_") if val and val != 'nan' and val != 'None' else 'Unknown'
            
            metadata_lookup[str(uuid_val)] = {
                'kingdom': clean_value(row.get('kingdom', 'Unknown')),
                'class': clean_value(row.get('class', 'Unknown')),
                'family': clean_value(row.get('family', 'Unknown')),
                'species': clean_value(row.get('species', 'Unknown'))
            }
    
    print(f"  Loaded metadata for {len(metadata_lookup)} records")
    return metadata_lookup


def _load_images_from_trusted(client):
    # Load image object references from trusted-zone.
    # Returns:
    #     List[Object]: MinIO object references for all images
    trusted_zone = "trusted-zone"
    images_prefix = "images/"
    
    # Get all image objects (JPEG and PNG formats)
    image_objects = [
        obj for obj in client.list_objects(trusted_zone, prefix=images_prefix, recursive=True)
        if obj.object_name.lower().endswith(('.jpg', '.jpeg', '.png'))
    ]
    
    return image_objects


def _get_available_augmentations():
    # Return list of available augmentation function identifiers.
    # AUGMENTATION SELECTION RATIONALE
    # ---------------------------------
    # Augmentations are chosen based on their relevance to wildlife imagery:
    # **Geometric (Spatial):**
    # - random_resized_crop: Camera zoom/distance variations
    # - center_crop: Consistent framing, reduces edge artifacts
    # - rotation: Animals appear at various orientations
    # - translation: Animals not always centered in frame
    # - partial_crop: Handles partially visible subjects
    # **Photometric (Appearance):**
    # - brightness_jitter: Sunlight/shadow variations in natural habitats
    # - contrast_jitter: Weather and atmospheric conditions
    # - saturation_jitter: Camera white balance differences
    # - gaussian_noise: Sensor noise in various cameras
    # - gaussian_blur: Motion blur or focus variation
    # Each augmentation is applied independently (not sequentially) to create
    # distinct variations, maximizing data diversity.
    # Returns:
    #     List[str]: Augmentation function identifiers
    return [
        'random_resized_crop',
        'center_crop',
        'rotation',
        'translation',
        'brightness_jitter',
        'contrast_jitter',
        'saturation_jitter',
        'gaussian_noise',
        'gaussian_blur',
        'partial_crop'
    ]


def _extract_uuid_from_path(image_path):
    # Extract UUID from image path.
    # IMPLEMENTATION NOTES
    # --------------------
    # Handles multiple path structures:
    # - Flat: images/uuid.jpg
    # - Hierarchical: images/kingdom/class/family/species/uuid.jpg
    # - Augmented: images/.../uuid_augtype.jpg (extracts base UUID)
    # Args:
    #     image_path: Full MinIO object path
    # Returns:
    #     str: Base UUID without augmentation suffix
    filename = os.path.basename(image_path)
    name_without_ext, _ = os.path.splitext(filename)
    
    # Handle augmentation suffix: uuid_augtype -> uuid
    parts = name_without_ext.split('_')
    base_uuid = parts[0]
    
    return base_uuid


def _select_images_by_family(client, image_objects, target_count, metadata_lookup):
    # Select images with balanced representation across taxonomic families.
    # STRATIFIED SAMPLING STRATEGY
    # -----------------------------
    # Balanced family representation is crucial for:
    # 1. **Avoiding Bias**: Prevents overrepresentation of common families
    #    (e.g., Colubridae) which could skew learned embeddings.
    # 2. **Rare Class Learning**: Ensures rare families contribute to training,
    #    improving retrieval performance for uncommon species.
    # 3. **Evaluation Fairness**: Enables fair comparison across families
    #    in A/B evaluation (per-family metrics).
    # ALGORITHM
    # ---------
    # 1. Group images by family using metadata lookup
    # 2. Calculate samples per family: target_count // num_families
    # 3. First pass: Sample equally from each family (use all if insufficient)
    # 4. Second pass: Fill shortfall from families with remaining images
    # 5. Third pass: Distribute remainder for exact target count
    # Args:
    #     client: MinIO client
    #     image_objects: List of image object references
    #     target_count: Total number of images to select
    #     metadata_lookup: UUID -> taxonomy mapping
    # Returns:
    #     List[Object]: Selected image objects with balanced family distribution
    # Group images by taxonomic family
    family_groups = {}
    
    for obj in image_objects:
        image_path = obj.object_name
        base_uuid = _extract_uuid_from_path(image_path)
        
        # Get family from metadata
        taxonomic_info = metadata_lookup.get(base_uuid, {})
        family = taxonomic_info.get('family', 'Unknown')
        
        if family not in family_groups:
            family_groups[family] = []
        
        family_groups[family].append(obj)
    
    if not family_groups:
        return []
    
    # Calculate balanced allocation
    num_families = len(family_groups)
    samples_per_family = target_count // num_families
    remainder = target_count % num_families
    
    print(f"  Found {num_families} families in dataset")
    print(f"  Target: {samples_per_family} images per family")
    if remainder > 0:
        print(f"  Note: {remainder} extra images will be distributed across families")
    
    # Sampling state tracking
    selected_images = []
    family_counts = {}
    sampled_uuids = set()
    
    # FIRST PASS: Equal sampling from each family
    for family, items in family_groups.items():
        available = len(items)
        requested = samples_per_family
        
        if available >= requested:
            sampled = random.sample(items, requested)
            selected_images.extend(sampled)
            family_counts[family] = requested
            sampled_uuids.update(_extract_uuid_from_path(obj.object_name) for obj in sampled)
        else:
            # Use all available if family has fewer than requested
            selected_images.extend(items)
            family_counts[family] = available
            sampled_uuids.update(_extract_uuid_from_path(obj.object_name) for obj in items)
            print(f"    Warning: Family '{family}' has only {available} images (requested {requested})")
    
    # Calculate shortfall
    images_needed = target_count - len(selected_images)
    
    # SECOND PASS: Fill shortfall from families with remaining images
    if images_needed > 0:
        available_images_pool = []
        for family, items in family_groups.items():
            remaining_items = [obj for obj in items 
                             if _extract_uuid_from_path(obj.object_name) not in sampled_uuids]
            available_images_pool.extend(remaining_items)
        
        if available_images_pool:
            random.shuffle(available_images_pool)
            to_sample = min(images_needed, len(available_images_pool))
            additional_samples = available_images_pool[:to_sample]
            selected_images.extend(additional_samples)
            sampled_uuids.update(_extract_uuid_from_path(obj.object_name) for obj in additional_samples)
            
            # Update family counts
            for obj in additional_samples:
                base_uuid = _extract_uuid_from_path(obj.object_name)
                taxonomic_info = metadata_lookup.get(base_uuid, {})
                family = taxonomic_info.get('family', 'Unknown')
                family_counts[family] = family_counts.get(family, 0) + 1
            
            if to_sample < images_needed:
                print(f"    Warning: Only {to_sample} additional images available (needed {images_needed})")
    
    # THIRD PASS: Distribute remainder
    if remainder > 0 and len(selected_images) < target_count:
        families_with_extra = []
        for family, items in family_groups.items():
            already_sampled = family_counts.get(family, 0)
            available = len(items)
            if available > already_sampled:
                families_with_extra.append((family, available - already_sampled))
        
        if families_with_extra:
            random.shuffle(families_with_extra)
            extra_needed = min(remainder, target_count - len(selected_images))
            
            for family, available_extra in families_with_extra:
                if extra_needed <= 0:
                    break
                
                all_items = family_groups[family]
                remaining_items = [obj for obj in all_items 
                                 if _extract_uuid_from_path(obj.object_name) not in sampled_uuids]
                
                if remaining_items:
                    to_sample = min(extra_needed, len(remaining_items))
                    sampled = random.sample(remaining_items, to_sample)
                    selected_images.extend(sampled)
                    sampled_uuids.update(_extract_uuid_from_path(obj.object_name) for obj in sampled)
                    family_counts[family] = family_counts.get(family, 0) + to_sample
                    extra_needed -= to_sample
    
    # Final shuffle and trim
    random.shuffle(selected_images)
    if len(selected_images) > target_count:
        selected_images = selected_images[:target_count]
    
    # Summary
    print(f"  Final selection: {len(selected_images)} images")
    print(f"  Distribution by family:")
    for family in sorted(family_counts.keys()):
        count = family_counts[family]
        print(f"    {family}: {count} images")
    
    return selected_images


def _apply_image_augmentations(client, image_objects, num_augmentations=3, metadata_lookup=None):
    # Apply random augmentations to images from trusted-zone.
    # AUGMENTATION STRATEGY
    # ---------------------
    # For each image, we apply N INDEPENDENT augmentations, creating N separate
    # augmented images. This is different from sequential augmentation where
    # transforms are chained.
    # **Independent Augmentation Benefits:**
    # - Each augmented image represents a single, interpretable transformation
    # - Easier to analyze which augmentations help/hurt performance
    # - Avoids compounding artifacts from chained transforms
    # REPRODUCIBILITY (Constraint 9)
    # ------------------------------
    # Fixed random seed (42) ensures:
    # - Same images get same augmentation selections
    # - Experiments can be exactly reproduced
    # - Debug sessions are deterministic
    # Args:
    #     client: MinIO client
    #     image_objects: List of image objects to augment
    #     num_augmentations: Number of augmented variants per image
    #     metadata_lookup: UUID -> taxonomy mapping for folder structure
    # Returns:
    #     List[Tuple]: (original_path, augmented_image, aug_types, new_path, taxonomy)
    trusted_zone = "trusted-zone"
    augmented_images = []
    available_augmentations = _get_available_augmentations()
    
    # Set random seed for reproducibility
    random.seed(42)
    
    if metadata_lookup is None:
        metadata_lookup = {}
    
    for obj in image_objects:
        image_path = obj.object_name
        
        try:
            # Load original image
            data = client.get_object(trusted_zone, image_path)
            img_bytes = data.read()
            data.close()
            data.release_conn()
            
            original_image = Image.open(io.BytesIO(img_bytes)).convert("RGB")
            
            # Get metadata
            base_uuid = _extract_uuid_from_path(image_path)
            taxonomic_info = metadata_lookup.get(base_uuid, {
                'kingdom': 'Unknown',
                'class': 'Unknown',
                'family': 'Unknown',
                'species': 'Unknown'
            })
            
            # Get file extension
            _, ext = os.path.splitext(image_path)
            if not ext:
                ext = '.jpg'
            
            # Randomly select augmentations (without replacement)
            selected_augmentations = random.sample(
                available_augmentations, 
                min(num_augmentations, len(available_augmentations))
            )
            
            # Apply each augmentation independently
            for aug_name in selected_augmentations:
                # Apply to a COPY of original (not sequential)
                augmented_image = _apply_single_augmentation(original_image.copy(), aug_name)
                
                # Build structured output path
                kingdom = taxonomic_info.get('kingdom', 'Unknown')
                class_name = taxonomic_info.get('class', 'Unknown')
                family = taxonomic_info.get('family', 'Unknown')
                species = taxonomic_info.get('species', 'Unknown')
                
                new_filename = f"images/{kingdom}/{class_name}/{family}/{species}/{base_uuid}_{aug_name}{ext}"
                
                augmented_images.append((
                    image_path,
                    augmented_image,
                    [aug_name],
                    new_filename,
                    taxonomic_info
                ))
            
        except Exception as e:
            print(f" Warning: Could not process image {image_path}: {e}")
            continue
    
    return augmented_images


def _apply_single_augmentation(image, augmentation_name):
    # Apply a single augmentation transform to an image.
    # AUGMENTATION IMPLEMENTATIONS
    # ----------------------------
    # Each augmentation is designed to simulate realistic variations:
    # **Geometric Transforms:**
    # - random_resized_crop: Scale 0.7-1.0, ratio 0.8-1.2 (zoom/framing)
    # - center_crop: 85% of original size (stabilization)
    # - rotation: ±10-15° (orientation variance)
    # - translation: ±10% shift (camera movement)
    # - partial_crop: 10-20% edge removal (occlusion)
    # **Photometric Transforms:**
    # - brightness_jitter: Factor 0.7-1.3 (lighting)
    # - contrast_jitter: Factor 0.8-1.2 (atmosphere)
    # - saturation_jitter: Factor 0.7-1.3 (color balance)
    # - gaussian_noise: σ=10 (sensor noise)
    # - gaussian_blur: Radius 0.5-1.5 (focus/motion)
    # FILL COLOR
    # ----------
    # Gray (128,128,128) is used for geometric transforms to avoid
    # introducing strong color artifacts at image boundaries.
    # Args:
    #     image: PIL Image to transform
    #     augmentation_name: Augmentation identifier
    # Returns:
    #     PIL Image: Augmented image
    if augmentation_name == 'random_resized_crop':
        # Random resized crop: Simulates zoom & framing variations
        # Scale 0.7-1.0 captures zoom range, ratio 0.8-1.2 allows slight aspect changes
        size = image.size
        transform = transforms.RandomResizedCrop(size=size, scale=(0.7, 1.0), ratio=(0.8, 1.2))
        return transform(image)
    
    elif augmentation_name == 'center_crop':
        # Center crop: Removes edges, focuses on central subject
        # 85% crop maintains most content while removing edge artifacts
        size = image.size
        crop_size = (int(size[0] * 0.85), int(size[1] * 0.85))
        transform = transforms.CenterCrop(crop_size)
        cropped = transform(image)
        return cropped.resize(size, Image.Resampling.LANCZOS)
    
    elif augmentation_name == 'rotation':
        # Small rotation: Handles non-horizontal orientations
        # ±10-15° is typical for hand-held camera tilt
        angle = random.choice([-15, -12, -10, 10, 12, 15])
        return image.rotate(angle, expand=False, fillcolor=(128, 128, 128))
    
    elif augmentation_name == 'translation':
        # Translation: Camera/subject movement tolerance
        # ±10% shift simulates slight reframing
        width, height = image.size
        x_offset = random.randint(-int(width * 0.1), int(width * 0.1))
        y_offset = random.randint(-int(height * 0.1), int(height * 0.1))
        return image.transform(
            image.size,
            Image.Transform.AFFINE,
            (1, 0, x_offset, 0, 1, y_offset),
            fillcolor=(128, 128, 128)
        )
    
    elif augmentation_name == 'brightness_jitter':
        # Brightness jitter: Lighting condition variations
        # Factor 0.7-1.3 covers shade to direct sunlight range
        enhancer = ImageEnhance.Brightness(image)
        factor = random.uniform(0.7, 1.3)
        return enhancer.enhance(factor)
    
    elif augmentation_name == 'contrast_jitter':
        # Contrast jitter: Atmospheric/weather condition effects
        # Factor 0.8-1.2 is conservative to avoid unrealistic results
        enhancer = ImageEnhance.Contrast(image)
        factor = random.uniform(0.8, 1.2)
        return enhancer.enhance(factor)
    
    elif augmentation_name == 'saturation_jitter':
        # Saturation jitter: Camera white balance differences
        # Factor 0.7-1.3 simulates various camera settings
        enhancer = ImageEnhance.Color(image)
        factor = random.uniform(0.7, 1.3)
        return enhancer.enhance(factor)
    
    elif augmentation_name == 'gaussian_noise':
        # Gaussian noise: Sensor noise simulation
        # σ=10 is subtle but noticeable, typical of low-light sensors
        img_array = np.array(image, dtype=np.float32)
        noise = np.random.normal(0, 10, img_array.shape).astype(np.float32)
        noisy_array = np.clip(img_array + noise, 0, 255).astype(np.uint8)
        return Image.fromarray(noisy_array)
    
    elif augmentation_name == 'gaussian_blur':
        # Gaussian blur: Motion or focus variation
        # Radius 0.5-1.5 creates subtle blur, not severe degradation
        return image.filter(ImageFilter.GaussianBlur(radius=random.uniform(0.5, 1.5)))
    
    elif augmentation_name == 'partial_crop':
        # Partial cropping: Simulates partially visible subjects
        # 10-20% removal from one edge, common in wildlife photography
        width, height = image.size
        crop_percent = random.uniform(0.1, 0.2)
        side = random.choice(['left', 'right', 'top', 'bottom'])
        
        if side == 'left':
            left = int(width * crop_percent)
            box = (left, 0, width, height)
        elif side == 'right':
            box = (0, 0, int(width * (1 - crop_percent)), height)
        elif side == 'top':
            top = int(height * crop_percent)
            box = (0, top, width, height)
        else:  # bottom
            box = (0, 0, width, int(height * (1 - crop_percent)))
        
        cropped = image.crop(box)
        return cropped.resize((width, height), Image.Resampling.LANCZOS)
    
    else:
        # Unknown augmentation: return original (fail-safe)
        return image


def _clear_augmentation_zone_images(client):
    # Delete all existing augmented images from augmentation-zone.
    # IDEMPOTENCY PRINCIPLE
    # ---------------------
    # Clearing before generation ensures:
    # 1. Consistent state regardless of prior runs
    # 2. No stale data accumulation
    # 3. Reproducible experiments without manual cleanup
    augmentation_zone = "augmentation-zone"
    
    if not client.bucket_exists(augmentation_zone):
        print("  Augmentation-zone does not exist. Nothing to clear.")
        return
    
    try:
        images_prefix = "images/"
        objects_to_delete = list(client.list_objects(augmentation_zone, prefix=images_prefix, recursive=True))
        
        if not objects_to_delete:
            print("  No existing augmented images found in augmentation-zone.")
            return
        
        print(f"  Found {len(objects_to_delete)} existing augmented images to delete...")
        
        deleted_count = 0
        for obj in objects_to_delete:
            try:
                client.remove_object(augmentation_zone, obj.object_name)
                deleted_count += 1
            except Exception as e:
                print(f"  Warning: Could not delete {obj.object_name}: {e}")
        
        print(f"  Deleted {deleted_count} existing augmented images")
        
    except Exception as e:
        print(f"  Warning: Error clearing augmentation-zone: {e}")


def _save_images_to_augmentation(client, augmented_images):
    # Save augmented images to augmentation-zone with structured folder format.
    # FOLDER STRUCTURE (Constraint 6: Meaningful Names)
    # --------------------------------------------------
    # images/{kingdom}/{class}/{family}/{species}/{uuid}_{augmentation}.{ext}
    # Example: images/Animalia/Reptilia/Elapidae/naja/abc123_rotation.jpg
    # This hierarchical structure enables:
    # - Taxonomic browsing and inspection
    # - Family-level data validation
    # - Easy integration with existing trusted-zone structure
    # QUALITY SETTINGS
    # ----------------
    # - JPEG: Quality 95 (high quality, reasonable file size)
    # - PNG: Lossless (when original is PNG)
    # Args:
    #     client: MinIO client
    #     augmented_images: List of (path, image, aug_types, new_path, taxonomy) tuples
    augmentation_zone = "augmentation-zone"
    
    saved_count = 0
    skipped_count = 0
    
    for item in augmented_images:
        # Handle backward compatibility with different tuple formats
        if len(item) == 4:
            original_path, image, augmentation_types, new_filename = item
            taxonomic_info = {}
        else:
            original_path, image, augmentation_types, new_filename, taxonomic_info = item
        
        try:
            # Serialize image to bytes
            img_buffer = io.BytesIO()
            
            ext = os.path.splitext(new_filename)[1].lower()
            if ext in ['.jpg', '.jpeg']:
                image.save(img_buffer, format='JPEG', quality=95)
            elif ext == '.png':
                image.save(img_buffer, format='PNG')
            else:
                image.save(img_buffer, format='JPEG', quality=95)
            
            img_buffer.seek(0)
            
            # Upload to MinIO
            client.put_object(
                augmentation_zone,
                new_filename,
                img_buffer,
                length=img_buffer.getbuffer().nbytes,
                content_type=f"image/{ext[1:]}" if ext else "image/jpeg"
            )
            
            saved_count += 1
            if saved_count % 10 == 0:
                print(f"   Saved {saved_count} images...")
                
        except Exception as e:
            print(f" Warning: Could not save image {new_filename}: {e}")
            skipped_count += 1
            continue
    
    print(f" Saved {saved_count} augmented images")
    if skipped_count > 0:
        print(f"   Skipped {skipped_count} images due to errors")


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    # Configuration from environment variables (supports containerized execution)
    minio_host = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'admin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'password123')
    
    # Environment variables for non-interactive execution (e.g., CI/CD)
    num_images = None
    num_augmentations = None
    
    if 'AUGMENTATION_NUM_IMAGES' in os.environ:
        num_images = int(os.getenv('AUGMENTATION_NUM_IMAGES', '100'))
    if 'AUGMENTATION_NUM_PER_IMAGE' in os.environ:
        num_augmentations = int(os.getenv('AUGMENTATION_NUM_PER_IMAGE', '3'))
    
    process_image_augmentation(
        minio_host=minio_host,
        access_key=access_key,
        secret_key=secret_key,
        num_images=num_images,
        num_augmentations_per_image=num_augmentations
    )
