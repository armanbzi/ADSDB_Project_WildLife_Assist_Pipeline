# Fine-Tuning Zone

This zone handles fine-tuning of CLIP models using efficient parameter-efficient fine-tuning techniques (LoRA/QLoRA). It addresses Constraint 1 (Fine-Tuning) and Constraint 3 (Data Augmentation) from the ADSDB project guidelines.

## Overview

The Fine-Tuning Zone extends the pipeline with the capability to fine-tune embedding models on your specific wildlife dataset. This allows the model to better adapt to your domain-specific data while maintaining computational efficiency through parameter-efficient methods.

## Components

### 1. augmentation_text.py
Implements text data augmentation for CLIP model fine-tuning using template-based augmentation.

**Features:**
- Generates natural language descriptions from metadata
- Creates query variations (e.g., "show me a cobra", "what is Naja naja")
- Template-based augmentation for reproducibility
- Stores augmented text in augmentation-zone

**Usage:**
```python
from augmentation_text import process_text_augmentation

process_text_augmentation(
    minio_host="localhost:9000",
    access_key="admin",
    secret_key="password123"
)
```

### 2. augmentation_image.py
Implements image data augmentation using geometric and photometric transformations.

**Features:**
- Geometric transformations (rotation, crop, translation)
- Photometric transformations (brightness, contrast, saturation)
- Random augmentations for training robustness
- Stores augmented images in augmentation-zone

**Usage:**
```python
from augmentation_image import process_image_augmentation

process_image_augmentation(
    minio_host="localhost:9000",
    access_key="admin",
    secret_key="password123"
)
```

### 3. Fine_Tuning_CLIP.py
Main script for fine-tuning CLIP models using LoRA (Low-Rank Adaptation) or QLoRA (Quantized LoRA).

**Features:**
- Loads image-text pairs from trusted-zone and augmentation-zone
- Fine-tunes CLIP model with LoRA/QLoRA adapters
- Saves checkpoints after each epoch
- Tracks training metrics (loss, learning rate)
- Supports both GPU (CUDA) and CPU training
- Automatic QLoRA fallback to LoRA on CPU

**Usage:**
```python
from Fine_Tuning_CLIP import process_fine_tuning_clip

process_fine_tuning_clip(
    minio_host="localhost:9000",
    access_key="admin",
    secret_key="password123",
    model_name="ViT-B-32",
    checkpoint="laion2b_s34b_b79k",
    use_qlora=True,
    lora_r_text=16,
    lora_r_vision=8,
    lora_alpha=16,
    batch_size=8,
    num_epochs=3,
    learning_rate=1e-4
)
```

### 4. Checkpoint_Manager.py
Manages fine-tuned checkpoints, allowing selection and activation of specific checkpoints.

**Features:**
- List available checkpoints
- Set active checkpoint
- Get checkpoint configuration
- Validate checkpoint integrity
- Clear active checkpoint (revert to baseline)

**Usage:**
```bash
# List all checkpoints
python Checkpoint_Manager.py list

# Set active checkpoint
python Checkpoint_Manager.py set checkpoint_epoch_3

# Clear active checkpoint (revert to baseline)
python Checkpoint_Manager.py clear

# Get active checkpoint info
python Checkpoint_Manager.py get
```

### 5. AB_Evaluation.py
Performs A/B evaluation comparing baseline CLIP model with fine-tuned checkpoints.

**Features:**
- Evaluates both models on the same test set
- Computes similarity scores (cosine similarity)
- Calculates top-k retrieval accuracy (k=1, 5, 10)
- Computes Mean Reciprocal Rank (MRR)
- Measures inference time and memory usage
- Generates visualization plots (similarity distributions, improvement metrics)
- Shows improvement metrics and statistical comparisons

**Usage:**
```python
from AB_Evaluation import evaluate_ab_comparison

evaluate_ab_comparison(
    minio_host="localhost:9000",
    access_key="admin",
    secret_key="password123",
    test_size=100,
    baseline_model_name="ViT-B-32",
    baseline_checkpoint="laion2b_s34b_b79k"
)
```

## Workflow

The complete fine-tuning workflow consists of five sequential steps:

1. **Text Augmentation:**
   - Run `augmentation_text.py` to generate text variations
   - Augmented text is stored in augmentation-zone

2. **Image Augmentation:**
   - Run `augmentation_image.py` to generate image variations
   - Augmented images are stored in augmentation-zone

3. **Fine-tune the model:**
   - Run `Fine_Tuning_CLIP.py` to fine-tune CLIP on your data
   - Training data is loaded from trusted-zone and augmentation-zone
   - Checkpoints are saved in `Fine-Tuning-Zone/checkpoints/` after each epoch

4. **Manage checkpoints:**
   - Use `Checkpoint_Manager.py` to list and select checkpoints
   - Set an active checkpoint to use in downstream tasks

5. **Evaluate performance:**
   - Run `AB_Evaluation.py` to compare baseline vs fine-tuned models
   - Review metrics (top-k accuracy, MRR, similarity scores) to assess improvement

## Integration with Other Zones

### Exploitation Zone
The Exploitation Zone scripts (`Exploitation_Images.py`, `Exploitation_Multimodal.py`) automatically:
- Detect active fine-tuned checkpoints
- Fall back to baseline model if no checkpoint is active
- Use fine-tuned embeddings for improved similarity search

### Task Scripts
The Multi-Modal Task scripts (`Same_Modality_Search.py`, `Multimodal_Similarity_Task.py`, `Generative_Task.py`) support:
- User selection between baseline and fine-tuned models
- Automatic detection of active checkpoints
- Improved retrieval performance with fine-tuned models

## Technical Details

### LoRA (Low-Rank Adaptation)
- Reduces trainable parameters to <1% of original model
- Adds low-rank matrices (A and B) to attention layers (q_proj, v_proj)
- Update formula: W' = W + BA where r << min(d,k)
- Preserves original model weights
- Text encoder typically uses higher rank (r=16) for semantic understanding
- Vision encoder typically uses lower rank (r=8) as visual features need less adaptation

### QLoRA (Quantized LoRA)
- Combines 4-bit quantization with LoRA
- Base model stored in 4-bit (8x memory reduction)
- Adapters trained in FP16
- Further reduces memory requirements
- Enables fine-tuning on consumer GPUs
- Automatically falls back to LoRA on CPU systems

### Contrastive Loss (InfoNCE)
- CLIP uses contrastive learning to align image-text pairs in embedding space
- For a batch of N pairs, the loss maximizes similarity of matching pairs
- While minimizing similarity to N-1 negative pairs per sample
- Negatives are created implicitly by CLIP's contrastive loss

### Checkpoint Structure
```
checkpoints/
├── checkpoint_epoch_1/
│   ├── config.json              # Model configuration
│   ├── model.safetensors        # Model weights
│   ├── training_config.json     # Training hyperparameters and history
│   ├── text_encoder/            # LoRA adapters for text encoder
│   │   ├── adapter_config.json
│   │   └── adapter_model.safetensors
│   └── vision_encoder/          # LoRA adapters for vision encoder
│       ├── adapter_config.json
│       └── adapter_model.safetensors
├── checkpoint_epoch_2/
├── checkpoint_epoch_3/
├── final_checkpoint/
└── active_checkpoint.json       # Active checkpoint configuration
```

## Dependencies

Additional dependencies required for fine-tuning (included in main `requirements.txt`):
- `peft>=0.5.0` - Parameter-Efficient Fine-Tuning (LoRA/QLoRA)
- `transformers>=4.35.0` - Hugging Face Transformers (CLIP models)
- `huggingface_hub>=0.34.0,<1.0.0` - Model and dataset access
- `bitsandbytes>=0.41.0` - 4-bit quantization support for QLoRA
- `accelerate>=0.20.0` - Training acceleration and optimization
- `torchvision>=0.15.0` - Image transformations for augmentation

## Hardware Requirements

- **GPU (Recommended)**: CUDA-capable GPU for faster training
  - QLoRA: Requires GPU with sufficient VRAM (4GB+ recommended)
  - LoRA: Works on GPU or CPU, but GPU is significantly faster
- **CPU**: LoRA training is supported but will be slower
  - QLoRA automatically falls back to LoRA on CPU systems
- **Memory**: 
  - QLoRA: Lower memory requirements (~4-8GB VRAM)
  - LoRA: Higher memory requirements (~8-16GB VRAM or RAM)

## Notes

- Fine-tuning is computationally intensive; ensure adequate GPU memory
- QLoRA is recommended for limited hardware (consumer GPUs)
- Training time depends on dataset size, batch size, and hardware
- Checkpoints can be large; manage disk space accordingly
- The complete workflow (augmentation → training → evaluation) can be run via the orchestrator's "Run Fine-tune" option
- Individual scripts can also be executed separately for flexibility

