"""
-Arman Bazarchi-
Multimodal Similarity Search Task

Performs similarity searches using both text and image queries
Returns mixed results with both images and text descriptions

- Connects to a ChromaDB multimodal collection where text and image embeddings are stored.
- Supports text-based queries to retrieve similar items with both descriptions and images.
- Supports image-based queries to retrieve similar items with both descriptions and images.
- Returns results enriched with enough details (species, scientific name, kingdom, class, family, etc.).
- Displays results with images and textual description.
- Displays basic collection statistics (total items, distribution by kingdom, class, and family).
- Users cannot query with both text and image at same time here, queries are performed one modality at a time.

 Updated to let user to set which model to use(baseline or fine tuned)

"""

import chromadb
from chromadb.utils import embedding_functions
import pandas as pd
import io
import os
import secrets
from minio import Minio
import sys
from PIL import Image
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import importlib.util
from langchain_experimental.open_clip import OpenCLIPEmbeddings

# ==============================
#   Model Selection Functions
# ==============================

def _get_active_checkpoint():
    """Get active checkpoint from Checkpoint_Manager."""
    try:
        try:
            SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            SCRIPT_DIR = os.getcwd()
        
        # Try to find Checkpoint_Manager
        checkpoint_manager_path = None
        possible_paths = [
            os.path.join(SCRIPT_DIR, "../../Fine-Tuning-Zone/scripts/Checkpoint_Manager.py"),
            os.path.join(SCRIPT_DIR, "../Fine-Tuning-Zone/scripts/Checkpoint_Manager.py"),
            "Fine-Tuning-Zone/scripts/Checkpoint_Manager.py"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                checkpoint_manager_path = path
                break
        
        if checkpoint_manager_path:
            spec = importlib.util.spec_from_file_location("Checkpoint_Manager", checkpoint_manager_path)
            checkpoint_manager = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(checkpoint_manager)
            return checkpoint_manager.get_active_checkpoint()
    except Exception as e:
        pass
    return None

def _select_model():
    """Let user choose between baseline and fine-tuned model."""
    active_checkpoint = _get_active_checkpoint()
    
    if active_checkpoint:
        checkpoint_name = active_checkpoint.get('checkpoint_name', 'Unknown')
        print(f"\n Active fine-tuned checkpoint found: {checkpoint_name}")
        print(" Choose model to use:")
        print(" 1. Baseline model (ViT-B-32)")
        print(f" 2. Fine-tuned model ({checkpoint_name})")
        
        # Check if non-interactive mode
        is_non_interactive = (
            os.getenv('CI') == 'true' or
            os.getenv('GITHUB_ACTIONS') == 'true' or
            os.getenv('GITLAB_CI') == 'true' or
            '--non-interactive' in sys.argv or
            not sys.stdin.isatty()
        )
        
        if is_non_interactive:
            choice = "1"
            print(" Non-interactive mode: using baseline model")
        else:
            while True:
                choice = input("\n Enter choice (1 or 2): ").strip()
                if choice in ["1", "2"]:
                    break
                print(" Invalid choice. Please enter 1 or 2.")
        
        if choice == "2":
            return "fine_tuned", active_checkpoint
        else:
            return "baseline", None
    else:
        print("\n No active fine-tuned checkpoint found. Using baseline model.")
        return "baseline", None

def _load_embedding_model(model_type, checkpoint_info=None):
    """Load embedding model (baseline or fine-tuned)."""
    if model_type == "baseline":
        print(" Loading baseline model (ViT-B-32)...")
        return OpenCLIPEmbeddings(
            model_name="ViT-B-32",
            checkpoint="laion2b_s34b_b79k"
        )
    else:
        # Load fine-tuned model
        print(" Loading fine-tuned model...")
        try:
            import torch
            from transformers import CLIPModel, CLIPProcessor
            from peft import PeftModel
            
            checkpoint_path = checkpoint_info.get("checkpoint_path")
            if not checkpoint_path or not os.path.exists(checkpoint_path):
                print(" Warning: Checkpoint path not found. Falling back to baseline.")
                return OpenCLIPEmbeddings(
                    model_name="ViT-B-32",
                    checkpoint="laion2b_s34b_b79k"
                )
            
            # Load base model
            model_name = "openai/clip-vit-base-patch32"
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            model = CLIPModel.from_pretrained(model_name)
            processor = CLIPProcessor.from_pretrained(model_name)
            
            # Load LoRA adapters with wrappers to filter invalid kwargs
            text_encoder_path = os.path.join(checkpoint_path, "text_encoder")
            vision_encoder_path = os.path.join(checkpoint_path, "vision_encoder")
            
            # Wrapper classes to filter invalid kwargs
            class VisionModelWrapper(torch.nn.Module):
                """Wrapper for vision model to filter kwargs while preserving LoRA functionality."""
                def __init__(self, peft_model):
                    super().__init__()
                    self.peft_model = peft_model
                    if hasattr(peft_model, 'config'):
                        self.config = peft_model.config
                    if hasattr(peft_model, 'base_model') and hasattr(peft_model.base_model, 'model'):
                        self._lora_model = peft_model.base_model.model
                    elif hasattr(peft_model, 'base_model'):
                        self._lora_model = peft_model.base_model
                    else:
                        self._lora_model = peft_model
                
                def forward(self, *args, **kwargs):
                    """Filter kwargs and call model with LoRA adapters active."""
                    pixel_values = kwargs.get("pixel_values", None)
                    if pixel_values is None and len(args) > 0:
                        pixel_values = args[0]
                    valid_kwargs = {}
                    valid_keys = ['pixel_values', 'output_attentions', 'output_hidden_states', 
                                 'interpolate_pos_encoding', 'return_dict']
                    for key in valid_keys:
                        if key in kwargs:
                            valid_kwargs[key] = kwargs[key]
                    if pixel_values is not None:
                        valid_kwargs['pixel_values'] = pixel_values
                    return self._lora_model(**valid_kwargs)
            
            class TextModelWrapper(torch.nn.Module):
                """Wrapper for text model to filter kwargs while preserving LoRA functionality."""
                def __init__(self, peft_model):
                    super().__init__()
                    self.peft_model = peft_model
                    if hasattr(peft_model, 'config'):
                        self.config = peft_model.config
                    if hasattr(peft_model, 'base_model') and hasattr(peft_model.base_model, 'model'):
                        self._lora_model = peft_model.base_model.model
                    elif hasattr(peft_model, 'base_model'):
                        self._lora_model = peft_model.base_model
                    else:
                        self._lora_model = peft_model
                
                def forward(self, *args, **kwargs):
                    """Filter kwargs and call model with LoRA adapters active."""
                    valid_kwargs = {}
                    valid_keys = ['input_ids', 'attention_mask', 'position_ids', 
                                 'output_attentions', 'output_hidden_states', 'return_dict']
                    for key in valid_keys:
                        if key in kwargs:
                            valid_kwargs[key] = kwargs[key]
                    if 'input_ids' not in valid_kwargs and len(args) > 0:
                        valid_kwargs['input_ids'] = args[0]
                    return self._lora_model(**valid_kwargs)
            
            if os.path.exists(text_encoder_path):
                peft_text_model = PeftModel.from_pretrained(model.text_model, text_encoder_path)
                peft_text_model.eval()
                model.text_model = TextModelWrapper(peft_text_model)
            
            if os.path.exists(vision_encoder_path):
                peft_vision_model = PeftModel.from_pretrained(model.vision_model, vision_encoder_path)
                peft_vision_model.eval()
                model.vision_model = VisionModelWrapper(peft_vision_model)
            
            model.eval()
            model = model.to(device)
            
            # Create wrapper class that mimics OpenCLIPEmbeddings interface
            class FineTunedCLIPEmbeddings:
                def __init__(self, model, processor, device):
                    self.model = model
                    self.processor = processor
                    self.device = device
                
                def embed_image(self, images):
                    """Embed images using fine-tuned model. Returns list of embeddings."""
                    import torch
                    from PIL import Image
                    
                    # Handle single image or list of images
                    if isinstance(images, str):
                        images = [images]
                    elif not isinstance(images, list):
                        images = [images]
                    
                    pil_images = []
                    for img in images:
                        if isinstance(img, str):
                            pil_images.append(Image.open(img).convert("RGB"))
                        elif isinstance(img, Image.Image):
                            pil_images.append(img.convert("RGB"))
                        else:
                            raise ValueError(f"Unsupported image type: {type(img)}")
                    
                    inputs = self.processor(images=pil_images, return_tensors="pt").to(self.device)
                    with torch.no_grad():
                        image_features = self.model.get_image_features(**inputs)
                        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
                    
                    # Return list of embeddings (one per image)
                    return image_features.cpu().numpy().tolist()
            
            return FineTunedCLIPEmbeddings(model, processor, device)
            
        except Exception as e:
            print(f" Error loading fine-tuned model: {e}")
            print(" Falling back to baseline model.")
            import traceback
            traceback.print_exc()
            return OpenCLIPEmbeddings(
                model_name="ViT-B-32",
                checkpoint="laion2b_s34b_b79k"
            )

# Global variable to store selected model (to avoid re-selecting multiple times)
_selected_embedding_model = None

def _get_embedding_model():
    """Get embedding model (with selection if not already done)."""
    global _selected_embedding_model
    if _selected_embedding_model is None:
        model_type, checkpoint_info = _select_model()
        _selected_embedding_model = _load_embedding_model(model_type, checkpoint_info)
    return _selected_embedding_model

# ==============================
#          Functions
# ==============================

def get_minio_config():
    # Load MinIO configuration from environment variables (set by orchestrator).
    
    import os
    
    # Get configuration from environment variables (set by orchestrator)
    endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'admin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'admin123')
    
    print(f"Using MinIO configuration from environment variables: endpoint={endpoint}, access_key={access_key[:3]}***")
    return endpoint, access_key, secret_key

def setup_minio_connection(minio_endpoint, access_key, secret_key, trusted_bucket):
    # Setup and verify MinIO client connection.
    print(" Connecting to MinIO...")
    client = Minio(
        minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    
    # Verify trusted-zone bucket exists
    if not client.bucket_exists(trusted_bucket):
        sys.exit(f" ERROR: Bucket '{trusted_bucket}' does not exist in MinIO.")
    
    print(f" Connected to MinIO - Bucket '{trusted_bucket}' verified")
    return client

def setup_chromadb_connection(chroma_db_path, collection_name):
    # Setup ChromaDB client and collection.
    
    print(" Connecting to ChromaDB...")
    chroma_client = chromadb.PersistentClient(path=chroma_db_path)
    
    # Get the multimodal collection
    try:
        collection = chroma_client.get_collection(name=collection_name)
        print(f" Connected to collection: {collection_name}")
        
        # Get collection info
        collection_count = collection.count()
        print(f" Collection contains {collection_count} items")
        
        return collection
    except Exception as e:
        sys.exit(f" ERROR: Could not connect to collection '{collection_name}' -> {e}")

def setup_query_images(query_images_path):
    # Setup and verify query images directory.
    
    if not os.path.exists(query_images_path):
        sys.exit(f" ERROR: Query images directory '{query_images_path}' does not exist.")
    
    query_images = [f for f in os.listdir(query_images_path) if f.lower().endswith(('.jpg', '.jpeg', '.png', '.webp'))]
    print(f" Found {len(query_images)} query images: {query_images[:3]}...")
    return query_images

def preload_image_paths(client, trusted_bucket):
    # Pre-load image paths for efficient lookup.
    
    all_image_objects = list(client.list_objects(trusted_bucket, prefix="images/", recursive=True))
    image_paths = {obj.object_name.split('/')[-1].replace('.jpg', ''): obj.object_name 
                   for obj in all_image_objects if obj.object_name.endswith('.jpg')}
    print(f" Found {len(image_paths)} image files in bucket")
    return image_paths

def load_image_from_minio(client, trusted_bucket, image_paths, uuid):
    # Load image from MinIO using pre-loaded path dictionary.
    try:
        # Get image path
        image_path = image_paths.get(uuid)
        if not image_path:
            print(f" Image not found for UUID: {uuid}")
            return None
            
        image_data = client.get_object(trusted_bucket, image_path)
        image_bytes = image_data.read()
        image_data.close()
        image_data.release_conn()
        
        image = Image.open(io.BytesIO(image_bytes))
        return image
    except Exception as e:
        print(f" Could not load image {uuid}: {e}")
        return None

def get_random_query_image(query_images, query_images_path):
    # Get a random image from the query_images directory. for performing images tasks.
    if not query_images:
        return None, None
    # selecting a random image
    random_image = secrets.choice(query_images)
    image_path = os.path.join(query_images_path, random_image)
    
    try:
        image = Image.open(image_path)
        print(f" Selected random query image: {random_image}")
        return image, random_image
    except Exception as e:
        print(f" Error loading image {random_image}: {e}")
        return None, None

def prepare_result_data(ids, distances, metadatas, documents, collection, client, trusted_bucket, image_paths):
    # Prepare result data for display.
    result_data = []
    for i, (item_id, distance, metadata, document) in enumerate(zip(ids, distances, metadatas, documents)):
        # as in multimodal embeddings we are storing with a _txt or _img in end.
        uuid = item_id.replace('_img', '').replace('_txt', '')
        
        # Load image (defined the function in lower part)
        image = load_image_from_minio(client, trusted_bucket, image_paths, uuid)
        
        # Load text description
        text_description = get_text_description(collection, uuid, document)
        
        # appending needed data
        result_data.append({
            'index': i + 1,
            'similarity': 1 - distance,
            'image': image,
            'description': text_description,
            'metadata': metadata,
            'common_name': metadata.get('common', 'Unknown'),
            'scientific_name': metadata.get('scientific_name', 'Unknown'),
            'species': metadata.get('species', 'Unknown'),
            'kingdom': metadata.get('kingdom', 'N/A'),
            'class': metadata.get('class', 'N/A'),
            'family': metadata.get('family', 'N/A'),
            'genus': metadata.get('genus', 'N/A'),
            'order': metadata.get('order', 'N/A'),
            'phylum': metadata.get('phylum', 'N/A')
        })
    return result_data

def get_text_description(collection, uuid, document):
    # Get text description for a result.
    
    text_description = ""
    try:
        # get only text using modality as text
        text_results = collection.get(
            where={"$and": [{"uuid": uuid}, {"modality": "text"}]})

        # get documents
        if text_results['documents'] and len(text_results['documents']) > 0:
            text_description = text_results['documents'][0]
        elif document and document != "None":
            text_description = document
    except Exception:
        if document and document != "None":
            text_description = document
    return text_description

def display_result_items(result_data):
    # Display individual result items.
    
    for i, data in enumerate(result_data):
        print(f"\n{'='*80}")
        print(f"RESULT #{data['index']} - {data['species']}")
        print(f"{'='*80}")
        
        # Print details 
        print(f"Species: {data['species']}")
        print(f"Scientific Name: {data['scientific_name']}")
        print(f"Common Name: {data['common_name']}")
        print(f"Kingdom: {data['kingdom']}")
        print(f"Phylum: {data['phylum']}")
        print(f"Class: {data['class']}")
        print(f"Order: {data['order']}")
        print(f"Family: {data['family']}")
        print(f"Genus: {data['genus']}")
        print(f"Similarity: {data['similarity']:.3f}")
        
        # Add description if available
        if data['description']:
            desc = data['description']
            print(f"\nDescription:\n{desc}")
        else:
            print("\nDescription: No description available")
        
        print(f"\n{'-'*60}")
        
        # Then show the image
        if data['image']:
            _, ax = plt.subplots(1, 1, figsize=(8, 6))
            ax.imshow(data['image'])
            ax.set_title(f"Image: {data['species']}", fontsize=14, fontweight='bold')
            ax.axis('off')
            plt.tight_layout()
            plt.show()
        else:
            print(f" Image not available for {data['species']}")
        
        print(f" Displayed result {i+1}/{len(result_data)}: {data['species']}")
        print(f"{'='*80}\n")

def display_results(results, query_type="text", query_value="", n_results=5, is_cluster_search=False, 
                   collection=None, client=None, trusted_bucket=None, image_paths=None):
    # Display search results with images and descriptions.
    
    print(f"\n🔍 {query_type.upper()} QUERY RESULTS")
    print(f"Query: '{query_value}'")
    # if we are displayin results of a cluster search
    if is_cluster_search:
        print(" Cluster-based search (top 3 most frequent species)")
    print("=" * 60)

    # if no results
    if not results['ids'] or len(results['ids'][0]) == 0:
        print(" No results found!")
        return
        
    # get id and other data from each result
    ids = results['ids'][0][:n_results]
    distances = results['distances'][0][:n_results]
    metadatas = results['metadatas'][0][:n_results]
    documents = results['documents'][0][:n_results] if 'documents' in results else [None] * len(ids)
    
    print(f"Found {len(ids)} similar items:")
    print("-" * 60)
    
    # Prepare data for all results first
    result_data = prepare_result_data(ids, distances, metadatas, documents, collection, client, trusted_bucket, image_paths)
    
    # Display results in simple format (description first, then image)
    display_result_items(result_data)

def cluster_based_search(collection, query_text="", query_image=None, n_results=15, return_count=3, clip_embd=None):
    # Perform cluster-based search showing top species representatives.
    print("\n CLUSTER-BASED SEARCH")
    print(f"Analyzing top-{n_results} results for most frequent species...")
    print("=" * 60)
    
    try:
        # Get model if not provided
        if clip_embd is None:
            clip_embd = _get_embedding_model()
        
        # so that when entered 'image' it should not take also '' empty text to do an extra text query
        if query_text and query_text.strip() != "":
            # Generate text embedding using selected model
            if hasattr(clip_embd, 'embed_query'):
                query_embedding = clip_embd.embed_query(query_text)
                if isinstance(query_embedding, list):
                    query_embedding = query_embedding[0] if len(query_embedding) > 0 else query_embedding
                results = collection.query(
                    query_embeddings=[query_embedding],
                    n_results=n_results)
            else:
                # Fallback to collection's default embedder
                results = collection.query(
                    query_texts=[query_text],
                    n_results=n_results)
            
            print(f" Text cluster search: '{query_text}'")
        elif query_image is not None:
            results = perform_image_cluster_search(collection, query_image, n_results, clip_embd)
            if results is None:
                return None
        else:
            # if on query available
            print(" No query provided!")
            return None
        
        if not results['ids'] or len(results['ids'][0]) == 0:
            print(" No results found!")
            return None
        
        print(f"\n Analyzing {len(results['ids'][0])} similar items for species frequency...\n")
        
        # Extract species from enriched metadata
        species_list = []
        for metadata in results['metadatas'][0]:
            # Use the enriched metadata 
            species = metadata.get('species', 'Unknown')
            species_list.append(species)
        
        # Count frequency of species
        from collections import Counter
        species_counts = Counter(species_list)
        print("\n Species frequency analysis:")
        for species, count in species_counts.most_common():
            print(f"  {species}: {count} occurrences")
        
        # Get top 3 most frequent species
        top_species = [species for species, _ in species_counts.most_common(return_count)]
        print(f"\n Top {return_count} most frequent species: {top_species}")
        
        # Filter results to show exactly one representative from each top species
        filtered_results = filter_results_by_top_species(results, top_species, return_count)
        
        print("\n Species cluster analysis complete!")
        print(f" Original results: {len(results['ids'][0])}")
        print(f" Top species representatives: {len(filtered_results['ids'][0])}")
        print(f" Species represented: {set([meta.get('species', 'Unknown') for meta in filtered_results['metadatas'][0]])}")
        
        return filtered_results
        
    except Exception as e:
        print(f" Error during cluster-based search: {e}")
        return None

def perform_image_cluster_search(collection, query_image, n_results, clip_embd=None):
    # Perform image-based cluster search using embeddings.
    try:
        # Use provided model or default to baseline
        if clip_embd is None:
            clip_embd = OpenCLIPEmbeddings(
                model_name="ViT-B-32",
                checkpoint="laion2b_s34b_b79k"
            )
        
        # check and Convert to embeddable format, handling different types.
        query_embedding = generate_image_embedding(clip_embd, query_image)
        
        # Query the collection
        # Use the provided collection (already opened correctly elsewhere)
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=n_results
        )
        print(" Image cluster search (using image embeddings)")
        return results
        
    except Exception as e:
        print(f" Could not perform image search: {e}")
        return None

def generate_image_embedding(clip_embd, query_image):
    # Generate embedding for different image formats.
    if isinstance(query_image, Image.Image):
        # Save PIL image temporarily
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp_file:
            query_image.save(tmp_file.name)
            query_embedding = clip_embd.embed_image([tmp_file.name])[0]
        os.unlink(tmp_file.name)
    elif isinstance(query_image, np.ndarray):
        # Convert NumPy array to PIL first
        img = Image.fromarray(query_image.astype('uint8'), 'RGB')
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp_file:
            img.save(tmp_file.name)
            query_embedding = clip_embd.embed_image([tmp_file.name])[0]
        os.unlink(tmp_file.name)
    elif isinstance(query_image, str) and os.path.exists(query_image):
        # If a file path then embedd directly
        query_embedding = clip_embd.embed_image([query_image])[0]
    else:
        raise ValueError("Unsupported image format for embedding generation.")
    
    return query_embedding

def filter_results_by_top_species(results, top_species, return_count):
    # Filter results to show one representative from each top species.
    filtered_ids = []
    filtered_distances = []
    filtered_metadatas = []
    species_found = set()  # Track which species we've already included
    
    for i, (item_id, distance, metadata) in enumerate(zip(
        results['ids'][0], results['distances'][0], 
        results['metadatas'][0]
    )):
        # Use the enriched metadata field again
        species = metadata.get('species', 'Unknown')
        # Only add if this species is in top 3 and we haven't included it yet
        if species in top_species and species not in species_found:
            filtered_ids.append(item_id)
            filtered_distances.append(distance)
            filtered_metadatas.append(metadata)
            species_found.add(species)
            
            # Stop when we have one representative from each of the top 3 species
            if len(species_found) >= return_count:
                break
    
    # Create filtered results
    filtered_results = {
        'ids': [filtered_ids],
        'distances': [filtered_distances],
        'metadatas': [filtered_metadatas]
    }
    
    return filtered_results
    
# Helper functions for interactive_search
def _is_non_interactive_mode():
    """Check if running in non-interactive mode."""
    import os
    import sys
    
    return (
        os.getenv('CI') == 'true' or
        os.getenv('GITHUB_ACTIONS') == 'true' or
        os.getenv('GITLAB_CI') == 'true' or
        '--non-interactive' in sys.argv or
        not sys.stdin.isatty()
    )

def _execute_search_by_input(collection, user_input, query_images, 
                             query_images_path, client, trusted_bucket, image_paths, clip_embd=None):
    """Execute search based on user input (text or image)."""
    if user_input.lower() == 'image':
        query_image, image_name = get_random_query_image(query_images, query_images_path)
        if query_image:
            search_by_image(collection, query_image, image_name,
                          client=client, trusted_bucket=trusted_bucket, image_paths=image_paths, clip_embd=clip_embd)
        else:
            print(" No query images available!")
    else:
        search_by_text(collection, user_input, n_results=5, client=client,
                      trusted_bucket=trusted_bucket, image_paths=image_paths, clip_embd=clip_embd)

def _handle_non_interactive_search(collection, query_images, 
                                   query_images_path, client, trusted_bucket, image_paths, clip_embd=None):
    """Handle non-interactive mode search using environment variable."""
    import os
    
    print("Running in non-interactive mode - using environment variable for query")
    user_input = os.getenv('USER_QUERY', 'rattlesnake')
    print(f"Using query: {user_input}")
    
    if not user_input:
        print("No query provided in environment variable")
        return
    
    _execute_search_by_input(collection, user_input, query_images,
                            query_images_path, client, trusted_bucket, image_paths, clip_embd=clip_embd)

def _handle_interactive_search_loop(collection, query_images, 
                                    query_images_path, client, trusted_bucket, image_paths, clip_embd=None):
    """Handle interactive search loop."""
    print("\n MULTIMODAL WILDLIFE SEARCH")
    print("=" * 60)
    print("Instructions:")
    print("• Enter any text to search for similar wildlife")
    print("• Enter 'image' to search using a random image")
    print("• Enter 'quit' to exit")
    print("=" * 60)
    
    while True:
        try:
            user_input = input("\n Enter your search query (or 'image'/'quit'): ").strip()
            
            if user_input.lower() == 'quit':
                print(" Goodbye!")
                break
            
            elif user_input.lower() == 'image' or user_input:
                _execute_search_by_input(collection, user_input, query_images,
                                        query_images_path, client, trusted_bucket, image_paths, clip_embd=clip_embd)
            else:
                print(" Please enter a search query, 'image', or 'quit'")
                
        except KeyboardInterrupt:
            print("\n Goodbye!")
            break
        except Exception as e:
            print(f" Error: {e}")

# search functions
def search_by_text(collection, query_text, n_results=5, client=None, trusted_bucket=None, image_paths=None, clip_embd=None):
     # erform text-based similarity search.
    print(f"\n Searching with text: '{query_text}'")
    
    try:
        # Get model if not provided
        if clip_embd is None:
            clip_embd = _get_embedding_model()
        
        # Generate text embedding using selected model
        if hasattr(clip_embd, 'embed_query'):
            query_embedding = clip_embd.embed_query(query_text)
            if isinstance(query_embedding, list):
                query_embedding = query_embedding[0] if len(query_embedding) > 0 else query_embedding
            # query using embeddings
            results = collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results)
        else:
            # Fallback to collection's default embedder
            results = collection.query(
                query_texts=[query_text],
                n_results=n_results)
        
        display_results(results, query_type="text", query_value=query_text, n_results=n_results, 
                       is_cluster_search=False, collection=collection, client=client, 
                       trusted_bucket=trusted_bucket, image_paths=image_paths)
        return results
        
    except Exception as e:
        print(f" Error during text search: {e}")
        return None

def search_by_image(collection, query_image, image_name="", 
                   client=None, trusted_bucket=None, image_paths=None, clip_embd=None):
    # Perform image-based similarity search using cluster-based approach.
    print(f"\n Searching with image: '{image_name}'")
    
    # Display the query image in smaller size and centered
    try:
        if isinstance(query_image, Image.Image):
            # Create a smaller, centered view
            _, ax = plt.subplots(1, 1, figsize=(4, 3))
            ax.imshow(query_image)
            ax.set_title(f"Query Image: {image_name}", fontsize=11, fontweight='bold')
            ax.axis('off')
            # Center the figure
            plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
            plt.show()
            print(f" Query image displayed: {image_name}")
        
        # Use cluster-based search for images 
        results = cluster_based_search(collection, query_image=query_image, n_results=15, return_count=3, clip_embd=clip_embd)
        
        if results:
            display_results(results, query_type="image", query_value=image_name, n_results=3, 
                           is_cluster_search=True, collection=collection, client=client, 
                           trusted_bucket=trusted_bucket, image_paths=image_paths)
        else:
            print(" No cluster results found!")
        
        return results
        
    except Exception as e:
        print(f" Error during image search: {e}")
        return None

def interactive_search(collection, query_images, query_images_path, 
                      client, trusted_bucket, image_paths, clip_embd=None):
    # Interactive search interface for user input
    # Supports both interactive and non-interactive (CI/CD) modes.
    
    if _is_non_interactive_mode():
        _handle_non_interactive_search(collection, query_images,
                                      query_images_path, client, trusted_bucket, image_paths, clip_embd=clip_embd)
        return
    
    _handle_interactive_search_loop(collection, query_images,
                                   query_images_path, client, trusted_bucket, image_paths, clip_embd=clip_embd)

def get_collection_statistics(collection):
    # Get statistics about the multimodal_embedding collection.
    print("\n COLLECTION STATISTICS")
    print("=" * 60)
    
    try:
        # Get all data
        all_data = collection.get()
        
        if all_data['ids']:
            print(f" Total items: {len(all_data['ids'])}")
            
            # Count by kingdom
            kingdoms = {}
            classes = {}
            families = {}
            
            for metadata in all_data['metadatas']:
                kingdom = metadata.get('kingdom', 'Unknown')
                cls = metadata.get('class', 'Unknown')
                family = metadata.get('family', 'Unknown')
                
                kingdoms[kingdom] = kingdoms.get(kingdom, 0) + 1
                classes[cls] = classes.get(cls, 0) + 1
                families[family] = families.get(family, 0) + 1
            
            print("\n By Kingdom:")
            for kingdom, count in sorted(kingdoms.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"  {kingdom}: {count}")
                
            print("\n By Class:")
            for cls, count in sorted(classes.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"  {cls}: {count}")
                
            print("\n Top Families:")
            for family, count in sorted(families.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"  {family}: {count}")
                
        else:
            print(" Collection is empty!")
            
    except Exception as e:
        print(f" Error getting statistics: {e}")

# ==============================
#        Main Function
# ==============================
def process_multimodal_task():
    
    # Get MinIO configuration from environment variables (set by orchestrator)
    minio_endpoint, access_key, secret_key = get_minio_config()
    
    # Configuration
    trusted_bucket = "trusted-zone"
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__)) # in orchestrated
    except NameError:
        script_dir = os.getcwd() # in notebook
        
    chroma_db = os.path.join(script_dir, "../../Exploitation-Zone/exploitation_db")
    query_images_path = os.path.join(script_dir, "../query_images")
    collection_name = "multimodal_embeddings"
    
    print(" Starting Multimodal Similarity Search...")
    print(f" Collection: {collection_name}")
    print(f" Query Images: {query_images_path}")
    print("=" * 60)
    
    # Setup connections
    client = setup_minio_connection(minio_endpoint, access_key, secret_key, trusted_bucket)
    collection = setup_chromadb_connection(chroma_db, collection_name)
    query_images = setup_query_images(query_images_path)
    image_paths = preload_image_paths(client, trusted_bucket)
    
    print("=" * 60)
    print(" Helper functions defined")
    print("=" * 60)
    
    # Initialize embedding model (user selects baseline or fine-tuned)
    clip_embd = _get_embedding_model()
    
    # Start the interactive search
    print(" Starting Multimodal Wildlife Search...")
    interactive_search(collection, query_images, query_images_path, 
                     client, trusted_bucket, image_paths, clip_embd=clip_embd)
    
    # Display collection statistics
    get_collection_statistics(collection)

process_multimodal_task();
