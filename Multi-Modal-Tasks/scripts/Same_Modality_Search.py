"""
-Arman Bazarchi-
— Same-Modality Similarity Search Notebook

 - Demonstrate image-to-image similarity search using existing embeddings in ChromaDB.
 - Query: find visually similar images to a given example.
 - code takes a random image form the folder query_images which must be places along with some image example
   in the same location with the task notebooks
 - with that query image applies a similarity check with available image embeddings, retrieve 15 nearest to the query image.
 - counts the species in that 15 nearest, takes the 3 most frequent species in the 15 nearest ones, and visualizes them 
   along with the query images
 - we take 3 most frequent to have more precise detection.
 - we must use the same model we used for embedding them to check for similarities

  Updated to let user to set which model to use(baseline or fine tuned)

"""


from langchain_experimental.open_clip import OpenCLIPEmbeddings
import chromadb
from PIL import Image
import matplotlib.pyplot as plt
import os, secrets, tempfile
from minio import Minio
import io
import sys
import importlib.util

# -----------------------
#    Configuration
# -----------------------
def process_same_modality(
    minio_endpoint="localhost:9000",
    access_key="admin",
    secret_key="password123"):
    # Main function to orchestrate same-modality similarity search.
    
    # Setup connections and validate environment
    minio_client, _, image_collection = _setup_connections(minio_endpoint, access_key, secret_key)
    
    # Initialize embedding model and select query image
    clip_embd, query_image = _setup_query_processing()
    
    # Generate query embedding
    query_embedding = clip_embd.embed_image([query_image])[0]
    print(f" Generated embedding for query image ({len(query_embedding)} dimensions).")
    
    # Perform cluster-based search
    TOP_K = 15
    cluster_results = cluster_based_image_search(
        image_collection,
        query_embedding,
        n_results=TOP_K, 
        return_count=3
    )
    
    # Visualize results
    _visualize_results(query_image, cluster_results, minio_client)

def _setup_connections(minio_endpoint, access_key, secret_key):
    # Setup MinIO and ChromaDB connections with validation.
    
    # Connect to MinIO
    minio_client = Minio(
        minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    
    TRUSTED_BUCKET = "trusted-zone"
    
    # Check if trusted bucket exists
    if not minio_client.bucket_exists(TRUSTED_BUCKET):
        raise SystemExit(" ERROR: MinIO bucket 'trusted-zone' does not exist.")
    
    print(" Connected to MinIO and verified bucket 'trusted-zone'.")
    
    # Setup ChromaDB connection
    try:
        SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) # in orchestrated
    except NameError:
        SCRIPT_DIR = os.getcwd() # in notebook
        
    CHROMA_DB_DIR = os.path.join(SCRIPT_DIR, "../../Exploitation-Zone/exploitation_db")
    
    # Check if ChromaDB directory exists
    if not os.path.exists(CHROMA_DB_DIR):
        raise SystemExit(f" ERROR: ChromaDB directory '{CHROMA_DB_DIR}' does not exist.")
    
    chroma_client = chromadb.PersistentClient(path=CHROMA_DB_DIR)
    
    collection_name = "image_embeddings"
    
    # Check if collection exists
    try:
        image_collection = chroma_client.get_collection(name=collection_name)
    except Exception:
        raise SystemExit(f" ERROR: Collection '{collection_name}' does not exist in ChromaDB.")
    
    print(f" Connected to ChromaDB at '{CHROMA_DB_DIR}'.")
    print(f" Loaded collection '{collection_name}' (contains {image_collection.count()} embeddings).")
    
    return minio_client, chroma_client, image_collection

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

def _setup_query_processing():
    # Setup embedding model and select query image.
    # Let user choose between baseline and fine-tuned model
    model_type, checkpoint_info = _select_model()
    clip_embd = _load_embedding_model(model_type, checkpoint_info)
    
    # Pick a random query image from the local folder
    try:
        SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) # in orchestrated
    except NameError:
        SCRIPT_DIR = os.getcwd() # in notebook
        
    QUERY_FOLDER = "../query_images" # query images path from root script or notebook folders
    
    all_images = [
        os.path.join(SCRIPT_DIR, QUERY_FOLDER, f)
        for f in os.listdir(os.path.join(SCRIPT_DIR, QUERY_FOLDER))
        if f.lower().endswith((".jpg", ".jpeg", ".png"))]
    
    if not all_images:
        raise SystemExit(" No images found in query_images folder.")
    
    query_image = secrets.choice(all_images)
    print(f" Selected query image: {query_image}")
    
    return clip_embd, query_image

def cluster_based_image_search(image_collection, query_embedding, n_results=15, return_count=3):
    # Perform cluster-based image similarity search focused on species frequency.
    # most frequent spceies in k number of similar ones will be displayed
    print(f"Analyzing top-{n_results} results for most frequent species...")
    print("=" * 60)
    
    # Get top-k results (image collection)
    results = image_collection.query(
        query_embeddings=[query_embedding],
        n_results=n_results,
        include=["metadatas", "distances"]
    )
    
    # Check if results are available
    if not results['ids'] or len(results['ids'][0]) == 0:
        print(" No results found!")
        return None
    
    # Show details of all k nearest results
    _display_search_results(results, n_results, "image_embeddings")
    
    # Extract and analyze species frequency
    species_list = _extract_species_list(results)
    species_counts = _count_species_frequency(species_list)
    
    # Get top species and filter results
    top_species = _get_top_species(species_counts, return_count)
    filtered_results = _filter_results_by_species(results, top_species, return_count)
    
    # Display analysis summary
    _display_analysis_summary(results, filtered_results)
    
    return filtered_results

def _display_search_results(results, n_results, collection_name):
    # Display detailed results of the search.
    print(f" Top {n_results} similar images found in collection '{collection_name}':\n")
    for i in range(len(results["ids"][0])):
        uid = results["ids"][0][i]
        meta = results["metadatas"][0][i]
        dist = results["distances"][0][i]
        
        print(f"{i+1}. UUID: {uid}")
        print(f"   Distance: {dist:.4f}")
        print("   Taxonomy:")
        print(f"     Species: {meta.get('species', 'N/A')}")
        print(f"     Family: {meta.get('family', 'N/A')}")
        print(f"     Class: {meta.get('class', 'N/A')}")
        print(f"     Kingdom: {meta.get('kingdom', 'N/A')}")
        print(f"     Scientific Name: {meta.get('scientific_name', 'N/A')}")
        print(f"     Common Name: {meta.get('common', 'N/A')}")
        print(f"     Path: {meta.get('path', 'N/A')}")
        print()

def _extract_species_list(results):
    # Extract species list from metadata.
    species_list = []
    for metadata in results['metadatas'][0]:
        species = metadata.get('species', 'Unknown')
        species_list.append(species)
    return species_list

def _count_species_frequency(species_list):
    # Count frequency of species and display analysis.
    from collections import Counter
    species_counts = Counter(species_list)
    print("\n Species frequency analysis:")
    for species, count in species_counts.most_common():
        print(f"  {species}: {count} occurrences")
    return species_counts

def _get_top_species(species_counts, return_count):
    # Get top most frequent species.
    top_species = [species for species, _ in species_counts.most_common(return_count)]
    print(f"\n Top {return_count} most frequent species: {top_species}")
    return top_species

def _filter_results_by_species(results, top_species, return_count):
    # Filter results to show one representative from each top species.
    filtered_ids = []
    filtered_distances = []
    filtered_metadatas = []
    species_found = set()
    
    for item_id, distance, metadata in zip(
        results['ids'][0], results['distances'][0], results['metadatas'][0]
    ):
        species = metadata.get('species', 'Unknown')
        if species in top_species and species not in species_found:
            filtered_ids.append(item_id)
            filtered_distances.append(distance)
            filtered_metadatas.append(metadata)
            species_found.add(species)
            
            if len(species_found) >= return_count:
                break
    
    return {
        'ids': [filtered_ids],
        'distances': [filtered_distances],
        'metadatas': [filtered_metadatas]
    }

def _display_analysis_summary(results, filtered_results):
    # Display summary of the analysis.
    print("\n Species cluster analysis complete!")
    print(f" Original results: {len(results['ids'][0])}")
    print(f" Top species representatives: {len(filtered_results['ids'][0])}")
    print(f" Species represented: {set([meta.get('species', 'Unknown') for meta in filtered_results['metadatas'][0]])}")

def show_top3_species_results(query_path, results, minio_client):
    # Display query image and top 3 species results side by side.
    top_dists = results["distances"][0]
    top_metadatas = results["metadatas"][0]

    n = len(top_dists)
    plt.figure(figsize=(20, 5))

    # Show query image first
    plt.subplot(1, n + 1, 1)
    img = Image.open(query_path).convert("RGB")
    plt.imshow(img)
    plt.axis("off")
    plt.title("Query Image", fontsize=12, weight="bold")

    # Show top 3 species images (download from MinIO)
    for i, (d, meta) in enumerate(zip(top_dists, top_metadatas), start=2):
        try:
            # Use the enriched metadata path directly
            image_path = meta.get('path', '')
            if not image_path:
                print(f" No path found in metadata for result {i-1}")
                continue
            
            response = minio_client.get_object("trusted-zone", image_path)
            img_bytes = response.read()
            response.close()
            response.release_conn()
            img = Image.open(io.BytesIO(img_bytes)).convert("RGB")

            plt.subplot(1, n + 1, i)
            plt.imshow(img)
            plt.axis("off")
            
            # Create detailed title with enriched metadata for the image
            species_name = meta.get('species', 'Unknown')
            family_name = meta.get('family', 'Unknown')
            class_name = meta.get('class', 'Unknown')
            kingdom_name = meta.get('kingdom', 'Unknown')
            common_name = meta.get('common', 'Unknown')
            scientific_name = meta.get('scientific_name', 'Unknown')
            
            title = f"Species: {species_name}\nFamily: {family_name}\nClass: {class_name}\nKingdom: {kingdom_name}\nCommon: {common_name}\nScientific: {scientific_name}\nDist: {d:.3f}"
            plt.title(title, fontsize=9)

        except Exception as e:
            print(f" Could not fetch image for path {image_path}: {e}")
            # Show placeholder 
            plt.subplot(1, n + 1, i)
            plt.text(0.5, 0.5, f"Image not\navailable\nPath: {image_path}", 
                    ha='center', va='center', fontsize=8)
            plt.axis("off")

    plt.tight_layout()
    plt.show()

def _visualize_results(query_image, cluster_results, minio_client):
    # Visualize the top 3 species results.
    if cluster_results:
        print("\n VISUALIZING TOP 3 SPECIES RESULTS")
        print("=" * 60)
        show_top3_species_results(query_image, cluster_results, minio_client)
        print(" Top 3 species visualization complete.")
    else:
        print(" No cluster results to visualize.")
    
process_same_modality();