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

"""


from langchain_experimental.open_clip import OpenCLIPEmbeddings
import chromadb
from PIL import Image
import matplotlib.pyplot as plt
import os, random, tempfile
from minio import Minio
import io

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

def _setup_query_processing():
    # Setup embedding model and select query image.
    # Initialize the same embedding model (must be the same embedding model)
    clip_embd = OpenCLIPEmbeddings(
        model_name="ViT-B-32", # same model
        checkpoint="laion2b_s34b_b79k")
    
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
    
    query_image = random.choice(all_images)
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