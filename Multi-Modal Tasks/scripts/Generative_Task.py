"""
-Arman Bazarchi-
Generative Tasks with External Models
Multimodal generation including text, image, and RAG capabilities

Connects to a ChromaDB multimodal database of species embeddings.
Retrieves relevant wildlife data based on text queries, images, or both (multimodal).
Displays top species matches, their taxonomy, descriptions, and associated images from a trusted MinIO bucket.
Uses Qwen3-VL (with fallback to llama) for text generation and reasoning.
Supports querying: text-only, image-only, or image+text.

User can enter a text query to seearch for similar text embeddings and retrieeve similar results in text and image,
and a response for their query from Qwen,
User can enter 'take image' for including a random query image to their query, then similar images to the query will 
be displayed and the image with their rest of query text with a founded similar image will be sent to Qwen for a response.

"""

import chromadb
from chromadb.utils import embedding_functions
import pandas as pd
import io
import os
import random
import re
from minio import Minio
import sys
from PIL import Image
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import requests
import json
import base64
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables from .env file
# here we have store the api key for calling the used model
load_dotenv()


NO_SIMILAR_SPECIES_FOUND = " No similar species found"

# ==============================
#         Hugging Face Token Configuration
# ==============================
def get_huggingface_token():
    # Get Hugging Face token from user input or environment
    print("\n" + "="*60)
    print(" Hugging Face Token Configuration")
    print("="*60)
    
    # Check if running in non-interactive mode (CI/CD)
    import sys
    is_non_interactive = (
        os.getenv('CI') == 'true' or  # GitHub Actions
        os.getenv('GITHUB_ACTIONS') == 'true' or  # GitHub Actions
        os.getenv('GITLAB_CI') == 'true' or  # GitLab CI
        '--non-interactive' in sys.argv or  # Command line flag
        not sys.stdin.isatty()  # No TTY (piped input)
    )
    
    if is_non_interactive:
        # In non-interactive mode, only use environment variables
        if os.getenv('HUGGINGFACE_API_TOKEN'):
            print("Using Hugging Face token from environment variable")
            return os.getenv('HUGGINGFACE_API_TOKEN')
        else:
            print("No Hugging Face token found in environment - skipping")
            return None
    
    # Interactive mode - check environment first, then prompt user
    if os.getenv('HUGGINGFACE_API_TOKEN'):
        print(" Hugging Face token found in environment variables")
        return os.getenv('HUGGINGFACE_API_TOKEN')
    
    # Get token from user input with limited attempts
    max_attempts = 3
    for _ in range(max_attempts):
        token = input("Enter Hugging Face API token (or press Enter to skip): ").strip()
        if token:
            # Save token to .env file
            env_file_path = ".env"
            with open(env_file_path, "a") as f:
                f.write(f"\nHUGGINGFACE_API_TOKEN={token}\n")
            
            # Save token to environment variable
            os.environ['HUGGINGFACE_API_TOKEN'] = token
            print(f" Hugging Face token saved to {env_file_path}")
            return token
        else:
            print(" No token provided.")
            return None
    
    print(" Maximum attempts reached.")
    return None

def generate_text_with_llm(prompt, image=None, model="meta-llama/Llama-4-Scout-17B-16E-Instruct:groq", max_length=500):
    # Generate text using a multimodal Llama model via Hugging Face router.
    hf_token = os.getenv("HUGGINGFACE_API_TOKEN")
    if not hf_token:
        return " ERROR: No Hugging Face API token found!"

    # Build base URL and headers
    api_url = "https://router.huggingface.co/v1/chat/completions"
    headers = {"Authorization": f"Bearer {hf_token}"}

    # Build multimodal content
    if image is not None:
        # Convert PIL image to base64-encoded JPEG
        img_buffer = io.BytesIO()
        image.save(img_buffer, format="JPEG")
        img_base64 = base64.b64encode(img_buffer.getvalue()).decode("utf-8")
        img_data_url = f"data:image/jpeg;base64,{img_base64}"

        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": img_data_url}},
                ],
            }
        ]
        print(" Processing multimodal query with Llama-4-Scout (text + image)...")
    else:
        messages = [{"role": "user", "content": [{"type": "text", "text": prompt}]}]

    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_length,
        "temperature": 0.7,
    }

    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=60)

        if response.status_code == 200:
            result = response.json()
            message = result["choices"][0]["message"]["content"]
            return message
        else:
            return f" API Error {response.status_code}: {response.text[:300]}"
    except Exception as e:
        return f" Error generating text: {e}"

def generate_text_with_qwen3(prompt, image=None, max_length=500):
    # Generate text using Qwen3-VL model with OpenAI-compatible API.
    # we use this model because it is light, fast, and appears to respond well in our interests.
    hf_token = os.getenv("HUGGINGFACE_API_TOKEN")
    if not hf_token:
        return " ERROR: No Hugging Face API token found!"

    try:
        # Use OpenAI-compatible API for Qwen3-VL
        client = OpenAI(
            base_url="https://router.huggingface.co/v1",
            api_key=hf_token,
        )

        # Prepare messages for multimodal or image queries
        messages = []

        # for multimodal or image only
        if image is not None:
            # Convert PIL image to base64 for Qwen3-VL
            if hasattr(image, 'convert') and image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Save image to bytes
            img_buffer = io.BytesIO()
            image.save(img_buffer, format='JPEG')
            img_bytes = img_buffer.getvalue()
            
            # Convert to base64 as needed for Qwen3-VL
            img_base64 = base64.b64encode(img_bytes).decode('utf-8')
            img_data_url = f"data:image/jpeg;base64,{img_base64}"
            
            # Create multimodal message with both image and text
            messages.append({
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": img_data_url
                        }
                    },
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            })
            
            print(" Processing multimodal query with Qwen3-VL (image + text)...")
        else:
            # Text-only message
            messages.append({
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            })
            
            print(" Processing text query with Qwen3-VL...")

        completion = client.chat.completions.create(
            model="Qwen/Qwen3-VL-8B-Instruct:novita",
            messages=messages, # the queried messages
            max_tokens=max_length, # a maximum length
            temperature=0.7 # creativity of model
        )
        return completion.choices[0].message.content

        # use the fallback model Llama
    except Exception as e:
        print(f" Qwen3-VL error: {e}, falling back to Llama...")
        return generate_text_with_llm(prompt, model="meta-llama/Llama-4-Scout-17B-16E-Instruct:groq", max_length=max_length)

def retrieve_relevant_data(collection, query_text="", query_image=None, n_results=15):
    # Retrieve similar data from the multimodal collection based on user query text and image.
    print(" Retrieving relevant data for RAG...")
    
    try:
        if query_text and query_image:
            # Multimodal query
            results = collection.query(
                query_texts=[query_text],
                query_images=[query_image],
                n_results=n_results
            )
        elif query_text:
            # Text-only query
            results = collection.query(
                query_texts=[query_text],
                n_results=n_results
            )
        elif query_image:
            # Image-only query
            results = collection.query(
                query_images=[query_image],
                n_results=n_results
            )
        else:
            print(" No query provided for retrieval")
            return None
            
        print(f" Retrieved {len(results['ids'][0])} relevant items")
        return results
        
    except Exception as e:
        print(f" Error during retrieval: {e}")
        return None

def get_most_frequent_species(retrieved_data, top_n=3):
    # Get the most frequent species from retrieved data for clustering.
    if not retrieved_data or not retrieved_data['metadatas']:
        return []
    # Counting top 3 species
    species_count = {}
    metadatas = retrieved_data['metadatas'][0]

    # Count using species to have 
    for metadata in metadatas:
        scientific_name = metadata.get('scientific_name', 'Unknown')
        if scientific_name != 'Unknown':
            species_count[scientific_name] = species_count.get(scientific_name, 0) + 1
    
    # Sort by frequency and get top 3
    sorted_species = sorted(species_count.items(), key=lambda x: x[1], reverse=True)
    return sorted_species[:top_n]

def display_species_header(search_type):
    # Display the header for species search results.
    print(f"\n MOST FREQUENT SPECIES FOUND ({search_type.upper()} SEARCH):")
    print("=" * 60)

def display_frequent_species_list(frequent_species):
    # isplay the list of most frequent species.
    print("\n MOST FREQUENT SPECIES:")
    for i, (species, count) in enumerate(frequent_species, 1):
        print(f"{i}. {species} (appears {count} times)")

def display_taxonomy_details(metadata):
    # Display taxonomy details for a species.
    taxonomy = []
    if metadata.get('kingdom') and str(metadata.get('kingdom')) != 'nan':
        taxonomy.append(f"Kingdom: {metadata.get('kingdom')}")
    if metadata.get('phylum') and str(metadata.get('phylum')) != 'nan':
        taxonomy.append(f"Phylum: {metadata.get('phylum')}")
    if metadata.get('class') and str(metadata.get('class')) != 'nan':
        taxonomy.append(f"Class: {metadata.get('class')}")
    if metadata.get('order') and str(metadata.get('order')) != 'nan':
        taxonomy.append(f"Order: {metadata.get('order')}")
    if metadata.get('family') and str(metadata.get('family')) != 'nan':
        taxonomy.append(f"Family: {metadata.get('family')}")
    if metadata.get('genus') and str(metadata.get('genus')) != 'nan':
        taxonomy.append(f"Genus: {metadata.get('genus')}")
    if metadata.get('species') and str(metadata.get('species')) != 'nan':
        taxonomy.append(f"Species: {metadata.get('species')}")
    
    if taxonomy:
        print("\n".join(taxonomy))

def display_single_species_info(client, trusted_bucket, item_id, metadata, document, distance, scientific_name):
    # Display information for a single species.
    similarity_score = 1 - distance

    if not document or str(document).lower() == 'none':
        document = metadata.get('combined_text', 'No description available.')
    
    print(f"\n--- {scientific_name} (Best Match - Similarity: {similarity_score:.3f}) ---")
    
    # Clean metadata display
    print(f"Scientific Name: {scientific_name}")
    print(f"Common Name: {metadata.get('common', 'Unknown')}")
    
    # Show taxonomy details
    display_taxonomy_details(metadata)
    
    # Show full stored description
    print(f"\nDescription: {document}")
    
    # Display image from trusted zone 
    try:
        success = display_image_from_trusted_zone(
            client, trusted_bucket,
            image_id=item_id,
            species_name=scientific_name,
            image_path_in_metadata=metadata.get('path', None)
        )
        if not success:
            print(" No image found using any method")
    except Exception as e:
        print(f"\nImage: Error loading image - {e}")
        
    print("-" * 50)

def display_similar_species_info(client, trusted_bucket, retrieved_data, search_type="text"):
    # Display only top 3 most frequent species to user.
    # if nothing retrieved
    if not retrieved_data or not retrieved_data['ids']:
        print(NO_SIMILAR_SPECIES_FOUND)
        return
    
    display_species_header(search_type)
    
    # Get 3 most frequent species
    frequent_species = get_most_frequent_species(retrieved_data, 3)
    
    if not frequent_species:
        print(NO_SIMILAR_SPECIES_FOUND)
        return
    
    ids = retrieved_data['ids'][0]
    metadatas = retrieved_data['metadatas'][0]
    documents = retrieved_data['documents'][0]
    distances = retrieved_data['distances'][0]
    
    display_frequent_species_list(frequent_species)
    
    print("\n📋 DETAILED RESULTS:")
    print("-" * 50)
    
    # show the top 3 most frequent species and their best matches
    species_to_show = [species for species, count in frequent_species]
    # to check which ones already selected
    shown_species = set()
    
    for i, (item_id, metadata, document, distance) in enumerate(zip(ids, metadatas, documents, distances)):
        scientific_name = metadata.get('scientific_name', 'Unknown')
        
        # Only show if this species is in top 3 and we haven't shown it yet
        if scientific_name in species_to_show and scientific_name not in shown_species:
            shown_species.add(scientific_name)
            display_single_species_info(client, trusted_bucket, item_id, metadata, document, distance, scientific_name)
            
            # Stop after showing 3 species
            if len(shown_species) >= 3:
                break
    
    print("=" * 60)

def display_image_from_trusted_zone(client, trusted_bucket, image_id, species_name, image_path_in_metadata=None):
    # Display image from trusted zone - try path first, then fallback to ID search.
    try:
        # Try using image_path_in_metadata first
        if image_path_in_metadata:
            try:
                temp_image_path = f"temp_{species_name.replace(' ', '_')}.jpg"
                client.fget_object(trusted_bucket, image_path_in_metadata, temp_image_path)

                img = Image.open(temp_image_path)
                _, ax = plt.subplots(figsize=(6, 4))
                ax.imshow(img)
                ax.set_title(species_name, fontsize=12, fontweight='bold')
                ax.axis('off')
                plt.tight_layout()
                plt.show()

                os.remove(temp_image_path)
                return True
            except Exception as e:
                print(f" Failed to load from metadata path → {e}")
                # continue to fallback

        # Fallback: Search for image using image_id
        base_name = image_id.split('_')[0] if '_' in image_id else image_id
        target_filename = f"{base_name}.jpg"

        # Search in trusted zone bucket
        objects = list(client.list_objects(trusted_bucket, prefix="", recursive=True))
        matched_object = next((obj for obj in objects if obj.object_name.endswith(target_filename)), None)

        if not matched_object:
            print(f" Image not found in trusted zone for ID: {image_id}")
            return False

        # Download and display image
        temp_image_path = f"temp_{species_name.replace(' ', '_')}.jpg"
        client.fget_object(trusted_bucket, matched_object.object_name, temp_image_path)

        img = Image.open(temp_image_path)
        _, ax = plt.subplots(figsize=(6, 4))
        ax.imshow(img)
        ax.set_title(species_name, fontsize=12, fontweight='bold')
        ax.axis('off')
        plt.tight_layout()
        plt.show()

        os.remove(temp_image_path)
        return True

    except Exception as e:
        print(f" Error displaying image for {species_name}: {e}")
        return False

def find_similar_image(collection, query_image, n_results=15):
    # Find similar species from database using image embeddings.
    print("🔍 Finding similar species ...")
    
    try:
        # Convert PIL image to numpy array
        if hasattr(query_image, 'convert'):
            # Convert to RGB if needed
            if query_image.mode != 'RGB':
                query_image = query_image.convert('RGB')
            # Convert to numpy array
            image_array = np.array(query_image)
        else:
            image_array = query_image
        
        results = collection.query(
            query_images=[image_array],
            n_results=n_results
        )
        
        if results and results['ids']:
            print(f" Found {len(results['ids'][0])} similar species")
            return results
        else:
            print(NO_SIMILAR_SPECIES_FOUND)
            return None
    except Exception as e:
        print(f" Error finding similar species: {e}")
        return None

def create_prompt(user_query, retrieved_data, query_image=None, top_n=3):
    # Create a prompt with users query text also image if available, to send to model.
    print(" Creating unified RAG prompt...")

    # if no similar data was retrieved
    if not retrieved_data or not retrieved_data['ids']:
        return f"User Query: {user_query}\n\nNote: No relevant data found in the database."
    
    ids = retrieved_data['ids'][0]
    metadatas = retrieved_data['metadatas'][0]
    documents = retrieved_data['documents'][0]
    distances = retrieved_data['distances'][0]

    # Get top-N frequent species
    frequent_species = get_most_frequent_species(retrieved_data, top_n)
    species_to_include = [species for species, count in frequent_species]
    included_species = set()
    
    context_parts = []
    for i, (item_id, metadata, document, distance) in enumerate(zip(ids, metadatas, documents, distances)):
        scientific_name = metadata.get('scientific_name', 'Unknown')
        if scientific_name in species_to_include and scientific_name not in included_species:
            included_species.add(scientific_name)
            similarity_score = 1 - distance
            context_item = f"\n--- {scientific_name} (Similarity: {similarity_score:.3f}) ---\n"
            context_item += f"Species: {scientific_name}\n"
            context_item += f"Common Name: {metadata.get('common', 'Unknown')}\n"
            context_item += f"Kingdom: {metadata.get('kingdom', 'Unknown')}\n"
            context_item += f"Phylum: {metadata.get('phylum', 'Unknown')}\n"
            context_item += f"Class: {metadata.get('class', 'Unknown')}\n"
            context_item += f"Order: {metadata.get('order', 'Unknown')}\n"
            context_item += f"Family: {metadata.get('family', 'Unknown')}\n"
            context_item += f"Genus: {metadata.get('genus', 'Unknown')}\n"
            context_item += f"Species: {metadata.get('species', 'Unknown')}\n"
            context_parts.append(context_item)

    #  Show top-3 to user, send only top-1 to model, we cant send a lot of images and data to model.
    model_context = ''.join(context_parts[:1])          

    # Include user image as base64 if available
    image_context = ""
    if query_image is not None:
        try:
            # convert the query image to RGB 
            if query_image.mode != 'RGB':
                query_image = query_image.convert('RGB')
            img_buffer = io.BytesIO()
            query_image.save(img_buffer, format='JPEG')
            img_bytes = img_buffer.getvalue()
            # must include as base64 to model
            img_base64 = base64.b64encode(img_bytes).decode('utf-8')
            image_context = f"\nUser provided image (base64 JPEG): data:image/jpeg;base64,{img_base64}\n"
        except Exception as e:
            print(f" Error converting user image to base64: {e}")

    # RAG prompt including user image and text (only top-1 to model)
    rag_prompt = f"""You are a wildlife expert with access to a comprehensive database of species information. 
    
    User Query: {user_query}
    
    {image_context}
    Relevant Database Information:
    {model_context}
    
    Based on the retrieved information from the wildlife database, please provide a comprehensive and accurate response to the user's query. 
    Use the specific data provided above to give detailed, factual information about the species, their characteristics, habitat, 
    behavior, and any other relevant details.
    """

    print(f" Unified RAG prompt created with {len(context_parts)} total species")
    return rag_prompt

def parse_user_query(query):
    # Parse user query to determine workflow type.
    query_lower = query.lower()
    
    has_take_image = 'take image' in query_lower
    
    # Extract the actual query text by removing keywords
    clean_query = query
    if has_take_image:
        clean_query = clean_query.replace('take image', '').strip()
    
    # Remove extra spaces
    clean_query = ' '.join(clean_query.split())
    
    return {
        'original_query': query,
        'clean_query': clean_query,
        'has_take_image': has_take_image,
    }

def get_random_query_image(query_images_list, query_images):
    # Get a random image from query_images folder.
    if not query_images_list:
        return None, None
    
    random_image = random.choice(query_images_list)
    image_path = os.path.join(query_images, random_image)
    
    try:
        image = Image.open(image_path)
        return image, random_image
    except Exception as e:
        print(f" Error loading image {random_image}: {e}")
        return None, None

def display_query_image(image, image_name):
    # Display the query image in controlled size.
    try:
        _, ax = plt.subplots(1, 1, figsize=(8, 6))
        ax.imshow(image)
        ax.set_title(f"Query Image: {image_name}", fontsize=14, fontweight='bold')
        ax.axis('off')
        
        plt.tight_layout()
        plt.show()
        
    except Exception as e:
        print(f"Could not display query image: {e}")

def handle_image_query(client, trusted_bucket, collection, query_images_list, query_images, parsed_query):
    # Handle multimodal query with image.
    
    print(" Selecting random image from query_images...")
    query_image, image_name = get_random_query_image(query_images_list, query_images)
    
    if not query_image:
        print(" No query images available")
        return False
    
    print(f" Using image: {image_name}")
    
    print("\n QUERY IMAGE:")
    print("=" * 40)
    display_query_image(query_image, image_name)
    print("=" * 40)
    
    retrieved_data = find_similar_image(collection, query_image)
    
    if not retrieved_data:
        print(NO_SIMILAR_SPECIES_FOUND)
        return False
    
    display_similar_species_info(client, trusted_bucket, retrieved_data, "image")
    user_prompt_text = parsed_query['clean_query'].strip()
    if not user_prompt_text:
        user_prompt_text = "Tell me about this image."
    
    prompt = create_prompt(user_prompt_text, retrieved_data, query_image)
    print("\n Generating multimodal response with Qwen3-VL...")
    print(" Sending to Qwen3-VL...")
    response = generate_text_with_qwen3(prompt, query_image)
    return response

def handle_text_query(client, trusted_bucket, collection, parsed_query):
    # Handle text-only query.
    
    print(f" Searching for similar species to: '{parsed_query['clean_query']}'")
    retrieved_data = retrieve_relevant_data(collection, parsed_query['clean_query'])
    
    if not retrieved_data:
        print(" No relevant data found")
        return False
    
    display_similar_species_info(client, trusted_bucket, retrieved_data, "text")
    user_prompt_text = parsed_query['clean_query'].strip()
    if not user_prompt_text:
        user_prompt_text = "Identify this species based on the image."
    
    prompt = create_prompt(user_prompt_text, retrieved_data)
    print("\n Generating text response with Qwen3-VL...")
    print(" Sending to Qwen3-VL...")
    response = generate_text_with_qwen3(prompt)
    return response

def interactive_generative_interface(client, trusted_bucket, collection, query_images_list, query_images):
    # Interactive interface for all generative tasks.
    # Supports both interactive and non-interactive (CI/CD) modes.
    
    import os
    import sys
    
    # Check if running in non-interactive mode (CI/CD)
    is_non_interactive = (
        os.getenv('CI') == 'true' or  # GitHub Actions
        os.getenv('GITHUB_ACTIONS') == 'true' or  # GitHub Actions
        os.getenv('GITLAB_CI') == 'true' or  # GitLab CI
        '--non-interactive' in sys.argv or  # Command line flag
        not sys.stdin.isatty()  # No TTY (piped input)
    )
    
    if is_non_interactive:
        # Non-interactive mode - use environment variable for query
        print("Running in non-interactive mode - using environment variable for query")
        user_query = os.getenv('USER_QUERY', 'What is a rattlesnake?')
        print(f"Using query: {user_query}")
        
        if not user_query:
            print("No query provided in environment variable")
            return
        
        # Parse the query
        parsed_query = parse_user_query(user_query)
        print(f"\nParsed query: {parsed_query['clean_query']}")
        
        # Execute based on workflow type
        if parsed_query['has_take_image']:
            response = handle_image_query(client, trusted_bucket, collection, query_images_list, query_images, parsed_query)
        else:
            response = handle_text_query(client, trusted_bucket, collection, parsed_query)
        
        if response:
            print("\nRESPONSE:")
            print("=" * 60)
            print(response)
            print("=" * 60)
        
        return
    
    # Interactive mode
    print("Welcome to the Multimodal Generative Wildlife System!")
    print("\n Query Examples:")
    print("• 'What is a rattlesnake?' - Text generation with database context")
    print("• 'take image what species is this?' - Multimodal analysis with image + text + database")
    print("\nType 'quit' to exit")
    print("=" * 60)
    
    while True:
        try:
            # Get user query
            user_query = input("\n Enter your query: ").strip()
            
            if user_query.lower() == 'quit':
                print(" Goodbye!")
                break
            
            if not user_query:
                print(" Please enter a query")
                continue
            
            # Parse the query
            parsed_query = parse_user_query(user_query)
            print(f"\n Parsed query: {parsed_query['clean_query']}")
            
            # Execute based on workflow type
            if parsed_query['has_take_image']:
                response = handle_image_query(client, trusted_bucket, collection, query_images_list, query_images, parsed_query)
            else:
                response = handle_text_query(client, trusted_bucket, collection, parsed_query)
            
            if response:
                print("\n RESPONSE:")
                print("=" * 60)
                print(response)
                print("=" * 60)
               
        except KeyboardInterrupt:
            print("\n Goodbye!")
            break
        except Exception as e:
            print(f" Error: {e}")

# ==============================
#          Functions
# ==============================

def setup_minio_connection(minio, access_key, secret_key, trusted_bucket):
    # Setup MinIO connection and verify bucket exists.
    
    print(" Connecting to MinIO...")
    client = Minio(
        minio,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    
    # Verify trusted-zone bucket exists
    if not client.bucket_exists(trusted_bucket):
        sys.exit(f" ERROR: Bucket '{trusted_bucket}' does not exist in MinIO.")
    
    print(f" Connected to MinIO - Bucket '{trusted_bucket}' verified")
    return client

def setup_chromadb_and_images(chroma_db, collection_name, query_images):
    # Setup ChromaDB connection and load query images.
    
  
    print(" Connecting to ChromaDB...")
    chroma_client = chromadb.PersistentClient(path=chroma_db)
    
    # Get the multimodal collection
    try:
        collection = chroma_client.get_collection(name=collection_name)
        print(f" Connected to collection: {collection_name}")
        
        # Get collection info
        collection_count = collection.count()
        print(f" Collection contains {collection_count} items")
        
    except Exception as e:
        sys.exit(f" ERROR: Could not connect to collection '{collection_name}' → {e}")
    
    # Check query images directory
    if not os.path.exists(query_images):
        sys.exit(f" ERROR: Query images directory '{query_images}' does not exist.")
    # load query images
    query_images_list = [f for f in os.listdir(query_images) if f.lower().endswith(('.jpg', '.jpeg', '.png', '.webp'))]
    print(f" Found {len(query_images_list)} query images: {query_images_list[:3]}...")
    
    return collection, query_images_list

# ==============================
#        Configuration
# ==============================
def process_generative_task(
    minio = "localhost:9000",
    access_key = "admin",
    secret_key = "password123"):

    trusted_bucket = "trusted-zone"
    # set the working directory
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__)) # in orchestrated
    except NameError:
        script_dir = os.getcwd() # in notebook
        
    chroma_db = os.path.join(script_dir, "../../Exploitation-Zone/exploitation_db")

    query_images = os.path.join(script_dir, "../query_images")

    collection_name = "multimodal_embeddings"
    
    print(" Starting Generative Tasks...")
    print("=" * 60)
    
    # Get HuggingFace token
    get_huggingface_token()
    
    # Setup connections
    client = setup_minio_connection(minio, access_key, secret_key, trusted_bucket)
    collection, query_images_list = setup_chromadb_and_images(chroma_db, collection_name, query_images)
    
    print("=" * 60)
    print(" Required dependencies: openai")
    print(" Using Qwen3-VL for multimodal processing (text + image)")
    print(" Fallback system: Qwen3-VL -> Llama-2")
    print("=" * 60)

    
    # Start the interactive interface
    interactive_generative_interface(client, trusted_bucket, collection, query_images_list, query_images)
    
process_generative_task();
    
