import os
import glob
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.http import models
import logging
import sys

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Qdrant Connection
try:
    qdrant_client = QdrantClient("localhost", port=6333, timeout=30)
    logger.info("✅ Connected to Qdrant successfully")
except Exception as e:
    logger.error(f"❌ Failed to connect to Qdrant: {str(e)}")
    sys.exit(1)

# Define Collection Name
collection_name = "txt_file_data"

# Initialize Sentence Transformer Model
embedder = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# Detect the embedding dimension automatically
try:
    sample_text = "Test sentence for embedding dimension check."
    sample_embedding = embedder.encode([sample_text], convert_to_tensor=False)[0]
    vector_dimension = len(sample_embedding)  # Should be 384 for this model
    logger.info(f"✅ Detected embedding dimension: {vector_dimension}")
except Exception as e:
    logger.error(f"❌ Failed to detect embedding dimension: {str(e)}")
    sys.exit(1)

# Ensure Collection Matches Correct Vector Size
if qdrant_client.collection_exists(collection_name):
    collection_info = qdrant_client.get_collection(collection_name)
    if collection_info.config.params.vectors.size == vector_dimension:
        logger.info(f"✅ Collection {collection_name} exists with correct vector dimension {vector_dimension}")
    else:
        qdrant_client.delete_collection(collection_name)
        logger.info(f"🗑 Deleted previous collection {collection_name} due to dimension mismatch")
        qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(size=vector_dimension, distance=models.Distance.COSINE),
        )
        logger.info(f"✅ Created collection {collection_name} with vector dimension {vector_dimension}")
else:
    qdrant_client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(size=vector_dimension, distance=models.Distance.COSINE),
    )
    logger.info(f"✅ Created collection {collection_name} with vector dimension {vector_dimension}")

# Read the TXT File
txt_file_path = "kongunadu_data.txt"

try:
    with open(txt_file_path, "r", encoding="utf-8") as file:
        raw_content = file.readlines()
    logger.info(f"📄 Successfully loaded text file: {txt_file_path}")
except Exception as e:
    logger.error(f"❌ Failed to read text file: {str(e)}")
    sys.exit(1)

# Function to Split and Process Content
def split_content(lines, max_chunk_length=150):
    """ Splits text dynamically without hardcoded keywords, using sentence boundaries. """
    chunks = []
    current_chunk = ""

    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # If adding the line exceeds max length, save current chunk and start a new one
        if len(current_chunk) + len(line) > max_chunk_length and current_chunk:
            chunks.append(current_chunk.strip())
            current_chunk = line
        else:
            current_chunk += " " + line

    if current_chunk.strip():
        chunks.append(current_chunk.strip())

    logger.info(f"📌 Split text into {len(chunks)} dynamic chunks")
    return chunks

# Process the Content
content_chunks = split_content(raw_content)

# Prepare Data Points for Qdrant
points = []
point_id = 0

try:
    embeddings = embedder.encode(content_chunks, convert_to_tensor=False)
    
    for embedding, chunk in zip(embeddings, content_chunks):
        points.append(
            models.PointStruct(
                id=point_id,
                vector=embedding.tolist(),
                payload={
                    "source": txt_file_path,
                    "content": chunk
                }
            )
        )
        point_id += 1

    # Store Points in Qdrant
    qdrant_client.upsert(collection_name=collection_name, points=points)
    logger.info(f"✅ Successfully stored {len(points)} entries in Qdrant.")
    print("🎯 TXT file content uploaded and stored in Qdrant!")

except Exception as e:
    logger.error(f"❌ Failed to store data in Qdrant: {str(e)}")
    sys.exit(1)