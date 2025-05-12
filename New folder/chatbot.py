import qdrant_client
from qdrant_client.http import models
from sentence_transformers import SentenceTransformer
import ollama
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Connect to Qdrant
QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "txt_file_data"

qdrant_client = qdrant_client.QdrantClient(QDRANT_URL)

# Initialize Sentence Transformer for Embeddings
embedder = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# Load Mistral model via Ollama
MODEL_NAME = "mistral:7b"
try:
    response = ollama.chat(model=MODEL_NAME, messages=[{"role": "system", "content": "You are an AI assistant, but you only structure answers retrieved from Qdrant. You will not generate any response beyond the retrieved data."}])
    logger.info(f"✅ Mistral model '{MODEL_NAME}' configured for strict RAG-based responses.")
except Exception as e:
    logger.error(f"❌ Error loading Mistral model: {str(e)}")
    exit(1)

def retrieve_data_from_qdrant(user_query):
    """ Converts user query to vector, searches Qdrant using `point_search()`, and retrieves matched results. """

    # Convert query to vector
    query_vector = embedder.encode(user_query).tolist()

    # Perform vector search using `point_search()`
    search_results = qdrant_client.point_search(
        collection_name=COLLECTION_NAME,
        query_vector=query_vector,
        limit=5,  # Retrieve top 5 relevant results
        with_payload=True,  # Ensure payload is included in results
        with_vectors=False  # Only retrieve the payload, not the vectors
    )

    # Extract relevant answers
    matched_responses = [result.payload["content"] for result in search_results if "content" in result.payload]

    if not matched_responses:
        return "**No relevant answer found in the vector database.** Please refine your query."

    return "\n".join(matched_responses)


def generate_final_answer(user_question):
    """ Retrieves relevant data using RAG and reformats response via Mistral. """

    # Step 1: Retrieve relevant content
    retrieved_context = retrieve_data_from_qdrant(user_question)

    # Step 2: Generate final answer using Mistral, **without adding its own response**
    mistral_prompt = f"""
    You are an AI assistant restricted to **only formatting answers from the retrieved context**.  
    You are a strict RAG-based model. Your only job is to format retrieved answers from the database.
    ❌ Do NOT generate new information.
    ✅ Only structure the retrieved answer for better readability. 
      
    **User Question:**  
    {user_question}
    
    **Retrieved Context from Qdrant Vector Database:**  
    {retrieved_context}
    
    **How to Answer:**  
    🔹 Reformat the retrieved answer concisely.  
    🔹 Do **not** add extra AI-generated responses.  
    🔹 Ensure the structure improves readability but keeps the original meaning intact.  

    **Final Answer (structured without additional AI-generated details):**
    """

    try:
        final_response = ollama.chat(model=MODEL_NAME, messages=[{"role": "user", "content": mistral_prompt}])
        return final_response["message"]["content"]
    except Exception as e:
        logger.error(f"❌ Error generating answer via Mistral: {str(e)}")
        return "Failed to process request."

# Execute Query with User Input
if __name__ == "__main__":
    user_query = input("📝 Enter your question: ")  # Allow dynamic user input
    final_answer = generate_final_answer(user_query)
    print(f"\n🎯 Final Answer:\n{final_answer}")
