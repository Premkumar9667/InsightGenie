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
MODEL_NAME = "mistral:7B"
try:
    response = ollama.chat(model=MODEL_NAME, messages=[{"role": "system", "content": "You are an AI assistant."}])
    logger.info(f"✅ Mistral model '{MODEL_NAME}' loaded successfully.")
except Exception as e:
    logger.error(f"❌ Error loading Mistral model: {str(e)}")
    exit(1)

def retrieve_data_from_qdrant(user_query):
    """ Converts user query to vector, searches Qdrant, and retrieves matched results. """

    # Convert query to vector
    query_vector = embedder.encode(user_query).tolist()

    # Search in Qdrant
    search_results = qdrant_client.search(
        collection_name=COLLECTION_NAME,
        query_vector=query_vector,
        limit=5,  # Retrieve top 5 relevant results
        query_filter=models.Filter(must=[models.FieldCondition(key="type", match=models.MatchValue(value="answer"))])
    )

    # Extract relevant answers
    matched_responses = [result.payload["content"] for result in search_results]

    if not matched_responses:
        return "No relevant answer found in the vector database."

    return "\n".join(matched_responses)

def analyze_question_format(user_question):
    """ Mistral reformulates the user's question for better retrieval. """
    analysis_prompt = f"""
    The user asked: "{user_question}".  
    Reformulate the question into a structured format to ensure proper retrieval.
    """

    try:
        analysis_response = ollama.chat(model=MODEL_NAME, messages=[{"role": "user", "content": analysis_prompt}])
        return analysis_response["message"]["content"]
    except Exception as e:
        logger.error(f"❌ Error analyzing question format via Mistral: {str(e)}")
        return user_question  # Default to original query if model fails

def generate_final_answer(user_question):
    """ Retrieves relevant data using RAG and reformulates response via Mistral. """

    # Step 1: Analyze question format
    structured_query = analyze_question_format(user_question)
    logger.info(f"🔍 Reformulated Query: {structured_query}")

    # Step 2: Retrieve relevant content
    retrieved_context = retrieve_data_from_qdrant(structured_query)

    # Step 3: Generate final answer using Mistral
    mistral_prompt = f"""
    You are an intelligent AI assistant. Your goal is to provide a **clear, informative, and structured answer** to the user's question.
    
    
    **User Question:**  
    {user_question}
    
    **Context Retrieved from Qdrant Vector Database:**  
    {retrieved_context}
    
    **How to Answer:**  
    1️⃣ First, **analyze the question type** (Informational, Definition-based, Reasoning, etc.).  
    2️⃣ Use relevant **retrieved data only give vector database collection only don't give own think ** as supporting content for the response.  
    3️⃣ Ensure your answer is **concise, direct, and user-friendly**.  
    4️⃣ If there are **multiple relevant points**, provide a **bullet list** for clarity.  

    **Final Answer (structured appropriately for the user):**
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
