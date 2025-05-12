from qdrant_client import QdrantClient
from qdrant_client.http import models
from sentence_transformers import SentenceTransformer
import pandas as pd
import logging
import sys
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connect to Qdrant
try:
    qdrant_client = QdrantClient("localhost", port=6333)
    logger.info("✅ Connected to Qdrant successfully")
except Exception as e:
    logger.error(f"❌ Failed to connect to Qdrant: {str(e)}")
    sys.exit(1)

# Define the collection name
collection_name = "web_scraping_data"

# Initialize the sentence transformer for embedding
embedder = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')

# Auto-detect the vector dimension
try:
    sample_text = "This is a test sentence."
    sample_embedding = embedder.encode([sample_text], convert_to_tensor=False)[0]
    vector_dimension = len(sample_embedding)  # Should be 768 for this model
    logger.info(f"✅ Detected embedding dimension: {vector_dimension}")
except Exception as e:
    logger.error(f"❌ Failed to detect embedding dimension: {str(e)}")
    sys.exit(1)

# Ensure collection matches the correct vector dimension
if qdrant_client.collection_exists(collection_name):
    qdrant_client.delete_collection(collection_name)
    logger.info(f"🗑 Deleted previous collection: {collection_name}")

qdrant_client.create_collection(
    collection_name=collection_name,
    vectors_config=models.VectorParams(size=vector_dimension, distance=models.Distance.COSINE),
)
logger.info(f"✅ Created collection {collection_name} with vector dimension {vector_dimension}")

# Selenium Setup for Web Scraping
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("user-agent=Mozilla/5.0")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
base_url = "https://www.kongunadu.ac.in/kongunadu-contact.html"

def scrape_page(url):
    """ Extracts structured content dynamically without predefined keywords """
    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        soup = BeautifulSoup(driver.page_source, "html.parser")

        structured_sections = []

        # Identify and categorize sections properly
        for section in soup.find_all(["div", "section", "article"]):
            section_title = section.find("h1") or section.find("h2") or section.find("h3")
            section_title = section_title.get_text(strip=True) if section_title else "Unnamed Section"

            section_content = []
            for tag in ["h1", "h2", "h3", "p", "ul", "li"]:
                elements = section.find_all(tag)
                for element in elements:
                    section_content.append({"tag": tag, "text": element.get_text(strip=True)})

            if section_content:
                structured_sections.append({"section": section_title, "elements": section_content})

        return structured_sections

    except Exception as e:
        logger.error(f"❌ Error scraping {url}: {str(e)}")
        return []

# Start scraping
scraped_data = scrape_page(base_url)
driver.quit()

# Store in Qdrant
points = []
point_id = 0

for section in scraped_data:
    section_name = section["section"]
    try:
        embeddings = embedder.encode([elem["text"] for elem in section["elements"]], convert_to_tensor=False)
        
        for embedding, element in zip(embeddings, section["elements"]):
            points.append(
                models.PointStruct(
                    id=point_id,
                    vector=embedding.tolist(),
                    payload={
                        "section": section_name,
                        "tag": element["tag"],
                        "content": element["text"]
                    }
                )
            )
            point_id += 1

    except Exception as e:
        logger.error(f"❌ Error during batch encoding for section {section_name}: {str(e)}")
        continue

# Store in Qdrant
try:
    qdrant_client.upsert(collection_name=collection_name, points=points)
    logger.info(f"✅ Successfully stored {len(points)} entries in Qdrant.")
except Exception as e:
    logger.error(f"❌ Failed to upsert points to Qdrant: {str(e)}")
    sys.exit(1)

print("🎯 Section-wise scraped content stored dynamically in Qdrant!")
