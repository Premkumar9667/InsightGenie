from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re
import logging
import sys

# Setup logging for debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# List of URLs to scrape (all from https://www.kongunadu.ac.in)
urls_to_scrape = [
    "https://www.kongunadu.ac.in/about-us-menu/kongunadu-profile.html",
    "https://www.kongunadu.ac.in/about-us-menu/kongunadu-vision-and-mission.html",
    "https://www.kongunadu.ac.in/about-us-menu/knect-trustees.html",
    "https://www.kongunadu.ac.in/about-us-menu/kongunadu-engineering-courses-offered.html",
    "https://www.kongunadu.ac.in/kongunadu-departments/bio-medical/department-vision-and-mission-bio.html",
    "https://www.kongunadu.ac.in/kongunadu-departments/kongunadu-be-civil-engineering/profile.html",
    "https://www.kongunadu.ac.in/kongunadu-departments/be-computer-science-engineering/profile.html",
    "https://www.kongunadu.ac.in/kongunadu-departments/kongunadu-be-eee/profile.html",
    "https://www.kongunadu.ac.in/kongunadu-departments/kongunadu-be-ece/profile.html",
    "https://www.kongunadu.ac.in/kongunadu-departments/be-mechanical/profile.html",
    "https://www.kongunadu.ac.in/images/PDF/coursesnew/Agri_Content_Poster.pdf",
    "https://www.kongunadu.ac.in/kongunadu-departments/artificial-intelligence-data-science/profile-aids.html",
    "https://www.kongunadu.ac.in/kongunadu-departments/btech-it/it-profile.html",
    "https://www.kongunadu.ac.in/kongunadu-departments/kongunadu-be-ece/profile.html",
    "https://www.kongunadu.ac.in/kongunadu-departments/be-computer-science-engineering/profile.html",
    "https://www.kongunadu.ac.in/kongunadu-departments/kongunadu-be-ece/profile.html",
    "https://www.kongunadu.ac.in/about-us-menu/engineering-admission-eligibility-criteria.html",
    # Add more URLs as needed
]

# Validate that all URLs belong to the same website
base_domain = "https://www.kongunadu.ac.in"
for url in urls_to_scrape:
    if not url.startswith(base_domain):
        logger.error(f"URL {url} does not belong to {base_domain}")
        sys.exit(1)

data = []

def scrape_page(url):
    try:
        # Use Playwright to handle dynamic content
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Set user-agent to avoid bot detection
            page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            })
            
            logger.info(f"Loading page: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            
            # Wait for the content to load
            page.wait_for_timeout(5000)  # Wait 5 seconds for JavaScript to render
            logger.info("Page loaded successfully")

            # Get the page content
            page_content = page.content()
            if not page_content:
                logger.error("Failed to retrieve page content")
                return None
            logger.info("Page content retrieved successfully")

            # Parse the content with BeautifulSoup
            soup = BeautifulSoup(page_content, 'html.parser')
            if not soup:
                logger.error("Failed to parse page content with BeautifulSoup")
                return None
            logger.info("Page content parsed successfully")

            # Extract the title
            title = soup.title.string.strip() if soup.title else "No Title"
            logger.info(f"Page title: {title}")

            # Remove unwanted elements
            for element in soup(["script", "style"]):
                element.decompose()
            logger.info("Removed unwanted elements (scripts, styles)")

            # Try to find the main content section with more specific selectors
            content_section = None
            possible_classes = [
                'contact', 'content', 'main', 'body', 'contact-info', 'contact-details',
                'contact-us', 'address', 'info', 'details', 'about', 'course', 'courses'
            ]
            for class_name in possible_classes:
                content_section = soup.find('div', class_=re.compile(class_name, re.I)) or \
                                 soup.find('section', class_=re.compile(class_name, re.I))
                if content_section:
                    raw_content = content_section.get_text(separator=' ', strip=True)
                    if raw_content and not raw_content.isspace():
                        logger.info(f"Found content section with class: {class_name}")
                        logger.info(f"Extracted raw content: {raw_content[:200]}...")
                        break
                    else:
                        content_section = None  # Reset if the section is empty
                        logger.debug(f"Section with class '{class_name}' found but contains no meaningful content")

            # Fallback: Search for keywords if no suitable section is found
            if not content_section:
                logger.warning("Could not find specific content section, searching for keywords...")
                keywords = ['Contact:', 'Address:', 'Phone:', 'Email:', 'Location:', 'About', 'Courses']
                for element in soup.find_all(['p', 'div', 'span', 'li']):
                    text = element.get_text(separator=' ', strip=True)
                    if any(keyword in text for keyword in keywords):
                        content_section = element
                        break
                
                if content_section:
                    raw_content = content_section.get_text(separator=' ', strip=True)
                    logger.info(f"Found content section via keyword search")
                    logger.info(f"Extracted raw content: {raw_content[:200]}...")
                else:
                    logger.warning("Keyword search failed, falling back to entire body")
                    content_section = soup.find('body')
                    raw_content = content_section.get_text(separator=' ', strip=True) if content_section else ""
                    logger.info(f"Extracted raw content (fallback): {raw_content[:200]}...")

            # Check if raw content is empty or only whitespace
            if not raw_content or raw_content.isspace():
                logger.error("Raw content is empty or contains only whitespace")
                return None

            # Minimal cleaning to preserve content details
            text_content = re.sub(r'\s+', ' ', raw_content).strip()
            text_content = re.sub(r'(\bMenu\b|\bHome\b|\bQuick Links\b).*?(?=\w)', '', text_content)
            logger.info(f"Cleaned content: {text_content[:200]}...")

            # Final check for content
            if not text_content or text_content.isspace():
                logger.error("No content after cleaning")
                return None

            browser.close()
            return title, text_content

    except Exception as e:
        logger.error(f"Error scraping {url}: {str(e)}", exc_info=True)
        return None

# Scrape the specified pages
logger.info("Starting web scraping process...")
for url in urls_to_scrape:
    logger.info(f"Scraping URL: {url}")
    result = scrape_page(url)
    if result:
        title, content = result
        data.append({
            "url": url,
            "title": title,
            "content": content
        })
        logger.info(f"Successfully scraped {url}")
    else:
        logger.error(f"Failed to scrape {url} or extract content")

# Save to a .txt file
try:
    with open("kongunadu_data.txt", "w", encoding="utf-8") as f:
        for idx, entry in enumerate(data):
            f.write(f"--- Page {idx + 1} ---\n")
            f.write(f"URL: {entry['url']}\n")
            f.write(f"Title: {entry['title']}\n")
            f.write(f"Content: {entry['content']}\n")
            f.write("\n")
    print("✅ Saved dataset to kongunadu_data.txt")
    logger.info("Dataset saved to kongunadu_data.txt")
except Exception as e:
    logger.error(f"Error saving dataset to txt: {str(e)}", exc_info=True)
    sys.exit(1)