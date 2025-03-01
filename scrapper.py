import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
from requests_html import HTMLSession
import time
import random

def extract_emails(soup):
    """
    Extract emails from a BeautifulSoup object by checking mailto links, text content, and obfuscated emails.
    Returns a set of unique, lowercased emails.
    """
    emails = set()
    
    for a_tag in soup.find_all('a', href=True):
        if a_tag['href'].startswith('mailto:'):
            email = a_tag['href'].split('?')[0].replace('mailto:', '').strip().lower()
            if validate_email(email):
                emails.add(email)
    
    text = ' '.join(soup.stripped_strings)
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}'
    potential_emails = re.findall(email_pattern, text)
    for email in potential_emails:
        if validate_email(email): 
            emails.add(email.lower())
    
    script_tags = soup.find_all('script')
    for script in script_tags:
        script_text = script.get_text()
        emails.update(extract_obfuscated_emails(script_text))
    
    return emails

def extract_obfuscated_emails(text):
    """
    Extract emails obfuscated in JavaScript or HTML (e.g., string concatenation or character entities).
    Returns a set of validated emails.
    """
    emails = set()
    
    concat_pattern = r'[\'"][a-zA-Z0-9._%+-]+[\'"]\s*\+\s*[\'"]\@[\'"]\s*\+\s*[\'"][a-zA-Z0-9.-]+\.[A-Za-z]{2,}[\'"]'
    matches = re.findall(concat_pattern, text)
    for match in matches:
        parts = re.findall(r'[\'"]([^\'"]*)[\'"]\s*\+\s*', match + "+")
        reconstructed = ''.join(parts)
        if '@' in reconstructed and validate_email(reconstructed):
            emails.add(reconstructed)
    
    text_decoded = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
    emails.update(extract_emails_from_text(text_decoded))
    
    return emails

def extract_emails_from_text(text):
    """
    Extract emails from plain text using regex.
    Returns a set of validated emails.
    """
    if not text:
        return set()
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}'
    emails = set()
    matches = re.findall(email_pattern, text)
    for match in matches:
        email = match.lower().strip()
        if validate_email(email):
            emails.add(email)
    return emails

def validate_email(email):
    """
    Validate if a string is a proper email address, excluding common false positives.
    Returns True if valid, False otherwise.
    """
    if not email or '@' not in email:
        return False
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}$'
    common_false_positives = [
        'example.com', 'domain.com', 'email.com', 'your-email.com',
        'username@', '@domain', 'example@example'
    ]
    
    if any(fp in email.lower() for fp in common_false_positives):
        return False
    
    local_part = email.split('@')[0]
    phone_pattern = r'\d{3}-\d{3}-\d{4}'
    if re.search(phone_pattern, local_part):
        return False
    
    return bool(re.match(pattern, email))

def find_subpage_urls(soup, base_url):
    """
    Find subpage URLs that might contain contact info based on keywords in link text and URL path.
    Returns a set of absolute URLs within the same domain.
    """
    keywords = [
        "contact", "about", "reach", "support", "help", "info", "team", "staff",
        "brokers", "get in touch", "our people", "meet the team", "directory",
        "contact us", "about us", "reach us", 
    ]
    subpage_urls = set()
    base_netloc = urlparse(base_url).netloc
    
    for a_tag in soup.find_all('a', href=True):
        link_text = a_tag.get_text().strip().lower()
        href = a_tag['href']
        abs_url = urljoin(base_url, href)
        parsed_url = urlparse(abs_url)
        
        if (any(keyword in link_text for keyword in keywords) or 
            any(keyword in parsed_url.path.lower() for keyword in keywords)):
            if parsed_url.netloc == base_netloc and parsed_url.scheme in ['http', 'https']:
                subpage_urls.add(abs_url)
    
    return subpage_urls

def process_url(url, session, headers, visited_urls, max_retries=3):
    """
    Process a URL to extract emails and find subpages, with retry logic for failed requests.
    Returns a tuple of (emails, subpages).
    """
    emails = set()
    subpages = []
    
    if url in visited_urls:
        return emails, subpages
    
    visited_urls.add(url)
    
    for attempt in range(max_retries):
        try:
            response = session.get(url, headers=headers, timeout=10)
            response.html.render(sleep=10, timeout=60)  
            soup = BeautifulSoup(response.html.html, 'html.parser')
            emails.update(extract_emails(soup))
            subpages.extend(find_subpage_urls(soup, url))
            break
        except requests.RequestException as e:
            print(f"Attempt {attempt+1}/{max_retries} failed for {url}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 * (attempt + 1))
    else:
        print(f"Failed to retrieve {url} after {max_retries} attempts")
    
    return emails, subpages

def find_emails(base_url, max_subpages=10, max_retries=2, delay_between_requests=(1, 3)):
    """
    Extract emails from a website and its subpages.
    Returns a list of unique emails.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    if not base_url.startswith(('http://', 'https://')):
        base_url = 'https://' + base_url
    
    visited_urls = set()
    all_emails = set()
    subpages_to_visit = []
    
    try:
        session = HTMLSession()
        
        print(f"Scraping base URL: {base_url}")
        emails, subpages = process_url(base_url, session, headers, visited_urls, max_retries)
        all_emails.update(emails)
        
        # if all_emails:
        #     print(f"Emails found on base URL: {all_emails}. Skipping subpage crawling.")
        #     return list(all_emails)
        
        subpages_to_visit.extend(subpages)
        pages_visited = 1
        while subpages_to_visit and pages_visited < max_subpages:
            current_url = subpages_to_visit.pop(0)
            if current_url in visited_urls:
                continue
            
            time.sleep(random.uniform(delay_between_requests[0], delay_between_requests[1]))
            print(f"Scraping subpage ({pages_visited}/{max_subpages}): {current_url}")
            
            emails, _ = process_url(current_url, session, headers, visited_urls, max_retries)
            all_emails.update(emails)
            
            pages_visited += 1
    
    except Exception as e:
        print(f"Error during scraping: {str(e)}")
    
    return list(all_emails)

# url = "mostnyc.com"
# emails = find_emails(url)
# print(f"Found {len(emails)} unique email addresses:")
# for email in emails:
#     print(f"  - {email}")