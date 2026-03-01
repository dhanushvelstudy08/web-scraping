# ==========================================================
# WINDOWS FIX (VERY IMPORTANT - MUST BE FIRST)
# ==========================================================
import asyncio
import sys

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# ==========================================================
# IMPORTS
# ==========================================================
import nest_asyncio
nest_asyncio.apply()

import pandas as pd
import streamlit as st
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
import re
import os
from urllib.parse import urlparse, parse_qs, unquote

# ==========================================================
# SETTINGS
# ==========================================================
MAX_CONCURRENT = 5
RETRY_LIMIT = 2
BATCH_SIZE = 50
OUTPUT_FILE = "linkedin_company_data.xlsx"

# ==========================================================
# NORMALIZE LINKEDIN URL
# ==========================================================
def normalize_linkedin_company_url(url):
    if not url:
        return None

    url = url.split("?")[0].split("#")[0]

    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("/"):
        url = "https://www.linkedin.com" + url

    remove_parts = ["/posts", "/about", "/life", "/jobs"]
    for part in remove_parts:
        if part in url:
            url = url.split(part)[0]

    return url.rstrip("/")

# ==========================================================
# EXTRACT LINKEDIN URL FROM DOMAIN WEBSITE
# ==========================================================
def extract_linkedin_company(html):
    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "linkedin.com/company/" in href:
            clean_url = normalize_linkedin_company_url(href)
            if clean_url:
                candidates.append(clean_url)

    candidates = list(set(candidates))
    return candidates[0] if candidates else None

# ==========================================================
# CLEAN FUNCTIONS
# ==========================================================
def clean_industry(industry):
    if not industry:
        return None
    industry = re.sub(r'\s{2,}', ' ', industry.strip())
    if len(industry) > 100:
        industry = industry[:100]
    return industry


def clean_linkedin_website(href):
    if not href:
        return None

    if "linkedin.com/redir/redirect" in href:
        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        real_url = qs.get("url")
        if real_url:
            return unquote(real_url[0])

    return href

# ==========================================================
# EXTRACT COMPANY INFO FROM LINKEDIN PAGE
# ==========================================================
def extract_company_info(html):
    soup = BeautifulSoup(html, "html.parser")

    industry = None
    company_size = None
    website = None

    for dt in soup.find_all("dt"):
        label = dt.get_text(strip=True).lower()
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue

        if "industry" in label:
            industry = clean_industry(dd.get_text(" ", strip=True))

        elif "company size" in label:
            company_size = dd.get_text(" ", strip=True)

        elif "website" in label:
            link = dd.find("a", href=True)
            if link:
                website = clean_linkedin_website(link["href"])

    return industry, company_size, website

# ==========================================================
# PROCESS DOMAIN → GET LINKEDIN URL
# ==========================================================
async def process_domain(crawler, domain, semaphore):
    async with semaphore:
        for attempt in range(RETRY_LIMIT):
            try:
                result = await crawler.arun(url=f"https://{domain}")
                html = result.html

                linkedin_url = extract_linkedin_company(html)

                return {
                    "domain": domain,
                    "linkedin_url": linkedin_url,
                    "industry": None,
                    "company_size": None,
                    "company_website": None,
                    "status": "success" if linkedin_url else "not_found"
                }

            except Exception:
                if attempt == RETRY_LIMIT - 1:
                    return {
                        "domain": domain,
                        "linkedin_url": None,
                        "industry": None,
                        "company_size": None,
                        "company_website": None,
                        "status": "error"
                    }

# ==========================================================
# PROCESS LINKEDIN → GET COMPANY DATA
# ==========================================================
async def process_linkedin(crawler, record, semaphore):
    async with semaphore:
        url = record["linkedin_url"]

        for attempt in range(RETRY_LIMIT):
            try:
                result = await crawler.arun(url=url)
                html = result.html

                industry, size, website = extract_company_info(html)

                record["industry"] = industry
                record["company_size"] = size
                record["company_website"] = website

                return record

            except Exception:
                if attempt == RETRY_LIMIT - 1:
                    return record

# ==========================================================
# SAVE FUNCTION
# ==========================================================
def save_results(results):
    df = pd.DataFrame(results)
    df.to_excel(OUTPUT_FILE, index=False)

# ==========================================================
# MAIN SCRAPER FUNCTION
# ==========================================================
async def scrape_domains(domains):

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    results = []

    async with AsyncWebCrawler() as crawler:

        # -------------------------------
        # STEP 1: DOMAIN → LINKEDIN
        # -------------------------------
        st.write("🔎 Finding LinkedIn URLs...")

        tasks = [process_domain(crawler, d, semaphore) for d in domains]

        for i, task in enumerate(asyncio.as_completed(tasks)):
            res = await task
            results.append(res)
            st.write(f"{i+1}/{len(domains)} → {res['domain']}")

        # -------------------------------
        # STEP 2: LINKEDIN → COMPANY INFO
        # -------------------------------
        st.write("📊 Extracting LinkedIn company data...")

        linkedin_records = [r for r in results if r["linkedin_url"]]

        tasks = [process_linkedin(crawler, r, semaphore) for r in linkedin_records]

        updated_records = []
        for i, task in enumerate(asyncio.as_completed(tasks)):
            res = await task
            updated_records.append(res)
            st.write(f"{i+1}/{len(linkedin_records)} → {res['linkedin_url']}")

        # Merge updated data back
        for updated in updated_records:
            for original in results:
                if original["domain"] == updated["domain"]:
                    original.update(updated)

    save_results(results)
    st.success("✅ Scraping Completed!")
    st.download_button(
        "Download Result File",
        data=open(OUTPUT_FILE, "rb"),
        file_name=OUTPUT_FILE
    )

# ==========================================================
# STREAMLIT UI
# ==========================================================
# ==========================================================
# STREAMLIT UI
# ==========================================================
st.set_page_config(
    page_title="Workflow Optimization Tools",
    page_icon="⚙️",
    layout="wide"
)

# ==========================================================
# SIDEBAR UI
# ==========================================================
st.sidebar.title("⚙️ Workflow Optimization Tools")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Select Tool",
    [
        "🔎 LinkedIn Data Scraper",
        "🧩 Fuzzy Duplicate Finder"
    ]
)

st.sidebar.markdown("---")
st.sidebar.caption("Internal Automation Dashboard")

# ==========================================================
# FUZZY DUPLICATE FINDER PAGE
# ==========================================================
if page == "🧩 Fuzzy Duplicate Finder":
    st.title("🧩 Fuzzy Duplicate Finder")
    st.markdown("Open the tool below in a new tab.")

    st.link_button(
        "Open Fuzzy Duplicate Finder",
        "https://qc-dup-automation.streamlit.app/"
    )

# ==========================================================
# LINKEDIN SCRAPER PAGE
# ==========================================================
if page == "🔎 LinkedIn Data Scraper":

    st.title("Domain → LinkedIn Company Scraper")

    uploaded_file = st.file_uploader(
        "Upload Excel/CSV with 'domain' column",
        type=["csv", "xlsx"]
    )

    if uploaded_file:
        df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith("xlsx") else pd.read_csv(uploaded_file)

        if "domain" not in df.columns:
            st.error("File must contain a 'domain' column.")
        else:
            domains = df["domain"].dropna().tolist()
            st.success(f"Loaded {len(domains)} domains.")

            if st.button("Start Scraping"):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(scrape_domains(domains))