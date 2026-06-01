import asyncio
import requests
import firebase_admin
import json
import os
import gspread
from datetime import datetime, timezone
from google import genai
from google.oauth2.service_account import Credentials
from firebase_admin import credentials as fire_credentials, firestore
from playwright.async_api import async_playwright

# --- GATEWAY AUTHENTICATION ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
FIREBASE_JSON_STR = os.environ.get("FIREBASE_JSON")
GOOGLE_SHEET_URL = os.environ.get("GOOGLE_SHEET_URL")

ai_client = genai.Client(api_key=GEMINI_API_KEY)
firebase_creds_dict = json.loads(FIREBASE_JSON_STR)

# Initialize Firebase
if not firebase_admin._apps:
    cred = fire_credentials.Certificate(firebase_creds_dict)
    firebase_admin.initialize_app(cred)
db = firestore.client()

# Initialize Google Sheets
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
sheet_creds = Credentials.from_service_account_info(firebase_creds_dict, scopes=scopes)
gc = gspread.authorize(sheet_creds)
sheet = gc.open_by_url(GOOGLE_SHEET_URL).sheet1

def extract_real_estate_data(ad_text):
    prompt = f"""
    You are an expert real estate market analyst in Sri Lanka. 
    Analyze the following Facebook ad text and extract the vital data points.
    Return your output structured exactly like this template, with no extra conversation:
    
    Location: [City/Area name]
    Price: [Extracted price or stated rate, otherwise 'Not explicitly mentioned']
    Focus: [Core strategy focus: e.g., Highway access, 10-perch blocks]
    
    Ad Text to analyze:
    {ad_text}
    """
    response = ai_client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
    
    # Parse the AI response into a dictionary for clean spreadsheet insertion
    lines = response.text.strip().split('\n')
    data = {"Location": "Unknown", "Price": "Unknown", "Focus": "Unknown"}
    for line in lines:
        if "Location:" in line: data["Location"] = line.split("Location:")[1].strip().replace("**", "")
        if "Price:" in line: data["Price"] = line.split("Price:")[1].strip().replace("**", "")
        if "Focus:" in line: data["Focus"] = line.split("Focus:")[1].strip().replace("**", "")
    return response.text.strip(), data

def update_google_sheet(ad_hash, company, parsed_data, count, status, first_seen, last_verified, url):
    # Search if row exists, update if it does, append if new
    try:
        cell = sheet.find(ad_hash)
        sheet.update_cell(cell.row, 6, count)          # Active Instances
        sheet.update_cell(cell.row, 7, status)         # Status
        sheet.update_cell(cell.row, 9, last_verified)  # Last Verified
    except gspread.exceptions.CellNotFound:
        new_row = [ad_hash, company, parsed_data['Location'], parsed_data['Price'], 
                   parsed_data['Focus'], count, status, first_seen, last_verified, url]
        sheet.append_row(new_row)

def send_discord_alert(company, ai_analysis, count, ad_url):
    payload = {"content": f"🚨 **New Ad Configuration Identified: {company}** (Active Instances: {count})\n\n{ai_analysis}\n\n🔗 [Inspect on Meta Ad Library]({ad_url})"}
    requests.post(DISCORD_WEBHOOK_URL, json=payload)

async def process_live_ad_elements(company_name, page_id, live_ad_texts, target_url):
    current_run_hashes = set()
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    
    for text in live_ad_texts:
        ad_hash = f"hash_{abs(hash(text))}"
        current_run_hashes.add(ad_hash)
        
        doc_ref = db.collection('ads').document(ad_hash)
        doc_snapshot = doc_ref.get()
        
        if not doc_snapshot.exists:
            print(f"💥 Ground-new configuration discovered for {company_name}.")
            try:
                ai_text, parsed_data = extract_real_estate_data(text)
                
                doc_ref.set({
                    "company": company_name, "page_id": page_id, "raw_text": text,
                    "analysis_snapshot": ai_text, "status": "active", "instance_count": 1,
                    "first_seen": timestamp, "last_verified": timestamp
                })
                
                update_google_sheet(ad_hash, company_name, parsed_data, 1, "Active", timestamp, timestamp, target_url)
                send_discord_alert(company_name, ai_text, 1, target_url)
                await asyncio.sleep(5) 
            except Exception as e:
                print(f"⚠️ Neural translation delay: {e}.")
        else:
            ad_data = doc_snapshot.to_dict()
            new_count = ad_data.get("instance_count", 1) + 1
            first_seen = ad_data.get("first_seen", timestamp)
            
            doc_ref.update({"status": "active", "instance_count": new_count, "last_verified": timestamp})
            
            # Dummy parsed data since it's an update, sheet logic only updates count/status
            dummy_data = {"Location": "", "Price": "", "Focus": ""} 
            update_google_sheet(ad_hash, company_name, dummy_data, new_count, "Active", first_seen, timestamp, target_url)
            print(f".", end="", flush=True)
            
    return current_run_hashes

async def prune_inactive_ads(company_name, seen_hashes):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    active_ads_in_db = db.collection('ads').where('company', '==', company_name).where('status', '==', 'active').stream()
                         
    for doc in active_ads_in_db:
        if doc.id not in seen_hashes:
            db.collection('ads').document(doc.id).update({"status": "inactive", "instance_count": 0})
            dummy_data = {"Location": "", "Price": "", "Focus": ""}
            update_google_sheet(doc.id, company_name, dummy_data, 0, "Inactive", "", timestamp, "")

async def scrape_ads():
    pages_stream = db.collection('monitored_pages').stream()
    targets = [{"name": doc.to_dict()['company_name'], "id": doc.to_dict()['page_id']} for doc in pages_stream]
    
    if not targets: return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        for target in targets:
            url = f"https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=LK&view_all_page_id={target['id']}"
            await page.goto(url)
            await page.wait_for_timeout(7000) 
            
            elements = await page.query_selector_all('div[style*="white-space: pre-wrap;"]')
            live_texts = [await el.inner_text() for el in elements if len(await el.inner_text()) >= 40]
            
            seen_hashes = await process_live_ad_elements(target["name"], target["id"], live_texts, url)
            await prune_inactive_ads(target["name"], seen_hashes)
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(scrape_ads())
