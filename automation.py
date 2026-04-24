import os
import json
import csv
import datetime
import time
import argparse
import smtplib
import re
from email.message import EmailMessage
from google import genai
from google.genai import types

# --- CONFIGURATION ---
API_KEY = os.getenv("GEMINI_API_KEY")

# Initialize the Modern 2025 Google GenAI Client
client = genai.Client(api_key=API_KEY)

def load_nation_info(nation_id, list_name=None):
    """Loads specific nation configuration from the master JSON brain."""
    with open('nation_config.json', 'r') as f:
        config = json.load(f)

    if list_name:
        if list_name in config:
            for n in config[list_name]:
                if n['id'] == nation_id:
                    n['tier_number'] = list_name
                    return n
        return None
        
    for tier, nations in config.items():
        for n in nations:
            if n['id'] == nation_id:
                n['tier_number'] = tier
                return n
    return None

def fetch_new_deals_with_retries(nation_info, num_attempts=1, model_name="gemini-2.5-flash"):
    """Runs multiple search attempts and aggregates unique results."""
    all_deals = []
    seen_names = set()
    
    for attempt in range(num_attempts):
        print(f"Attempt {attempt + 1}/{num_attempts} for {nation_info['label']}")
        deals = fetch_new_deals(nation_info, model_name)
        
        for deal in deals:
            name_key = deal['Startup_Name'].lower().strip()
            if name_key not in seen_names:
                deal['Tier'] = nation_info['tier_number']
                deal['Nation'] = nation_info['label']
                deal['Flag'] = nation_info['flag']
                all_deals.append(deal)
                seen_names.add(name_key)

        if attempt < num_attempts - 1:
            time.sleep(60)
    
    print(f"Total unique deals found: {len(all_deals)}")
    return all_deals

def fetch_new_deals(nation_info, model_name="gemini-2.5-flash"):
    """Calls Gemini with configurable model. Note: JSON mode is disabled to allow Tool use."""
    today = datetime.date.today().isoformat()
    week_ago = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
    
    prompt = f"""
    You are a venture ca[ital intelligence analyst. Perform a COMPREHENSIVE search for ALL NEW funding announcements in the {nation_info['label']} startup ecosystem from {week_ago} to today (last 7 days).
    MANDATORY SOURCES TO CHECK: {nation_info['sources']}.
    {nation_info.get('prompt_extra', '')}

    SEARCH STRATEGY:
    1. Search each source individually.
    2. Use multiple sseach queries per source.
    3. Cross-reference announcements across sources.
    4. Include both major and emerging deals.
    
    FILTERS:
    1. Category: AI, Data, Machine Learning, SaaS, or Data Infrastructure.
    2. Stage: Pre-Series A, Seed, Seed-plus, debt, Series A, and above.
    
    CRITICAL: RETURN A RAW JSON LIST ONLY. 
    Do not include any conversational text before or after the JSON.
    Format:
    [
      {{
        "Country": "Organization Country name",
        "Startup_Name": "Name",
        "Description": "2-line business summary",
        "Amount": "Amount (USD)",
        "Round": "Funding Stage",
        "Investors": "Comma-separated investor list",
        "Founders": "Founder names",
        "LinkedIn_Profile": "Founder LinkedIn URL or N/A if not available",
        "Hiring": "Status: Yes/No/Unknown",
        "Careers_Link": "Careers page URL or N/A if not available"
      }}
    ]
    If ZERO deals found after thorough search, return: []
    
    IMPORTANT: Be exhaustive. Missing a deal is worse than finding none.
    """

    try:
        response = client.models.generate_content(
            model=model_name,
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[types.Tool(google_search=types.GoogleSearch())],
                temperature=0.2,
                top_k=40,
                top_p=0.95
            )
        )
        print(f"Raw Gemini Response: {response.text[:500]}")  # Debug line
        
        # Manually extract and clean JSON from the response text
        raw_text = response.text.strip()
        json_match = re.search(r'\[.*\]', raw_text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        
        print(f"Warning: No valid JSON array found for {nation_info['id']}")
        return []
        
    except json.JSONDecodeError as je:
        print(f"JSON Parsing Error for {nation_info['id']}: {je}")
        return []
    except genai.errors.ClientError as ce:
        print(f"Gemini API Client Error for {nation_info['id']}: {ce}")
        return []
    except Exception as e:
        print(f"Intelligence Error for {nation_info['id']}: {e}")
        return []

def process_historical_data(nation_id, fetched_deals):
    """Handles deduplication and long-term storage in the data/ folder."""
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)
    file_path = f"{data_dir}/{nation_id}.json"
    
    history = {"deals": []}
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            try:
                history = json.load(f)
            except:
                history = {"deals": []}
            
    existing_names = {d['Startup_Name'].lower().strip() for d in history['deals']}
    
    new_unique_deals = []
    for deal in fetched_deals:
        name_key = deal['Startup_Name'].lower().strip()
        if name_key not in existing_names:
            deal['Date_Captured'] = datetime.date.today().isoformat()
            new_unique_deals.append(deal)
            
    # Append truly new findings to history
    history['deals'].extend(new_unique_deals)
    history['last_updated'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    
    with open(file_path, 'w') as f:
        json.dump(history, f, indent=2)
            
    return new_unique_deals

# def send_email(new_deals, nation_info):
#     if not new_deals:
#         print(f"No new unique deals found for {nation_info['id']} to email today.")
#         return

#     today = datetime.date.today().isoformat()
#     csv_filename = f"Radar_{nation_info['id']}_{today}.csv"
    
#     msg = EmailMessage()
#     msg['Subject'] = f"{nation_info['flag']} {nation_info['label']} New AI/Data Funding – {today}"
#     msg['From'] = EMAIL_USER
#     msg['To'] = TARGET_EMAIL

#     msg.set_content(f"Found {len(new_deals)} NEW deals in {nation_info['label']}. See attached CSV.")
    
#     keys = new_deals[0].keys()
#     with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
#         writer = csv.DictWriter(f, fieldnames=keys)
#         writer.writeheader()
#         writer.writerows(new_deals)
        
#     with open(csv_filename, 'rb') as f:
#         msg.add_attachment(f.read(), maintype='application', subtype='csv', filename=csv_filename)

#     try:
#         with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
#             server.login(EMAIL_USER, EMAIL_PASS)
#             server.send_message(msg)
#         print(f"Email sent successfully for {nation_info['id']}.")
#     except Exception as e:
#         print(f"Failed to send email for {nation_info['id']}: {e}")
    
#     if os.path.exists(csv_filename): os.remove(csv_filename)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nation", required=True)
    parser.add_argument("--list", required=False, help="Specific list name (e.g., 'Tier 1)")
    parser.add_argument("--model", default="gemini-2.5-flash", help="Gemini model to use")
    parser.add_argument("--attempts", type=int, default=1, help="Number of retry attempts")
    args = parser.parse_args()
    
    info = load_nation_info(args.nation, args.list)
    if info:
        print(f"Running Radar for: {info['label']} with model {args.model} and {args.attempts} attempts")
        found_deals = fetch_new_deals_with_retries(info, num_attempts=args.attempts, model_name=args.model)
        unique_new_deals = process_historical_data(args.nation, found_deals)
        # send_email(unique_new_deals, info)

        if unique_new_deals:
            print(f"Found {len(unique_new_deals)} new deals for {info['label']}")
        else:
            print(f"No new deals found for {info['label']}")





            
