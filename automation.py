import os
import json
import csv
import datetime
import argparse
import smtplib
from email.message import EmailMessage
from google import genai
from google.genai import types
from automation_v2_helpers import (
    build_v2_prompt,
    dedupe_deals_by_deal_signature,
    normalize_row_for_legacy_compat,
    safe_extract_json_array,
)

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
    """Run exactly one Gemini call per nation and return normalized deals."""
    if num_attempts != 1:
        print(
            f"Requested attempts={num_attempts} for {nation_info['label']}; "
            "forcing single Gemini call for deterministic cost control."
        )
    print(f"Running single attempt for {nation_info['label']}")
    deals = fetch_new_deals(nation_info, model_name)
    enriched = []
    for deal in deals:
        if not isinstance(deal, dict):
            continue
        normalized = normalize_row_for_legacy_compat(deal)
        normalized['Tier'] = nation_info['tier_number']
        normalized['Nation'] = nation_info['label']
        normalized['Flag'] = nation_info['flag']
        enriched.append(normalized)
    unique_deals = dedupe_deals_by_deal_signature(enriched)
    print(f"Total unique deals found: {len(unique_deals)}")
    return unique_deals

def fetch_new_deals(nation_info, model_name="gemini-2.5-flash"):
    """Calls Gemini with configurable model. Note: JSON mode is disabled to allow Tool use."""
    today = datetime.date.today().isoformat()
    week_ago = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
    prompt = build_v2_prompt(
        nation_label=nation_info["label"],
        nation_sources=nation_info["sources"],
        prompt_extra=nation_info.get("prompt_extra", ""),
        week_ago=week_ago,
        today=today,
    )

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
        
        rows = safe_extract_json_array(response.text or "")
        if rows:
            return [normalize_row_for_legacy_compat(row) for row in rows]

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





            
