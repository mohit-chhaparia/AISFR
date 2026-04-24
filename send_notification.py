import os
import json
import csv
import datetime
import smtplib
from email.message import EmailMessage

# --- CONFIGURATION ---
TARGET_EMAIL = os.getenv("GET_TARGET_EMAIL")
EMAIL_USER = os.getenv("EMAIL_SENDER")
EMAIL_PASS = os.getenv("EMAIL_PASSWORD")

def collect_yesterdays_deals():
    """Collect all deals captured yesterday from all nation data files."""
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    today = datetime.date.today().isoformat()
    
    all_deals = []
    data_dir = 'data'
    
    if not os.path.exists(data_dir):
        print("No data directory found.")
        return []
    
    for filename in os.listdir(data_dir):
        if filename.endswith('.json'):
            filepath = os.path.join(data_dir, filename)
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                    
                # Filter deals from yesterday
                for deal in data.get('deals', []):
                    capture_date = deal.get('Date_Captured', '')
                    if capture_date == yesterday or capture_date == today:
                        all_deals.append(deal)
                        
            except Exception as e:
                print(f"Error reading {filename}: {e}")
    
    return all_deals

def send_daily_digest(deals):
    """Send one consolidated email with all deals from yesterday."""
    if not deals:
        print("No new deals to email today.")
        return
    
    today = datetime.date.today().isoformat()
    csv_filename = f"Daily_VC_Radar_Digest_{today}.csv"
    
    msg = EmailMessage()
    msg['Subject'] = f"🌍 Daily VC Radar Digest - {len(deals)} New AI/Data Deals – {today}"
    msg['From'] = EMAIL_USER
    msg['To'] = TARGET_EMAIL
    
    # Group deals by tier for summary
    tier_summary = {}
    for deal in deals:
        tier = deal.get('Tier', 'Unknown')
        tier_summary[tier] = tier_summary.get(tier, 0) + 1
    
    summary_text = "\n".join([f"{tier}: {count} deals" for tier, count in sorted(tier_summary.items())])
    
    msg.set_content(f"""Daily VC Intelligence Report

Total Deals Found: {len(deals)}

Breakdown by Tier:
{summary_text}

See attached CSV for complete details.

---
Automated by VC Radar Intelligence System
Powered by Gemini 2.5 Flash

Automation Created by Mohit Chhaparia!""")
    
    # Create CSV with proper column order
    if deals:
        keys = ['Tier', 'Nation', 'Flag', 'Date_Captured'] + [k for k in deals[0].keys() 
                if k not in ['Tier', 'Nation', 'Flag', 'Date_Captured']]
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(deals)
        
        with open(csv_filename, 'rb') as f:
            msg.add_attachment(f.read(), maintype='application', subtype='csv', filename=csv_filename)
    
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)
        print(f"✅ Daily digest email sent successfully with {len(deals)} deals.")
    except Exception as e:
        print(f"❌ Failed to send daily digest: {e}")
    
    # Clean up CSV file
    if os.path.exists(csv_filename):
        os.remove(csv_filename)

if __name__ == "__main__":
    print("Collecting yesterday's deals...")
    deals = collect_yesterdays_deals()
    print(f"Found {len(deals)} deals to send.")
    send_daily_digest(deals)
