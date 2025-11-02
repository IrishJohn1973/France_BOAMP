import requests
import os
from datetime import datetime
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values
import time
import re

class BOAMPScraper:
    def __init__(self):
        self.base_url = "https://www.boamp.fr/api/explore/v2.1/catalog/datasets/boamp-html/records"
        self.db_conn = self.connect_db()
        
    def connect_db(self):
        """Connect to Supabase database"""
        return psycopg2.connect(
            host=os.getenv('DB_HOST', 'db.hjekfyirwzlybhnnzcjm.supabase.co'),
            port=os.getenv('DB_PORT', 5432),
            database=os.getenv('DB_NAME', 'postgres'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'Killorgin1973!')
        )
    
    def fetch_tenders(self, limit=100, offset=0):
        """Fetch tenders from BOAMP API"""
        params = {
            'limit': limit,
            'offset': offset,
            'order_by': 'idweb DESC'
        }
        
        response = requests.get(self.base_url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            print(f"Fetched {len(data['results'])} tenders (total: {data['total_count']})")
            return data['results']
        else:
            print(f"Error: {response.status_code}")
            return []
    
    def parse_tender(self, tender_data):
        """Parse HTML and extract tender fields"""
        html = tender_data.get('html', '')
        soup = BeautifulSoup(html, 'lxml')
        
        # Extract what we know works
        title_tag = soup.find('title')
        title = title_tag.text if title_tag else None
        
        # Notice number
        notice_num = None
        annonce = soup.find(string=re.compile('Annonce n°'))
        if annonce:
            strong = annonce.find_next('strong')
            if strong:
                notice_num = strong.text
        
        # Notice type
        notice_type = None
        doc_titre = soup.find(id='doc_titre')
        if doc_titre:
            notice_type = doc_titre.text.strip()
        
        # Department
        department = None
        dept = soup.find(string=re.compile('Département'))
        if dept:
            dept_num = dept.find_next('strong')
            if dept_num:
                department = dept_num.text
        
        # Contract amounts
        amounts = re.findall(r'(\d[\d\s,]*)\s*euro\(s\)\s*[HT|Ht]', html)
        contract_amounts = ','.join([a.replace(' ', '').replace(',', '') for a in amounts[:3]]) if amounts else None
        
        return {
            'idweb': tender_data.get('idweb'),
            'title': title,
            'notice_number': notice_num,
            'notice_type': notice_type,
            'department': department,
            'contract_amounts': contract_amounts,
            'html_content': html,
            'scraped_at': datetime.now()
        }
    
    def save_to_db(self, tenders):
        """Save parsed tenders to database"""
        if not tenders:
            return
            
        cursor = self.db_conn.cursor()
        
        # Create table with parsed fields
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS france_boamp_parsed (
                id SERIAL PRIMARY KEY,
                idweb TEXT UNIQUE,
                title TEXT,
                notice_number TEXT,
                notice_type TEXT,
                department TEXT,
                contract_amounts TEXT,
                html_content TEXT,
                scraped_at TIMESTAMP
            )
        """)
        
        # Insert tenders
        values = [(t['idweb'], t['title'], t['notice_number'], t['notice_type'], 
                   t['department'], t['contract_amounts'], t['html_content'], t['scraped_at']) 
                  for t in tenders]
        
        execute_values(cursor, """
            INSERT INTO france_boamp_parsed 
            (idweb, title, notice_number, notice_type, department, contract_amounts, html_content, scraped_at)
            VALUES %s
            ON CONFLICT (idweb) DO NOTHING
        """, values)
        
        self.db_conn.commit()
        print(f"Saved {cursor.rowcount} new tenders")
    
    def run(self, total_records=1000):
        """Main scraping loop"""
        print(f"Starting BOAMP scrape for {total_records} records...")
        
        batch_size = 100
        offset = 0
        total_saved = 0
        
        while offset < total_records:
            # Fetch batch
            raw_tenders = self.fetch_tenders(limit=batch_size, offset=offset)
            
            if not raw_tenders:
                break
            
            # Parse tenders
            parsed = [self.parse_tender(t) for t in raw_tenders]
            
            # Save to database
            self.save_to_db(parsed)
            
            total_saved += len(parsed)
            offset += batch_size
            
            # Be polite - wait 2 seconds between requests
            time.sleep(2)
            
        print(f"Scraping complete. Total processed: {total_saved}")
        self.db_conn.close()

if __name__ == "__main__":
    scraper = BOAMPScraper()
    scraper.run(total_records=1000)
