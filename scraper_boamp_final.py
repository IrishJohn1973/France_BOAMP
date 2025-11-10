import requests
import os
from datetime import datetime
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
import psycopg2
from psycopg2.extras import execute_values
import time
import re
import logging

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BOAMPScraper:
    def __init__(self):
        self.base_url = "https://www.boamp.fr/api/explore/v2.1/catalog/datasets/boamp-html/records"
        self.db_conn = self.connect_db()
        self.session = requests.Session()
        
    def connect_db(self):
        try:
            conn = psycopg2.connect(
                host='db.hjekfyirwzlybhnnzcjm.supabase.co',
                port=5432,
                database='postgres',
                user='postgres',
                password='Killorgin1973!'
            )
            logger.info("Database connected successfully")
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def fetch_tenders(self, limit=100, offset=0):
        params = {
            'limit': limit,
            'offset': offset,
            'order_by': 'idweb DESC',
            'where': 'html IS NOT NULL'
        }
        
        try:
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            results = data.get('results', [])
            total_count = data.get('total_count', 0)
            
            valid_results = [r for r in results if r.get('html') and len(r.get('html', '')) > 100]
            
            logger.info(f"Fetched {len(results)} tenders from offset {offset} (total available: {total_count})")
            if len(valid_results) < len(results):
                logger.warning(f"  Filtered out {len(results) - len(valid_results)} incomplete records")
            
            if valid_results:
                idwebs = [r.get('idweb') for r in valid_results[:5]]
                logger.info(f"  Sample idwebs: {idwebs}")
            
            return valid_results, total_count
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching tenders at offset {offset}: {e}")
            return [], 0
    
    def parse_tender(self, tender_data):
        html = tender_data.get('html', '')
        soup = BeautifulSoup(html, 'lxml-xml')
        
        if not soup.find():
            soup = BeautifulSoup(html, 'lxml')
        
        title_tag = soup.find('title')
        title = title_tag.text.strip() if title_tag else None
        
        notice_num = None
        annonce = soup.find(string=re.compile(r'Annonce n°', re.IGNORECASE))
        if annonce:
            strong = annonce.find_next('strong')
            if strong:
                notice_num = strong.text.strip()
        
        notice_type = None
        doc_titre = soup.find(id='doc_titre')
        if doc_titre:
            notice_type = doc_titre.text.strip()
        
        department = None
        dept = soup.find(string=re.compile(r'Département', re.IGNORECASE))
        if dept:
            dept_num = dept.find_next('strong')
            if dept_num:
                department = dept_num.text.strip()
        
        amounts = re.findall(r'(\d[\d\s,]*)\s*euro\(s\)\s*(?:HT|Ht|ht)', html, re.IGNORECASE)
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
        if not tenders:
            return 0
            
        cursor = self.db_conn.cursor()
        
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS france_boamp_parsed (
                    id SERIAL PRIMARY KEY,
                    idweb TEXT UNIQUE NOT NULL,
                    title TEXT,
                    notice_number TEXT,
                    notice_type TEXT,
                    department TEXT,
                    contract_amounts TEXT,
                    html_content TEXT,
                    scraped_at TIMESTAMP DEFAULT NOW(),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_france_boamp_idweb 
                ON france_boamp_parsed(idweb)
            """)
            
            values = [
                (
                    t['idweb'], 
                    t['title'], 
                    t['notice_number'], 
                    t['notice_type'], 
                    t['department'], 
                    t['contract_amounts'], 
                    t['html_content'], 
                    t['scraped_at']
                ) 
                for t in tenders
            ]
            
            logger.info(f"  Attempting to insert {len(values)} records...")
            logger.info(f"  First idweb: {values[0][0]}")
            
            execute_values(cursor, """
                INSERT INTO france_boamp_parsed 
                (idweb, title, notice_number, notice_type, department, 
                 contract_amounts, html_content, scraped_at)
                VALUES %s
                ON CONFLICT (idweb) DO NOTHING
            """, values)
            
            saved_count = cursor.rowcount
            self.db_conn.commit()
            
            logger.info(f"Saved {saved_count} new tenders (skipped {len(tenders) - saved_count} duplicates)")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            self.db_conn.rollback()
            return 0
    
    def run(self, total_records=1000, batch_size=100, max_consecutive_zeros=5):
        logger.info(f"Starting BOAMP scrape for up to {total_records} records...")
        
        offset = 0
        total_processed = 0
        total_saved = 0
        consecutive_zeros = 0
        
        while offset < total_records:
            raw_tenders, total_available = self.fetch_tenders(limit=batch_size, offset=offset)
            
            if not raw_tenders:
                logger.warning("No tenders returned, stopping")
                break
            
            parsed = []
            for tender in raw_tenders:
                try:
                    parsed_tender = self.parse_tender(tender)
                    parsed.append(parsed_tender)
                except Exception as e:
                    logger.error(f"Error parsing tender {tender.get('idweb')}: {e}")
                    continue
            
            saved_count = self.save_to_db(parsed)
            
            total_processed += len(parsed)
            total_saved += saved_count
            
            if saved_count == 0:
                consecutive_zeros += 1
                logger.warning(f"No new records saved ({consecutive_zeros}/{max_consecutive_zeros})")
                
                if consecutive_zeros >= max_consecutive_zeros:
                    logger.info(f"Stopping early: {max_consecutive_zeros} consecutive batches with 0 new records")
                    break
            else:
                consecutive_zeros = 0
            
            offset += batch_size
            
            if offset >= total_available:
                logger.info(f"Reached end of available records ({total_available})")
                break
            
            time.sleep(2)
        
        logger.info("="*60)
        logger.info(f"Scraping complete!")
        logger.info(f"Total processed: {total_processed}")
        logger.info(f"Total saved (new): {total_saved}")
        logger.info(f"Total duplicates: {total_processed - total_saved}")
        logger.info("="*60)
        
        self.cleanup()
    
    def cleanup(self):
        if self.db_conn:
            self.db_conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    scraper = BOAMPScraper()
    scraper.run(
        total_records=10000,
        batch_size=100,
        max_consecutive_zeros=5
    )
