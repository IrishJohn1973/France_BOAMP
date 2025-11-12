import requests
import os
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import warnings
import psycopg2
from psycopg2.extras import execute_values
import time
import re
import logging

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BOAMPDailyScraper:
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
            logger.info("✅ Database connected successfully")
            return conn
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def create_staging_table(self):
        """Create the france_boamp_daily_staging table with all fields"""
        cursor = self.db_conn.cursor()
        
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS france_boamp_daily_staging (
                    id SERIAL PRIMARY KEY,
                    idweb TEXT UNIQUE NOT NULL,
                    
                    -- Basic info
                    title TEXT,
                    notice_number TEXT,
                    notice_type TEXT,
                    
                    -- Buyer information
                    buyer_name TEXT,
                    buyer_type TEXT,
                    buyer_activity TEXT,
                    
                    -- Tender information
                    tender_title TEXT,
                    description TEXT,
                    procedure_id TEXT,
                    internal_id TEXT,
                    procedure_type TEXT,
                    
                    -- Classification
                    cpv_codes TEXT,
                    department TEXT,
                    
                    -- Financial
                    estimated_value NUMERIC,
                    contract_amounts TEXT,
                    
                    -- Winner information
                    winner_name TEXT,
                    winner_email TEXT,
                    winner_phone TEXT,
                    winner_city TEXT,
                    winner_postal_code TEXT,
                    winner_country TEXT,
                    winner_size TEXT,
                    
                    -- Dates
                    published_date TEXT,
                    
                    -- Raw data
                    html_content TEXT,
                    
                    -- Metadata
                    scraped_at TIMESTAMP DEFAULT NOW(),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_france_boamp_daily_idweb 
                ON france_boamp_daily_staging(idweb)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_france_boamp_daily_scraped_at 
                ON france_boamp_daily_staging(scraped_at)
            """)
            
            self.db_conn.commit()
            logger.info("✅ Staging table created/verified")
            
        except Exception as e:
            logger.error(f"❌ Error creating staging table: {e}")
            self.db_conn.rollback()
            raise
    
    def fetch_recent_tenders(self, hours_back=24, limit=100, offset=0):
        """Fetch tenders from last N hours"""
        cutoff_date = (datetime.now() - timedelta(hours=hours_back)).strftime('%Y-%m-%d')
        
        params = {
            'limit': limit,
            'offset': offset,
            'order_by': 'idweb DESC',
            'where': f"html IS NOT NULL AND dateparution >= '{cutoff_date}'"
        }
        
        try:
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            results = data.get('results', [])
            total_count = data.get('total_count', 0)
            
            valid_results = [r for r in results if r.get('html') and len(r.get('html', '')) > 100]
            
            logger.info(f"📥 Fetched {len(valid_results)} valid tenders from offset {offset} (total: {total_count})")
            
            return valid_results, total_count
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching tenders at offset {offset}: {e}")
            return [], 0
    
    def extract_field(self, soup, field_name, section=None):
        """Extract a field value by its label - aggressive multi-method approach"""
        if section:
            section_div = soup.find(id=re.compile(section))
            if section_div:
                soup = section_div
        
        # Method 1: Find by exact text match
        field = soup.find(text=re.compile(field_name, re.IGNORECASE))
        if field:
            # Try multiple extraction strategies
            parent = field.parent
            
            # Strategy A: Next sibling span/div
            next_elem = field.find_next(['span', 'div', 'p', 'td'])
            if next_elem and next_elem.text.strip() and next_elem.text.strip() not in [':', '', field_name]:
                text = next_elem.text.strip()
                if len(text) > 0 and text != field_name:
                    return text
            
            # Strategy B: Parent's next sibling
            if parent:
                next_sib = parent.find_next_sibling()
                if next_sib and next_sib.text.strip():
                    text = next_sib.text.strip()
                    if len(text) > 0 and text != field_name:
                        return text
            
            # Strategy C: Within same parent, find span/strong after label
            if parent:
                for elem in parent.find_all(['span', 'strong', 'div']):
                    text = elem.text.strip()
                    if text and text != field_name and text != ':' and len(text) > 1:
                        return text
        
        # Method 2: Find by label tag
        label = soup.find('label', text=re.compile(field_name, re.IGNORECASE))
        if label:
            # Find associated input or span
            for_id = label.get('for')
            if for_id:
                target = soup.find(id=for_id)
                if target and target.text.strip():
                    return target.text.strip()
        
        return None
    
    def extract_winner_info(self, soup):
        """Extract winner/contractor information - comprehensive approach"""
        winner_data = {}
        
        # Strategy 1: Find by "Lauréat" text
        laureat = soup.find(text=re.compile('Lauréat de ces lots|Lauréat|Titulaire', re.IGNORECASE))
        if laureat:
            org_section = laureat.find_parent('div', class_='section')
            if org_section:
                parent = org_section.find_parent('div', class_='section')
                if parent:
                    winner_data['winner_name'] = self.extract_field(parent, 'Nom officiel|Nom')
                    winner_data['winner_email'] = self.extract_field(parent, 'Adresse électronique|Courriel|Email')
                    winner_data['winner_phone'] = self.extract_field(parent, 'Téléphone|Tél')
                    winner_data['winner_city'] = self.extract_field(parent, 'Ville')
                    winner_data['winner_postal_code'] = self.extract_field(parent, 'Code postal')
                    winner_data['winner_country'] = self.extract_field(parent, 'Pays')
                    winner_data['winner_size'] = self.extract_field(parent, "Taille de l'opérateur|Taille")
        
        # Strategy 2: Find in Section 8 (Organizations)
        if not winner_data.get('winner_name'):
            section_8 = soup.find(id=re.compile('section_8'))
            if section_8:
                winner_data['winner_name'] = self.extract_field(section_8, 'Nom officiel|Nom')
                winner_data['winner_email'] = self.extract_field(section_8, 'Adresse électronique|Courriel|Email')
                winner_data['winner_phone'] = self.extract_field(section_8, 'Téléphone|Tél')
                winner_data['winner_city'] = self.extract_field(section_8, 'Ville')
                winner_data['winner_postal_code'] = self.extract_field(section_8, 'Code postal')
                winner_data['winner_country'] = self.extract_field(section_8, 'Pays')
        
        # Strategy 3: Look for "Contractant" or "Attributaire"
        if not winner_data.get('winner_name'):
            contractant = soup.find(text=re.compile('Contractant|Attributaire', re.IGNORECASE))
            if contractant:
                section = contractant.find_parent('div', class_='section')
                if section:
                    winner_data['winner_name'] = self.extract_field(section, 'Nom officiel|Nom')
                    winner_data['winner_city'] = self.extract_field(section, 'Ville')
        
        return winner_data
    
    def parse_amount(self, amount_str):
        """Parse amount string to float"""
        if not amount_str:
            return None
        clean = amount_str.replace(' ', '').replace(',', '').replace('€', '')
        try:
            return float(clean)
        except:
            return None
    
    def parse_tender(self, tender_data):
        """Extract ALL fields from BOAMP tender - aggressive extraction for 90%+ population"""
        html = tender_data.get('html', '')
        soup = BeautifulSoup(html, 'lxml')
        
        # If lxml fails, try html.parser
        if not soup.find():
            soup = BeautifulSoup(html, 'html.parser')
        
        data = {
            'idweb': tender_data.get('idweb'),
            'html_content': html,
            'scraped_at': datetime.now()
        }
        
        # ==================== BASIC INFO ====================
        # Title - multiple strategies
        title = None
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.text.strip()
        if not title:
            h1 = soup.find('h1')
            if h1:
                title = h1.text.strip()
        if not title:
            # Look for "Intitulé" in the document
            intitule = self.extract_field(soup, 'Intitulé')
            if intitule:
                title = intitule
        data['title'] = title
        
        # Notice number - multiple patterns
        notice_num = None
        patterns = [
            r'Annonce n[°o]?\s*[:：]?\s*([A-Z0-9-]+)',
            r'Avis n[°o]?\s*[:：]?\s*([A-Z0-9-]+)',
            r'Notice\s*[:：]?\s*([A-Z0-9-]+)'
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                notice_num = match.group(1).strip()
                break
        if not notice_num:
            annonce = soup.find(text=re.compile(r'Annonce n[°o]?', re.IGNORECASE))
            if annonce:
                strong = annonce.find_next('strong')
                if strong:
                    notice_num = strong.text.strip()
        data['notice_number'] = notice_num
        
        # Notice type
        notice_type = None
        doc_titre = soup.find(id='doc_titre')
        if doc_titre:
            notice_type = doc_titre.text.strip()
        if not notice_type:
            notice_type = self.extract_field(soup, "Type d'avis|Type avis")
        data['notice_type'] = notice_type
        
        # ==================== BUYER INFORMATION ====================
        # Try section 1 first, then general search
        buyer_name = self.extract_field(soup, 'Nom officiel', section='section_1')
        if not buyer_name:
            buyer_name = self.extract_field(soup, "Nom et adresse de l'acheteur|Acheteur|Organisme")
        if not buyer_name:
            # Look for organization name in strong tags near "acheteur"
            acheteur = soup.find(text=re.compile('acheteur|organisme', re.IGNORECASE))
            if acheteur:
                strong = acheteur.find_next('strong')
                if strong and len(strong.text.strip()) > 3:
                    buyer_name = strong.text.strip()
        data['buyer_name'] = buyer_name
        
        data['buyer_type'] = self.extract_field(soup, 'Forme juridique|Type de pouvoir|Statut juridique')
        data['buyer_activity'] = self.extract_field(soup, 'Activité du pouvoir adjudicateur|Activité principale|Secteur')
        
        # ==================== TENDER INFORMATION ====================
        tender_title = self.extract_field(soup, 'Titre|Intitulé', section='section_2')
        if not tender_title:
            tender_title = self.extract_field(soup, "Objet du marché|Description sommaire")
        # If still empty, use main title
        if not tender_title and data.get('title'):
            tender_title = data['title']
        data['tender_title'] = tender_title
        
        # Description - try multiple fields
        description = self.extract_field(soup, 'Description', section='section_2')
        if not description:
            description = self.extract_field(soup, 'Objet|Description courte|Description détaillée')
        if not description:
            # Look for longest paragraph in section 2
            section_2 = soup.find(id=re.compile('section_2'))
            if section_2:
                paragraphs = section_2.find_all('p')
                if paragraphs:
                    longest = max(paragraphs, key=lambda p: len(p.text))
                    if len(longest.text.strip()) > 50:
                        description = longest.text.strip()
        data['description'] = description
        
        data['procedure_id'] = self.extract_field(soup, 'Identifiant de la procédure|Référence de la procédure|Numéro de marché')
        data['internal_id'] = self.extract_field(soup, 'Identifiant interne|Référence interne')
        data['procedure_type'] = self.extract_field(soup, 'Type de procédure|Procédure')
        
        # ==================== CPV CODES ====================
        cpv_codes = set()
        
        # Method 1: Find all elements with text 'cpv'
        cpv_elements = soup.find_all(text=re.compile('cpv', re.IGNORECASE))
        for cpv in cpv_elements:
            # Look for numeric codes nearby
            parent = cpv.parent
            if parent:
                for elem in parent.find_all(['span', 'strong', 'code']):
                    text = elem.text.strip().replace('-', '').replace(' ', '')
                    if text.isdigit() and len(text) == 8:
                        cpv_codes.add(elem.text.strip())
        
        # Method 2: Regex search for CPV patterns
        cpv_pattern = r'(?:cpv|CPV)[:\s]*([0-9]{8}(?:-[0-9])?)'
        cpv_matches = re.findall(cpv_pattern, html, re.IGNORECASE)
        cpv_codes.update(cpv_matches)
        
        # Method 3: Look for 8-digit numbers near "Classification"
        classification = soup.find(text=re.compile('Classification', re.IGNORECASE))
        if classification:
            section = classification.find_parent('div')
            if section:
                digits = re.findall(r'\b([0-9]{8})\b', section.text)
                cpv_codes.update(digits)
        
        data['cpv_codes'] = ','.join(sorted(cpv_codes)) if cpv_codes else None
        
        # ==================== FINANCIAL ====================
        # Estimated value
        estimated_value = self.extract_field(soup, 'Valeur estimée hors TVA|Valeur estimée|Montant estimé')
        if not estimated_value:
            # Regex search
            value_match = re.search(r'Valeur[^:]*:\s*([\d\s,.]+)\s*(?:€|EUR|euro)', html, re.IGNORECASE)
            if value_match:
                estimated_value = value_match.group(1)
        data['estimated_value'] = self.parse_amount(estimated_value) if estimated_value else None
        
        # Contract amounts - comprehensive extraction
        amounts = []
        # Pattern 1: X euro(s) HT
        amounts.extend(re.findall(r'(\d[\d\s,]*)\s*euro\(s\)\s*(?:HT|Ht|ht)', html, re.IGNORECASE))
        # Pattern 2: X EUR
        amounts.extend(re.findall(r'(\d[\d\s,]+)\s*(?:EUR|€)', html))
        # Pattern 3: Montant: X
        amounts.extend(re.findall(r'[Mm]ontant[^:]*:\s*([\d\s,]+)', html))
        
        # Clean and deduplicate
        clean_amounts = []
        for amt in amounts:
            cleaned = amt.replace(' ', '').replace(',', '')
            if cleaned.isdigit() and int(cleaned) > 100:  # Minimum threshold
                clean_amounts.append(cleaned)
        
        data['contract_amounts'] = ','.join(clean_amounts[:5]) if clean_amounts else None
        
        # ==================== WINNER INFORMATION ====================
        winner = self.extract_winner_info(soup)
        data.update(winner)
        
        # ==================== DATES ====================
        published_date = self.extract_field(soup, "Date d'envoi de l'avis|Date de publication|Date envoi")
        if not published_date:
            # Regex for date patterns
            date_match = re.search(r'\b(\d{2}/\d{2}/\d{4})\b', html)
            if date_match:
                published_date = date_match.group(1)
        data['published_date'] = published_date
        
        # ==================== LOCATION ====================
        # Department - multiple strategies
        department = None
        dept = soup.find(text=re.compile(r'Département', re.IGNORECASE))
        if dept:
            dept_num = dept.find_next('strong')
            if dept_num:
                department = dept_num.text.strip()
        if not department:
            # Look for 2-digit department codes
            dept_match = re.search(r'Département[^:]*:\s*(\d{2,3})', html, re.IGNORECASE)
            if dept_match:
                department = dept_match.group(1)
        if not department:
            # Extract from postal code if available
            postal = self.extract_field(soup, 'Code postal')
            if postal and len(postal) >= 2:
                department = postal[:2]
        data['department'] = department
        
        return data
    
    def save_to_db(self, tenders):
        """Save parsed tenders to staging table with population stats"""
        if not tenders:
            return 0
            
        cursor = self.db_conn.cursor()
        
        # Calculate field population statistics
        field_stats = {}
        critical_fields = [
            'title', 'notice_number', 'notice_type', 'buyer_name', 
            'tender_title', 'description', 'cpv_codes', 'department'
        ]
        
        for field in critical_fields:
            populated = sum(1 for t in tenders if t.get(field))
            field_stats[field] = (populated / len(tenders)) * 100
        
        avg_population = sum(field_stats.values()) / len(field_stats)
        
        try:
            values = [
                (
                    t['idweb'],
                    t.get('title'),
                    t.get('notice_number'),
                    t.get('notice_type'),
                    t.get('buyer_name'),
                    t.get('buyer_type'),
                    t.get('buyer_activity'),
                    t.get('tender_title'),
                    t.get('description'),
                    t.get('procedure_id'),
                    t.get('internal_id'),
                    t.get('procedure_type'),
                    t.get('cpv_codes'),
                    t.get('department'),
                    t.get('estimated_value'),
                    t.get('contract_amounts'),
                    t.get('winner_name'),
                    t.get('winner_email'),
                    t.get('winner_phone'),
                    t.get('winner_city'),
                    t.get('winner_postal_code'),
                    t.get('winner_country'),
                    t.get('winner_size'),
                    t.get('published_date'),
                    t.get('html_content'),
                    t.get('scraped_at')
                ) 
                for t in tenders
            ]
            
            execute_values(cursor, """
                INSERT INTO france_boamp_daily_staging 
                (idweb, title, notice_number, notice_type, buyer_name, buyer_type, 
                 buyer_activity, tender_title, description, procedure_id, internal_id, 
                 procedure_type, cpv_codes, department, estimated_value, contract_amounts,
                 winner_name, winner_email, winner_phone, winner_city, winner_postal_code,
                 winner_country, winner_size, published_date, html_content, scraped_at)
                VALUES %s
                ON CONFLICT (idweb) DO NOTHING
            """, values)
            
            saved_count = cursor.rowcount
            self.db_conn.commit()
            
            logger.info(f"💾 Saved {saved_count} new tenders (skipped {len(tenders) - saved_count} duplicates)")
            logger.info(f"📊 Field Population Rate: {avg_population:.1f}%")
            
            # Log any fields below 80%
            low_fields = [f for f, rate in field_stats.items() if rate < 80]
            if low_fields:
                logger.warning(f"⚠️  Fields below 80%: {', '.join(low_fields)}")
            
            # Log detailed stats
            for field, rate in sorted(field_stats.items(), key=lambda x: x[1]):
                logger.info(f"   {field}: {rate:.1f}%")
            
            return saved_count
            
        except Exception as e:
            logger.error(f"❌ Error saving to database: {e}")
            self.db_conn.rollback()
            return 0
    
    def run_daily(self, hours_back=24, max_records=1000, batch_size=100):
        """Run daily scrape for recent tenders"""
        logger.info("="*70)
        logger.info(f"🇫🇷 Starting BOAMP Daily Scraper - Last {hours_back} hours")
        logger.info("="*70)
        
        # Create/verify table
        self.create_staging_table()
        
        offset = 0
        total_processed = 0
        total_saved = 0
        
        while offset < max_records:
            raw_tenders, total_available = self.fetch_recent_tenders(
                hours_back=hours_back,
                limit=batch_size,
                offset=offset
            )
            
            if not raw_tenders:
                logger.info("✅ No more tenders to process")
                break
            
            # Parse all tenders
            parsed = []
            for tender in raw_tenders:
                try:
                    parsed_tender = self.parse_tender(tender)
                    parsed.append(parsed_tender)
                except Exception as e:
                    logger.error(f"❌ Error parsing tender {tender.get('idweb')}: {e}")
                    continue
            
            # Save batch
            saved_count = self.save_to_db(parsed)
            
            total_processed += len(parsed)
            total_saved += saved_count
            
            offset += batch_size
            
            if offset >= total_available:
                logger.info(f"✅ Processed all available records ({total_available})")
                break
            
            # Rate limiting
            time.sleep(1)
        
        logger.info("="*70)
        logger.info(f"🎉 Daily scrape complete!")
        logger.info(f"📊 Total processed: {total_processed}")
        logger.info(f"💾 New records saved: {total_saved}")
        logger.info(f"🔄 Duplicates skipped: {total_processed - total_saved}")
        logger.info("="*70)
        
        self.cleanup()
        
        return {
            'processed': total_processed,
            'saved': total_saved,
            'duplicates': total_processed - total_saved
        }
    
    def cleanup(self):
        if self.db_conn:
            self.db_conn.close()
            logger.info("🔒 Database connection closed")

if __name__ == "__main__":
    scraper = BOAMPDailyScraper()
    scraper.run_daily(
        hours_back=24,  # Last 24 hours
        max_records=1000,  # Safety limit
        batch_size=100
    )
