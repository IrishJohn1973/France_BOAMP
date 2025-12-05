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
import json

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# FIX: Government entity detection to prevent buyer/winner confusion
# =============================================================================
GOVERNMENT_KEYWORDS = [
    'mairie', 'commune', 'ville', 'region', 'departement', 'prefecture',
    'ministere', 'conseil', 'hopital', 'centre hospitalier', 'universite',
    'lycee', 'college', 'ecole', 'syndicat', 'office public', 'sdis',
    'communaute', 'metropole', 'agglomeration', 'etablissement public',
    'direction regionale', 'direction departementale', 'rectorat',
    'academie', 'caisse', 'chambre de commerce', 'port autonome'
]

def is_government_entity(name):
    """Check if name is a government entity (should be buyer, not winner)"""
    if not name:
        return False
    name_lower = name.lower()
    for keyword in GOVERNMENT_KEYWORDS:
        if keyword in name_lower:
            return True
    return False


class BOAMPComprehensiveScraper:
    def __init__(self):
        self.base_url = "https://www.boamp.fr/api/explore/v2.1/catalog/datasets/boamp-html/records"
        self.db_conn = self.connect_db()
        self.session = requests.Session()

        # Initialize Claude API for award extraction
        self.use_claude_for_awards = bool(os.environ.get('ANTHROPIC_API_KEY'))
        if self.use_claude_for_awards:
            self.anthropic_api_key = os.environ.get('ANTHROPIC_API_KEY')
            logger.info("Claude API enabled for award extraction")
        else:
            logger.info("Claude API disabled (ANTHROPIC_API_KEY not set) - using regex fallback")

    def connect_db(self):
        try:
            conn = psycopg2.connect(
                host='db.hjekfyirwzlybhnnzcjm.supabase.co',
                port=5432,
                database='postgres',
                user='postgres',
                password=os.environ.get('SUPABASE_DB_PASSWORD', 'Killorgin1973!')
            )
            logger.info("Database connected successfully")
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def create_staging_table(self):
        """Create comprehensive staging table with all master schema fields"""
        cursor = self.db_conn.cursor()

        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS france_boamp_comprehensive (
                    id SERIAL PRIMARY KEY,
                    idweb TEXT UNIQUE NOT NULL,
                    source TEXT DEFAULT 'BOAMP',
                    source_id TEXT,
                    internal_ref TEXT,
                    notice_number TEXT,
                    notice_type TEXT,
                    title TEXT,
                    tender_title TEXT,
                    short_description TEXT,
                    full_description TEXT,
                    language TEXT DEFAULT 'fr',
                    buyer_name TEXT,
                    buyer_country TEXT DEFAULT 'FR',
                    buyer_city TEXT,
                    buyer_postcode TEXT,
                    buyer_address TEXT,
                    buyer_organization_type TEXT,
                    buyer_sector TEXT,
                    buyer_region TEXT,
                    buyer_siret TEXT,
                    contact_name TEXT,
                    contact_email TEXT,
                    contact_phone TEXT,
                    cpv_codes TEXT,
                    cpv_primary TEXT,
                    department TEXT,
                    published_at TIMESTAMP,
                    deadline TIMESTAMP,
                    contract_start_date TIMESTAMP,
                    contract_end_date TIMESTAMP,
                    estimated_value NUMERIC,
                    value_min NUMERIC,
                    value_max NUMERIC,
                    contract_amounts TEXT,
                    currency TEXT DEFAULT 'EUR',
                    contract_duration_months INTEGER,
                    contract_type TEXT,
                    procurement_method TEXT,
                    procedure_type TEXT,
                    lot_structure TEXT,
                    number_of_lots INTEGER,
                    has_lots BOOLEAN,
                    has_tranches BOOLEAN,
                    framework_agreement BOOLEAN,
                    allows_consortia BOOLEAN,
                    allows_variants BOOLEAN,
                    requires_site_visit BOOLEAN,
                    reserved_contract BOOLEAN,
                    execution_location TEXT,
                    execution_locations TEXT[],
                    detail_url TEXT,
                    external_portal_url TEXT,
                    winner_name TEXT,
                    winner_email TEXT,
                    winner_phone TEXT,
                    winner_city TEXT,
                    winner_postal_code TEXT,
                    winner_country TEXT,
                    winner_size TEXT,
                    additional_info TEXT,
                    html_content TEXT,
                    scraped_at TIMESTAMP DEFAULT NOW(),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_boamp_comp_idweb ON france_boamp_comprehensive(idweb)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_boamp_comp_deadline ON france_boamp_comprehensive(deadline)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_boamp_comp_cpv ON france_boamp_comprehensive(cpv_primary)")

            self.db_conn.commit()
            logger.info("Comprehensive staging table created/verified")

        except Exception as e:
            logger.error(f"Error creating staging table: {e}")
            self.db_conn.rollback()
            raise

    def fetch_recent_tenders(self, hours_back=24, limit=100, offset=0):
        """Fetch tenders from last N hours"""
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

            logger.info(f"Fetched {len(valid_results)} valid tenders from offset {offset} (total: {total_count})")

            return valid_results, total_count

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching tenders at offset {offset}: {e}")
            return [], 0

    def extract_field(self, soup, field_patterns, section=None):
        """Extract field using multiple pattern matching"""
        if section:
            section_div = soup.find(id=re.compile(section))
            if section_div:
                soup = section_div

        if isinstance(field_patterns, str):
            field_patterns = [field_patterns]

        for pattern in field_patterns:
            spans = soup.find_all('span', class_='fr-text--bold')
            for span in spans:
                if re.search(pattern, span.text, re.IGNORECASE):
                    parent = span.parent
                    if parent:
                        full_text = parent.get_text()
                        value = full_text.replace(span.text, '').strip()
                        if value and value != ':' and len(value) > 0:
                            return value

            field = soup.find(text=re.compile(pattern, re.IGNORECASE))
            if field:
                parent = field.parent
                if parent:
                    parent_div = parent.parent if parent.parent else parent
                    if parent_div:
                        full_text = parent_div.get_text()
                        parts = re.split(pattern, full_text, maxsplit=1, flags=re.IGNORECASE)
                        if len(parts) > 1:
                            value = parts[1].strip().lstrip(':').strip()
                            value = re.split(r'\n|<span', value)[0].strip()
                            if value and len(value) > 0:
                                return value

        return None

    def extract_resultat_section4(self, soup, html):
        """Extract winner info from Section 4 plain text format - WITH FIX"""
        winner_data = {}

        section_4_match = re.search(r'id="section_4"[^>]*>(.*?)</div>\s*<hr', html, re.DOTALL)
        if not section_4_match:
            return winner_data

        section_4_text = section_4_match.group(1)

        # Extract award date
        date_match = re.search(r"Date d'attribution\s*:\s*(\d{2}/\d{2}/\d{2})", section_4_text)
        if date_match:
            date_str = date_match.group(1)
            parts = date_str.split('/')
            date_str = f"{parts[0]}/{parts[1]}/20{parts[2]}"
            winner_data['award_date'] = self.parse_date(date_str)

        # Extract award value
        value_match = re.search(r'Montant\s+Ht\s*:\s*([\d\s,]+)', section_4_text, re.IGNORECASE)
        if value_match:
            winner_data['award_value'] = self.parse_amount(value_match.group(1))

        if not winner_data.get('award_value'):
            value_match2 = re.search(r'Montant[^:]*:\s*[^0-9]*?([\d\s]+)\s*EUR', section_4_text, re.IGNORECASE)
            if value_match2:
                winner_data['award_value'] = self.parse_amount(value_match2.group(1))

        # Extract winner - Pattern 1: line after "Marche n : XX.XXX"
        marche_match = re.search(r'March[^\n]*n[^\n]*:\s*[\d\.]+\s*\n\s*([^\n]+)', section_4_text)
        if marche_match:
            winner_line = marche_match.group(1).strip()
            winner_line = re.sub(r'<[^>]+>', '', winner_line)

            parts = [p.strip() for p in winner_line.split(',')]

            # FIX: Check each part - skip government entities
            for part in parts:
                if part and not is_government_entity(part):
                    winner_data['winner_name'] = part
                    break

            if len(parts) >= 2:
                winner_data['winner_address'] = parts[1]

            if len(parts) >= 3:
                last_part = parts[-1]
                postal_match = re.search(r'(\d{5})\s+(.+)', last_part)
                if postal_match:
                    winner_data['winner_postal_code'] = postal_match.group(1)
                    winner_data['winner_city'] = postal_match.group(2).strip()

        # Extract winner - Pattern 2: "Attribution a l'agence NAME"
        if not winner_data.get('winner_name'):
            attrib_match = re.search(r'Attribution\s+[^\n]*\s+([^<\n]+)', section_4_text, re.IGNORECASE)
            if attrib_match:
                winner_line = attrib_match.group(1).strip()
                if 'Montant' in winner_line:
                    winner_line = winner_line.split('Montant')[0].strip()

                parts = [p.strip() for p in winner_line.split(' - ')]

                if len(parts) >= 1:
                    name = re.sub(r"^(l'agence|la societe|le|la|l')\s+", '', parts[0], flags=re.IGNORECASE)
                    # FIX: Validate it's not a government entity
                    if name and not is_government_entity(name):
                        winner_data['winner_name'] = name

                if len(parts) >= 2:
                    winner_data['winner_address'] = parts[1]

                if len(parts) >= 3:
                    last_part = parts[-1]
                    postal_match = re.search(r'(\d{5})\s+(.+)', last_part)
                    if postal_match:
                        winner_data['winner_postal_code'] = postal_match.group(1)
                        winner_data['winner_city'] = postal_match.group(2).strip().rstrip('-').strip()

        # FIX: Final validation
        if winner_data.get('winner_name') and is_government_entity(winner_data['winner_name']):
            logger.warning(f"Rejected govt entity as winner: {winner_data['winner_name']}")
            winner_data['winner_name'] = None

        return winner_data

    def extract_award_with_claude(self, html_content, notice_id):
        """Use Claude API to extract award data - WITH FIX"""
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            text_content = soup.get_text(separator='\n', strip=True)

            if 'Section 4' in text_content:
                start_idx = text_content.find('Section 4')
                relevant_text = text_content[start_idx:start_idx+3000]
            else:
                relevant_text = text_content[:3000]

            # FIX: Updated prompt with buyer/winner distinction
            prompt = f"""Extract structured data from this French award notice. Return ONLY valid JSON, no other text.

CRITICAL: Distinguish between BUYER and WINNER:
- BUYER (Acheteur): Government entity awarding the contract (Mairie, Commune, Ville, Region, Departement, Prefecture, Ministere, Conseil, Hopital, Universite, Lycee, College, Syndicat, SDIS, etc.)
- WINNER (Titulaire/Attributaire): Private COMPANY that won the contract (SARL, SAS, SA, EURL, commercial businesses)

If you see "Mairie de X", "Commune de X", "Ville de X" - that is the BUYER, NOT the winner!

Required fields (use null if not found):
- winner_name: Company name that won (NOT a government entity)
- winner_address: Street address
- winner_city: City
- winner_postal_code: 5-digit code
- winner_country: Country (default "France")
- award_value: Numeric euros
- award_date: YYYY-MM-DD
- contract_number: Contract number

VALIDATION: If winner_name contains Mairie, Commune, Ville, Region, Departement, Prefecture, Ministere, Conseil, Hopital, Universite, you made an error - that's the buyer!

AWARD NOTICE:
{relevant_text}

JSON only:"""

            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": self.anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json={
                    "model": "claude-3-haiku-20240307",
                    "max_tokens": 1024,
                    "messages": [{"role": "user", "content": prompt}]
                },
                timeout=30
            )

            response.raise_for_status()
            result = response.json()
            content = result['content'][0]['text'].strip()
            content = re.sub(r'```json\n?|\n?```', '', content)

            award_data = json.loads(content)

            # FIX: Validate winner is not a government entity
            if award_data.get('winner_name') and is_government_entity(award_data['winner_name']):
                logger.warning(f"Claude returned govt as winner: {award_data['winner_name']} - clearing")
                award_data['winner_name'] = None

            return award_data

        except Exception as e:
            logger.warning(f"Claude extraction failed for {notice_id}: {e}")
            return {}

    def parse_date(self, date_str):
        """Parse French date format to datetime"""
        if not date_str:
            return None
        try:
            if '/' in date_str:
                parts = re.search(r'(\d{2})/(\d{2})/(\d{4})(?:\s+(\d{2}):(\d{2}))?', date_str)
                if parts:
                    day, month, year = parts.group(1), parts.group(2), parts.group(3)
                    hour = parts.group(4) if parts.group(4) else '00'
                    minute = parts.group(5) if parts.group(5) else '00'
                    return datetime.strptime(f"{day}/{month}/{year} {hour}:{minute}", "%d/%m/%Y %H:%M")
        except:
            pass
        return None

    def parse_amount(self, amount_str):
        """Parse amount string to float"""
        if not amount_str:
            return None
        clean = amount_str.replace(' ', '').replace('\xa0', '').replace(',', '.').replace('EUR', '').strip()
        try:
            return float(clean)
        except:
            return None

    def extract_cpv_codes(self, soup, html):
        """Extract all CPV codes"""
        cpv_codes = set()

        cpv_labels = soup.find_all(text=re.compile(r'Code.*CPV', re.IGNORECASE))
        for label in cpv_labels:
            parent = label.parent
            if parent:
                next_elem = parent.find_next(['span', 'div'])
                if next_elem:
                    match = re.search(r'\b([0-9]{8})\b', next_elem.text)
                    if match:
                        cpv_codes.add(match.group(1))

        for section_id in ['section_4', 'section_5']:
            section = soup.find(id=section_id)
            if section:
                section_text = section.parent.text if section.parent else ''
                digits = re.findall(r'\b([0-9]{8})\b', section_text)
                cpv_codes.update(digits)

        all_cpv = re.findall(r'CPV[^0-9]{0,100}([0-9]{8})', html, re.IGNORECASE)
        cpv_codes.update(all_cpv)

        return sorted(cpv_codes)

    def parse_tender(self, tender_data):
        """Extract ALL fields comprehensively"""
        html = tender_data.get('html', '')
        soup = BeautifulSoup(html, 'lxml')

        if not soup.find():
            soup = BeautifulSoup(html, 'html.parser')

        data = {
            'idweb': tender_data.get('idweb'),
            'source_id': tender_data.get('idweb'),
            'html_content': html,
            'scraped_at': datetime.now()
        }

        # Title
        title_tag = soup.find('title')
        data['title'] = title_tag.text.strip() if title_tag else None

        # Notice number
        for pattern in [r'Annonce n[^\s]*\s*[:]*\s*([A-Z0-9-]+)', r'Avis n[^\s]*\s*[:]*\s*([A-Z0-9-]+)']:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                data['notice_number'] = match.group(1).strip()
                break

        data['internal_ref'] = self.extract_field(soup, ['Identifiant interne', 'Reference'])

        doc_titre = soup.find(id='doc_titre')
        data['notice_type'] = doc_titre.text.strip() if doc_titre else None

        # Buyer info
        buyer_span = soup.find('span', class_='fr-text--bold', string=lambda t: 'Nom complet' in t if t else False)
        if buyer_span and buyer_span.parent:
            full_text = buyer_span.parent.get_text()
            value = full_text.replace(buyer_span.text, '').strip()
            data['buyer_name'] = value if value else None

        if not data.get('buyer_name'):
            data['buyer_name'] = self.extract_field(soup, "Nom complet de l'acheteur", section='section_1')

        data['buyer_city'] = self.extract_field(soup, 'Ville')
        data['buyer_postcode'] = self.extract_field(soup, 'Code postal')
        data['buyer_siret'] = self.extract_field(soup, "N National d'identification")
        data['buyer_organization_type'] = self.extract_field(soup, ['Forme juridique', 'Type de pouvoir'])
        data['buyer_sector'] = self.extract_field(soup, 'Activite du pouvoir adjudicateur')

        # Contact
        data['contact_name'] = self.extract_field(soup, 'Nom du contact')
        data['contact_email'] = self.extract_field(soup, 'Adresse mail du contact')
        data['contact_phone'] = self.extract_field(soup, 'Numero de telephone du contact')

        # Tender info
        data['tender_title'] = self.extract_field(soup, 'Intitule du marche', section='section_4')
        if not data['tender_title']:
            data['tender_title'] = data['title']

        data['full_description'] = self.extract_field(soup, ['Description', 'Objet'])
        data['short_description'] = data['full_description'][:500] if data.get('full_description') else None

        data['contract_type'] = self.extract_field(soup, 'Type de marche')
        data['procedure_type'] = self.extract_field(soup, 'Type de procedure')
        data['procurement_method'] = self.extract_field(soup, "Technique d'achat")

        # CPV codes
        cpv_list = self.extract_cpv_codes(soup, html)
        data['cpv_codes'] = ','.join(cpv_list) if cpv_list else None
        data['cpv_primary'] = cpv_list[0] if cpv_list else None

        # Dates
        deadline_str = self.extract_field(soup, 'Date et heure limite de reception des plis')
        data['deadline'] = self.parse_date(deadline_str)

        published_str = self.extract_field(soup, "Date d'envoi du present avis")
        data['published_at'] = self.parse_date(published_str)

        # Financial
        estimated_value = self.extract_field(soup, 'Valeur estimee')
        data['estimated_value'] = self.parse_amount(estimated_value)

        # Contract details
        duration_str = self.extract_field(soup, 'Duree du marche')
        if duration_str:
            match = re.search(r'(\d+)', duration_str)
            if match:
                data['contract_duration_months'] = int(match.group(1))

        # Lot structure
        has_lots_str = self.extract_field(soup, 'Marche alloti')
        data['has_lots'] = has_lots_str == 'Oui' if has_lots_str else None

        has_tranches_str = self.extract_field(soup, 'La consultation comporte des tranches')
        data['has_tranches'] = has_tranches_str == 'Oui' if has_tranches_str else None

        if data['has_lots']:
            lot_sections = soup.find_all(text=re.compile('Description du lot', re.IGNORECASE))
            data['number_of_lots'] = len(lot_sections) if lot_sections else None
            data['lot_structure'] = 'multiple' if data['number_of_lots'] and data['number_of_lots'] > 1 else 'single'

        # Location
        data['execution_location'] = self.extract_field(soup, "Lieu principal d'execution")

        # URLs
        portal_url = self.extract_field(soup, "Autre moyen d'acces")
        if portal_url and 'http' in portal_url:
            data['external_portal_url'] = portal_url

        data['detail_url'] = f"https://www.boamp.fr/avis/detail/{data['idweb']}"

        # Department
        dept = soup.find(text=re.compile(r'Departement', re.IGNORECASE))
        if dept:
            dept_num = dept.find_next('strong')
            data['department'] = dept_num.text.strip() if dept_num else None

        if not data['department'] and data.get('buyer_postcode'):
            data['department'] = data['buyer_postcode'][:2]

        data['additional_info'] = self.extract_field(soup, 'Autres informations complementaires')

        # AWARD INFORMATION (for attribution notices)
        if data.get('notice_type') and ('attribution' in data['notice_type'].lower() or 'resultat' in data['notice_type'].lower()):
            logger.info(f"   Extracting award data for {data['idweb']}")

            # Method 1: Claude API
            if self.use_claude_for_awards and not data.get('winner_name'):
                claude_data = self.extract_award_with_claude(html, data['idweb'])
                if claude_data and claude_data.get('winner_name'):
                    # FIX: Double-check it's not a government entity
                    if not is_government_entity(claude_data['winner_name']):
                        data['winner_name'] = claude_data.get('winner_name')
                        data['winner_city'] = claude_data.get('winner_city')
                        data['winner_postal_code'] = claude_data.get('winner_postal_code')
                        data['winner_country'] = claude_data.get('winner_country') or 'France'
                        data['winner_email'] = claude_data.get('winner_email')
                        data['winner_phone'] = claude_data.get('winner_phone')

                        if claude_data.get('award_value'):
                            data['estimated_value'] = float(claude_data['award_value'])

                        if claude_data.get('award_date'):
                            try:
                                data['contract_start_date'] = datetime.strptime(claude_data['award_date'], '%Y-%m-%d')
                            except:
                                pass

                        logger.info(f"   Winner (Claude): {data['winner_name']}")
                    else:
                        logger.warning(f"   Claude returned govt entity: {claude_data['winner_name']} - skipping")

            # Method 2: Regex fallback - Section 4
            if not data.get('winner_name'):
                resultat_data = self.extract_resultat_section4(soup, html)
                if resultat_data and resultat_data.get('winner_name'):
                    data['winner_name'] = resultat_data.get('winner_name')
                    data['winner_city'] = resultat_data.get('winner_city')
                    data['winner_postal_code'] = resultat_data.get('winner_postal_code')

                    if resultat_data.get('award_value'):
                        data['estimated_value'] = resultat_data.get('award_value')

                    if resultat_data.get('award_date'):
                        data['contract_start_date'] = resultat_data.get('award_date')

                    logger.info(f"   Winner (Regex): {data['winner_name']}")

        return data

    def save_to_db(self, tenders):
        """Save comprehensive tender data"""
        if not tenders:
            return 0

        cursor = self.db_conn.cursor()

        try:
            values = [
                (
                    t['idweb'], t.get('source_id'), t.get('internal_ref'), t.get('notice_number'),
                    t.get('notice_type'), t.get('title'), t.get('tender_title'), t.get('short_description'),
                    t.get('full_description'), t.get('buyer_name'), t.get('buyer_city'), t.get('buyer_postcode'),
                    t.get('buyer_siret'), t.get('buyer_organization_type'), t.get('buyer_sector'),
                    t.get('contact_name'), t.get('contact_email'), t.get('contact_phone'),
                    t.get('cpv_codes'), t.get('cpv_primary'), t.get('department'),
                    t.get('published_at'), t.get('deadline'), t.get('estimated_value'),
                    t.get('contract_amounts'), t.get('contract_duration_months'), t.get('contract_type'),
                    t.get('procurement_method'), t.get('procedure_type'), t.get('has_lots'),
                    t.get('number_of_lots'), t.get('lot_structure'), t.get('has_tranches'),
                    t.get('allows_consortia'), t.get('allows_variants'), t.get('requires_site_visit'),
                    t.get('reserved_contract'), t.get('execution_location'), t.get('detail_url'),
                    t.get('external_portal_url'), t.get('additional_info'), t.get('html_content'),
                    t.get('winner_name'), t.get('winner_city'), t.get('winner_postal_code'),
                    t.get('winner_country'), t.get('winner_email'), t.get('winner_phone'),
                    t.get('contract_start_date'), t.get('scraped_at')
                )
                for t in tenders
            ]

            execute_values(cursor, """
                INSERT INTO france_boamp_comprehensive
                (idweb, source_id, internal_ref, notice_number, notice_type, title, tender_title,
                 short_description, full_description, buyer_name, buyer_city, buyer_postcode,
                 buyer_siret, buyer_organization_type, buyer_sector, contact_name, contact_email,
                 contact_phone, cpv_codes, cpv_primary, department, published_at, deadline,
                 estimated_value, contract_amounts, contract_duration_months, contract_type,
                 procurement_method, procedure_type, has_lots, number_of_lots, lot_structure,
                 has_tranches, allows_consortia, allows_variants, requires_site_visit,
                 reserved_contract, execution_location, detail_url, external_portal_url,
                 additional_info, html_content, winner_name, winner_city, winner_postal_code,
                 winner_country, winner_email, winner_phone, contract_start_date, scraped_at)
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

    def run_daily(self, hours_back=24, max_records=1000, batch_size=100):
        """Run comprehensive daily scrape"""
        logger.info("="*70)
        logger.info(f"BOAMP Comprehensive Scraper - Last {hours_back} hours")
        logger.info("="*70)

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
                logger.info("No more tenders to process")
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

            offset += batch_size

            if offset >= total_available:
                logger.info(f"Processed all available records ({total_available})")
                break

            time.sleep(1)

        logger.info("="*70)
        logger.info(f"Comprehensive scrape complete!")
        logger.info(f"Total processed: {total_processed}")
        logger.info(f"New records saved: {total_saved}")
        logger.info("="*70)

        self.cleanup()

        return {
            'processed': total_processed,
            'saved': total_saved
        }

    def cleanup(self):
        if self.db_conn:
            self.db_conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    scraper = BOAMPComprehensiveScraper()
    scraper.run_daily(
        hours_back=24,
        max_records=1000,
        batch_size=100
    )
