 import os
  import re
  import json
  import time
  import logging
  import hashlib
  from datetime import datetime, timedelta
  from typing import Optional, Dict, List, Tuple
  from dataclasses import dataclass
  from bs4 import BeautifulSoup
  import requests
  from anthropic import Anthropic
  from supabase import create_client, Client

  logging.basicConfig(level=logging.INFO, format='%(asctime)s - 
  %(levelname)s - %(message)s')
  logger = logging.getLogger(__name__)

  GOVERNMENT_ENTITY_PATTERNS = [
      r'^mairie\b', r'^commune\b', r'^ville\b', r'^communaute\b',
  r'^metropole\b',
      r'^agglomeration\b', r'^region\b', r'^departement\b',
      r'^conseil\s+(regional|departemental|general|municipal)',
      r'^ministere\b', r'^prefecture\b', r'^sous-prefecture\b',
      r'^direction\s+(regionale|departementale|generale)',
      r'^service\s+(departemental|regional)', r'^etablissement\s+public',
      r'^centre\s+hospitalier', r'^hopital\b', r'^chu\b', r'^chru\b',
      r'^centre\s+communal', r'^ccas\b', r'^cias\b', r'^universite\b',
      r'^lycee\b', r'^college\b', r'^ecole\b', r'^academie\b',
  r'^rectorat\b',
      r'^office\s+(public|hlm)', r'^opac\b', r'^oph\b', r'^sdis\b',
      r'^syndicat\b', r'^sivom\b', r'^sivu\b', r'^siaep\b', r'^caisse\b',
      r'^chambre\s+(de\s+commerce|des\s+metiers)',
  r'^port\s+(autonome|de)',
      r'^aeroport\b', r'^regie\b', r'^sem\b', r'^epl\b',
  ]

  GOVERNMENT_PATTERNS_COMPILED = [re.compile(p, re.IGNORECASE) for p in
  GOVERNMENT_ENTITY_PATTERNS]

  def is_government_entity(name):
      if not name:
          return False
      name_clean = name.strip().lower()
      for pattern in GOVERNMENT_PATTERNS_COMPILED:
          if pattern.search(name_clean):
              return True
      gov_keywords = ['mairie', 'commune', 'ville', 'region',
  'departement', 'prefecture',
                      'ministere', 'conseil', 'hopital', 'centre 
  hospitalier', 'universite',
                      'lycee', 'college', 'ecole', 'syndicat', 'office 
  public']
      for kw in gov_keywords:
          if kw in name_clean:
              return True
      return False

  def validate_and_swap_if_needed(buyer_name, winner_name):
      buyer_is_gov = is_government_entity(buyer_name) if buyer_name else
  False
      winner_is_gov = is_government_entity(winner_name) if winner_name
  else False
      if winner_is_gov and not buyer_name:
          logger.info(f"SWAP: Moving govt entity from winner to buyer: 
  {winner_name}")
          return winner_name, None
      if winner_is_gov and buyer_name and not buyer_is_gov:
          logger.info(f"SWAP: Winner is govt, buyer is company - 
  swapping")
          return winner_name, buyer_name
      if winner_is_gov and buyer_is_gov:
          logger.warning(f"BOTH are government entities")
          return buyer_name, None
      return buyer_name, winner_name

  @dataclass
  class Config:
      supabase_url: str
      supabase_key: str
      anthropic_key: str
      base_url: str = "https://www.boamp.fr"
      batch_size: int = 50
      request_delay: float = 1.0
      max_retries: int = 3

  def load_config():
      return Config(
          supabase_url=os.environ.get('SUPABASE_URL', ''),
          supabase_key=os.environ.get('SUPABASE_SERVICE_ROLE_KEY', ''),
          anthropic_key=os.environ.get('ANTHROPIC_API_KEY', ''),
      )

  @dataclass
  class AwardData:
      native_id: str
      source: str = "boamp_daily"
      title: Optional[str] = None
      buyer_name: Optional[str] = None
      buyer_address: Optional[str] = None
      buyer_city: Optional[str] = None
      winner_name: Optional[str] = None
      winner_address: Optional[str] = None
      winner_city: Optional[str] = None
      winner_country: Optional[str] = "FR"
      award_value: Optional[float] = None
      currency: str = "EUR"
      cpv_codes: Optional[List[str]] = None
      cpv_primary: Optional[str] = None
      published_at: Optional[datetime] = None
      detail_url: Optional[str] = None
      short_description: Optional[str] = None
      full_description: Optional[str] = None
      source_metadata: Optional[Dict] = None
      content_hash: Optional[str] = None

  class BOAMPDailyScraper:
      def __init__(self, config):
          self.config = config
          self.session = requests.Session()
          self.session.headers.update({
              'User-Agent': 'Mozilla/5.0 (compatible; TenderBridge/1.0)',
              'Accept': 'text/html,application/xhtml+xml',
              'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
          })
          self.supabase = create_client(config.supabase_url,
  config.supabase_key)
          self.anthropic = Anthropic(api_key=config.anthropic_key)
          self.stats = {'fetched': 0, 'parsed': 0, 'saved': 0, 'errors':
  0, 'skipped': 0, 'swapped': 0}

      def fetch_page(self, url):
          for attempt in range(self.config.max_retries):
              try:
                  response = self.session.get(url, timeout=30)
                  response.raise_for_status()
                  return response.text
              except requests.RequestException as e:
                  logger.warning(f"Attempt {attempt + 1} failed for {url}:
   {e}")
                  if attempt < self.config.max_retries - 1:
                      time.sleep(2 ** attempt)
          return None

      def get_search_url(self, date):
          date_str = date.strftime('%Y-%m-%d')
          return f"{self.config.base_url}/avis/liste?type=resultat&datePar
  ution={date_str}&page=1&sort=dateParution,desc"

      def extract_notice_links(self, html):
          soup = BeautifulSoup(html, 'html.parser')
          links = []
          for link in soup.select('a[href*="/avis/detail/"]'):
              href = link.get('href', '')
              if href and '/avis/detail/' in href:
                  full_url = href if href.startswith('http') else
  f"{self.config.base_url}{href}"
                  if full_url not in links:
                      links.append(full_url)
          return links

      def extract_native_id(self, url):
          match = re.search(r'/avis/detail/(\d+-\d+)', url)
          return match.group(1) if match else None

      def compute_content_hash(self, html):
          return hashlib.md5(html.encode('utf-8')).hexdigest()

      def extract_award_with_claude(self, html, url):
          soup = BeautifulSoup(html, 'html.parser')
          for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
              tag.decompose()
          text = soup.get_text(separator='\n', strip=True)[:15000]

          prompt = """Extract structured data from this French public 
  procurement award notice (BOAMP).

  CRITICAL DISTINCTION:
  - BUYER (Acheteur): The GOVERNMENT entity awarding the contract (Mairie,
   Commune, Ville, Region, Departement, Conseil, Ministere, Prefecture, 
  Centre Hospitalier, Universite, Lycee, College, Office Public, Syndicat,
   SDIS, etc.)
  - WINNER (Titulaire): The PRIVATE COMPANY that won (SARL, SAS, SA, EURL,
   commercial businesses)

  If you see Mairie de X or Commune de X - that is the BUYER not the 
  winner!

  Return ONLY valid JSON:
  {"buyer_name": "Government entity name", "buyer_address": "address", 
  "buyer_city": "city", "winner_name": "Company name (NOT government)", 
  "winner_address": "address", "winner_city": "city", "award_value": 
  number or null, "cpv_codes": ["codes"], "title": "title", 
  "short_description": "description"}

  Text:
  """ + text

          try:
              response = self.anthropic.messages.create(
                  model="claude-3-haiku-20240307",
                  max_tokens=1024,
                  messages=[{"role": "user", "content": prompt}]
              )
              result_text = response.content[0].text.strip()
              json_match = re.search(r'\{[\s\S]*\}', result_text)
              if json_match:
                  data = json.loads(json_match.group())
                  winner = data.get('winner_name')
                  buyer = data.get('buyer_name')
                  if winner and is_government_entity(winner):
                      logger.warning(f"Claude returned govt as winner: 
  {winner}")
                      new_buyer, new_winner =
  validate_and_swap_if_needed(buyer, winner)
                      data['buyer_name'] = new_buyer
                      data['winner_name'] = new_winner
                      self.stats['swapped'] += 1
                  return data
          except Exception as e:
              logger.error(f"Claude error: {e}")
          return None

      def extract_buyer_section1(self, soup):
          buyer_data = {'buyer_name': None, 'buyer_address': None,
  'buyer_city': None}
          section_1 =
  soup.find(string=re.compile(r'SECTION\s*1|IDENTIFICATION|ACHETEUR',
  re.I))
          if not section_1:
              return buyer_data
          container = section_1.find_parent(['div', 'section'])
          if not container:
              return buyer_data
          text = container.get_text(separator='\n', strip=True)
          for pattern in [r'Nom[:\s]*\n?\s*([^\n]+)',
  r'Pouvoir\s+adjudicateur[:\s]*\n?\s*([^\n]+)',
  r'Acheteur[:\s]*\n?\s*([^\n]+)']:
              match = re.search(pattern, text, re.I)
              if match:
                  buyer_data['buyer_name'] = match.group(1).strip()
                  break
          return buyer_data

      def extract_resultat_section4(self, soup, html):
          winner_data = {'winner_name': None, 'winner_address': None,
  'winner_city': None, 'award_value': None}
          section_4 =
  soup.find(string=re.compile(r'SECTION\s*4|RESULTAT|ATTRIBUTION', re.I))
          if not section_4:
              return winner_data
          container = section_4.find_parent(['div', 'section'])
          if not container:
              return winner_data
          text = container.get_text(separator='\n', strip=True)
          match =
  re.search(r'(?:Titulaire|Attributaire)[:\s]*\n?\s*([^\n]+)', text, re.I)
          if match:
              potential = match.group(1).strip()
              potential = re.sub(r'^(La societe|L\'entreprise|La SARL|La 
  SAS)\s+', '', potential, flags=re.I)
              if potential and not is_government_entity(potential):
                  winner_data['winner_name'] = potential
          for pattern in
  [r'Montant[:\s]*([\d\s]+[,\.]?\d*)\s*(?:EUR|euros?)',
  r'Valeur[:\s]*([\d\s]+[,\.]?\d*)\s*(?:EUR|euros?)']:
              match = re.search(pattern, text, re.I)
              if match:
                  val = match.group(1).replace(' ', '').replace(',', '.')
                  try:
                      winner_data['award_value'] = float(val)
                      break
                  except ValueError:
                      pass
          return winner_data

      def extract_cpv_codes(self, soup):
          text = soup.get_text()
          codes = []
          for code in re.findall(r'\b(\d{8}(?:-\d)?)\b', text):
              if code[:2] in ['03','09','14','15','16','18','19','22','24'
  ,'30','31','32','33','34','35','37','38','39','42','43','44','45','48','
  50','51','55','60','63','64','65','66','70','71','72','73','75','76','77
  ','79','80','85','90','92','98']:
                  if code not in codes:
                      codes.append(code)
          return codes[:10]

      def extract_title(self, soup):
          for sel in ['h1.notice-title', 'h1', '.titre-avis']:
              el = soup.select_one(sel)
              if el:
                  title = el.get_text(strip=True)
                  if title and len(title) > 10:
                      return title[:500]
          return None

      def parse_award_notice(self, html, url):
          soup = BeautifulSoup(html, 'html.parser')
          native_id = self.extract_native_id(url)
          if not native_id:
              return None
          claude_data = self.extract_award_with_claude(html, url)
          buyer_data = self.extract_buyer_section1(soup)
          winner_data = self.extract_resultat_section4(soup, html)
          cpv_codes = self.extract_cpv_codes(soup)
          title = self.extract_title(soup)
          final_buyer = (claude_data or {}).get('buyer_name') or
  buyer_data.get('buyer_name')
          final_winner = (claude_data or {}).get('winner_name') or
  winner_data.get('winner_name')
          final_buyer, final_winner =
  validate_and_swap_if_needed(final_buyer, final_winner)
          return AwardData(
              native_id=native_id,
              title=title or (claude_data or {}).get('title'),
              buyer_name=final_buyer,
              buyer_address=(claude_data or {}).get('buyer_address') or
  buyer_data.get('buyer_address'),
              buyer_city=(claude_data or {}).get('buyer_city') or
  buyer_data.get('buyer_city'),
              winner_name=final_winner,
              winner_address=(claude_data or {}).get('winner_address') or
  winner_data.get('winner_address'),
              winner_city=(claude_data or {}).get('winner_city') or
  winner_data.get('winner_city'),
              award_value=(claude_data or {}).get('award_value') or
  winner_data.get('award_value'),
              cpv_codes=cpv_codes or (claude_data or {}).get('cpv_codes'),
              cpv_primary=cpv_codes[0] if cpv_codes else None,
              published_at=datetime.now(),
              detail_url=url,
              short_description=(claude_data or
  {}).get('short_description'),
              content_hash=self.compute_content_hash(html),
          )

      def save_award(self, award):
          try:
              data = {
                  'native_id': award.native_id, 'source': award.source,
  'title': award.title,
                  'buyer_name': award.buyer_name, 'buyer_address':
  award.buyer_address,
                  'buyer_city': award.buyer_city, 'winner_name':
  award.winner_name,
                  'winner_address': award.winner_address, 'winner_city':
  award.winner_city,
                  'winner_country': award.winner_country, 'award_value':
  award.award_value,
                  'currency': award.currency, 'cpv_codes':
  award.cpv_codes,
                  'cpv_primary': award.cpv_primary,
                  'published_at': award.published_at.isoformat() if
  award.published_at else None,
                  'detail_url': award.detail_url, 'short_description':
  award.short_description,
                  'content_hash': award.content_hash,
              }

  self.supabase.table('france_boamp_daily_normalized').upsert(data,
  on_conflict='native_id').execute()
              return True
          except Exception as e:
              logger.error(f"Save failed: {e}")
              return False

      def scrape_date(self, date):
          logger.info(f"Scraping {date.strftime('%Y-%m-%d')}")
          html = self.fetch_page(self.get_search_url(date))
          if not html:
              return 0
          links = self.extract_notice_links(html)
          logger.info(f"Found {len(links)} notices")
          saved = 0
          for i, url in enumerate(links):
              try:
                  time.sleep(self.config.request_delay)
                  notice_html = self.fetch_page(url)
                  if not notice_html:
                      self.stats['errors'] += 1
                      continue
                  self.stats['fetched'] += 1
                  award = self.parse_award_notice(notice_html, url)
                  if not award:
                      self.stats['errors'] += 1
                      continue
                  self.stats['parsed'] += 1
                  if not award.winner_name and not award.buyer_name:
                      self.stats['skipped'] += 1
                      continue
                  if self.save_award(award):
                      saved += 1
                      self.stats['saved'] += 1
                      logger.info(f"[{i+1}/{len(links)}] {award.native_id}
   Buyer:{award.buyer_name} Winner:{award.winner_name}")
              except Exception as e:
                  logger.error(f"Error: {e}")
                  self.stats['errors'] += 1
          return saved

      def scrape_date_range(self, start_date, end_date):
          current = start_date
          total = 0
          while current <= end_date:
              total += self.scrape_date(current)
              current += timedelta(days=1)
          return {'total_saved': total, 'stats': self.stats}

      def print_stats(self):
          logger.info("=" * 50)
          logger.info(f"Fetched: {self.stats['fetched']} | Parsed: 
  {self.stats['parsed']} | Saved: {self.stats['saved']}")
          logger.info(f"Swapped: {self.stats['swapped']} | Skipped: 
  {self.stats['skipped']} | Errors: {self.stats['errors']}")
          logger.info("=" * 50)

  def main():
      config = load_config()
      if not config.supabase_url or not config.supabase_key:
          logger.error("Missing SUPABASE_URL or 
  SUPABASE_SERVICE_ROLE_KEY")
          return
      if not config.anthropic_key:
          logger.error("Missing ANTHROPIC_API_KEY")
          return
      scraper = BOAMPDailyScraper(config)
      end_date = datetime.now()
      start_date = end_date - timedelta(days=7)
      logger.info(f"Starting BOAMP scraper: {start_date.date()} to 
  {end_date.date()}")
      scraper.scrape_date_range(start_date, end_date)
      scraper.print_stats()

  if __name__ == '__main__':
      main()
