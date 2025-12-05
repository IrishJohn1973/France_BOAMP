 BOAMP Daily Award Scraper - FIXED VERSION
  ==========================================
  Fix: Correctly distinguishes government BUYERS from company WINNERS
  ...
  """

  import os
  import re

> GIVE ME THE FULL WORKING FIT 

⏺ """
  BOAMP Daily Award Scraper - FIXED VERSION
  ==========================================
  Fix: Correctly distinguishes government BUYERS from company WINNERS
  """

  import os
  import re
  import json
  import time
  import logging
  import hashlib
  from datetime import datetime, timedelta
  from typing import Optional, Dict, Any, List, Tuple
  from dataclasses import dataclass
  from bs4 import BeautifulSoup
  import requests
  from anthropic import Anthropic
  from supabase import create_client, Client

  logging.basicConfig(
      level=logging.INFO,
      format='%(asctime)s - %(levelname)s - %(message)s'
  )
  logger = logging.getLogger(__name__)

  GOVERNMENT_ENTITY_PATTERNS = [
      r'^mairie\b',
      r'^commune\b',
      r'^ville\b',
      r'^communauté\b',
      r'^métropole\b',
      r'^agglomération\b',
      r'^région\b',
      r'^département\b',
      r'^conseil\s+(régional|départemental|général|municipal)',
      r'^ministère\b',
      r'^préfecture\b',
      r'^sous-préfecture\b',
      r'^direction\s+(régionale|départementale|générale)',
      r'^service\s+(départemental|régional)',
      r'^établissement\s+public',
      r'^centre\s+hospitalier',
      r'^hôpital\b',
      r'^chu\b',
      r'^chru\b',
      r'^centre\s+communal',
      r'^ccas\b',
      r'^cias\b',
      r'^université\b',
      r'^lycée\b',
      r'^collège\b',
      r'^école\b',
      r'^académie\b',
      r'^rectorat\b',
      r'^office\s+(public|hlm)',
      r'^opac\b',
      r'^oph\b',
      r'^sdis\b',
      r'^syndicat\b',
      r'^sivom\b',
      r'^sivu\b',
      r'^siaep\b',
      r'^caisse\b',
      r'^chambre\s+(de\s+commerce|des\s+métiers|d\'agriculture)',
      r'^port\s+(autonome|de)',
      r'^aéroport\b',
      r'^régie\b',
      r'^sem\b',
      r'^epl\b',
  ]

  GOVERNMENT_PATTERNS_COMPILED = [
      re.compile(pattern, re.IGNORECASE) for pattern in
  GOVERNMENT_ENTITY_PATTERNS
  ]


  def is_government_entity(name: str) -> bool:
      if not name:
          return False
      name_clean = name.strip().lower()
      for pattern in GOVERNMENT_PATTERNS_COMPILED:
          if pattern.search(name_clean):
              return True
      gov_keywords = [
          'mairie', 'commune', 'ville', 'région', 'département',
          'préfecture', 'ministère', 'conseil', 'hôpital', 'centre 
  hospitalier',
          'université', 'lycée', 'collège', 'école', 'syndicat',
          'office public', 'établissement public', 'service public'
      ]
      for keyword in gov_keywords:
          if keyword in name_clean:
              return True
      return False


  def validate_and_swap_if_needed(buyer_name: str, winner_name: str) -> 
  Tuple[str, str]:
      buyer_is_gov = is_government_entity(buyer_name) if buyer_name else
  False
      winner_is_gov = is_government_entity(winner_name) if winner_name
  else False
      if winner_is_gov and not buyer_name:
          logger.info(f"SWAP: Moving government entity '{winner_name}' 
  from winner to buyer")
          return winner_name, None
      if winner_is_gov and buyer_name and not buyer_is_gov:
          logger.info(f"SWAP: Winner '{winner_name}' is gov, buyer 
  '{buyer_name}' is company -> swapping")
          return winner_name, buyer_name
      if winner_is_gov and buyer_is_gov:
          logger.warning(f"BOTH appear to be government: 
  buyer='{buyer_name}', winner='{winner_name}'")
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


  def load_config() -> Config:
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
      def __init__(self, config: Config):
          self.config = config
          self.session = requests.Session()
          self.session.headers.update({
              'User-Agent': 'Mozilla/5.0 (compatible; TenderBridge/1.0)',
              'Accept':
  'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
              'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
          })
          self.supabase: Client = create_client(config.supabase_url,
  config.supabase_key)
          self.anthropic = Anthropic(api_key=config.anthropic_key)
          self.stats = {
              'fetched': 0,
              'parsed': 0,
              'saved': 0,
              'errors': 0,
              'skipped': 0,
              'swapped': 0,
          }

      def fetch_page(self, url: str) -> Optional[str]:
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

      def get_search_url(self, date: datetime) -> str:
          date_str = date.strftime('%Y-%m-%d')
          return (
              f"{self.config.base_url}/avis/liste?"
              f"type=resultat"
              f"&dateParution={date_str}"
              f"&page=1"
              f"&sort=dateParution,desc"
          )

      def extract_notice_links(self, html: str) -> List[str]:
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

      def extract_native_id(self, url: str) -> Optional[str]:
          match = re.search(r'/avis/detail/(\d+-\d+)', url)
          return match.group(1) if match else None

      def compute_content_hash(self, html: str) -> str:
          return hashlib.md5(html.encode('utf-8')).hexdigest()

      def extract_award_with_claude(self, html: str, url: str) -> 
  Optional[Dict]:
          soup = BeautifulSoup(html, 'html.parser')
          for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
              tag.decompose()
          text = soup.get_text(separator='\n', strip=True)
          text = text[:15000]

          prompt = f"""Extract structured data from this French public 
  procurement award notice (BOAMP).

  CRITICAL DISTINCTION - READ CAREFULLY:
  - BUYER (Acheteur/Pouvoir adjudicateur): The GOVERNMENT entity awarding 
  the contract.
    These are ALWAYS public bodies like: Mairie, Commune, Ville, Region, 
  Departement,
    Conseil, Ministere, Prefecture, Centre Hospitalier, Universite, Lycee,
   College,
    Office Public, Syndicat, SDIS, etc.

  - WINNER (Titulaire/Attributaire): The PRIVATE COMPANY that won the 
  contract.
    These are typically: SARL, SAS, SA, EURL, companies with commercial 
  names,
    businesses providing goods/services.

  If you see "Mairie de X" or "Commune de X" or "Ville de X" - that is the
   BUYER, not the winner!
  The winner is the company that was awarded the contract.

  Return ONLY valid JSON with these fields (use null if not found):
  {{
      "buyer_name": "Government entity name (Mairie, Commune, Departement,
   etc.)",
      "buyer_address": "Buyer street address",
      "buyer_city": "Buyer city",
      "winner_name": "Company name that won (NOT a government entity)",
      "winner_address": "Winner street address",
      "winner_city": "Winner city (remove trailing dashes)",
      "award_value": numeric value without currency symbol or null,
      "cpv_codes": ["array", "of", "CPV", "codes"],
      "title": "Contract title/object",
      "short_description": "Brief description of the contract"
  }}

  IMPORTANT VALIDATION:
  - If winner_name contains "Mairie", "Commune", "Ville", "Region", 
  "Departement",
    "Conseil", "Ministere", "Hopital", "Universite", "Lycee", "Ecole", 
  "Syndicat",
    "Office", "Centre hospitalier", "Prefecture" - you have made an error!
    That is the BUYER, not the winner.

  Text to analyze:
  {text}"""

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
                      logger.warning(f"Claude returned government entity 
  as winner: {winner}")
                      new_buyer, new_winner =
  validate_and_swap_if_needed(buyer, winner)
                      data['buyer_name'] = new_buyer
                      data['winner_name'] = new_winner
                      self.stats['swapped'] += 1
                  return data
          except json.JSONDecodeError as e:
              logger.error(f"JSON parse error: {e}")
          except Exception as e:
              logger.error(f"Claude extraction error: {e}")
          return None

      def extract_resultat_section4(self, soup: BeautifulSoup, html: str) 
  -> Dict:
          winner_data = {
              'winner_name': None,
              'winner_address': None,
              'winner_city': None,
              'award_value': None,
          }
          section_4 =
  soup.find(string=re.compile(r'SECTION\s*4|RESULTAT|ATTRIBUTION', re.I))
          if not section_4:
              return winner_data
          section_4_container = section_4.find_parent(['div', 'section'])
          if not section_4_container:
              return winner_data
          section_4_text = section_4_container.get_text(separator='\n',
  strip=True)

          titulaire_match = re.search(
              r'(?:Titulaire|Attributaire)[:\s]*\n?\s*([^\n]+)',
              section_4_text,
              re.IGNORECASE
          )
          if titulaire_match:
              potential_winner = titulaire_match.group(1).strip()
              potential_winner = re.sub(r'^(La societe|L\'entreprise|La 
  SARL|La SAS)\s+', '', potential_winner, flags=re.I)
              if potential_winner and not
  is_government_entity(potential_winner):
                  winner_data['winner_name'] = potential_winner
              else:
                  logger.warning(f"Rejected government entity from 
  titulaire: {potential_winner}")

          if not winner_data['winner_name']:
              marche_match = re.search(
                  r'Marche n\s*:\s*[\d\.]+\s*\n\s*([^\n]+)',
                  section_4_text
              )
              if marche_match:
                  winner_line = marche_match.group(1).strip()
                  parts = [p.strip() for p in winner_line.split(',')]
                  for part in parts:
                      if part and not is_government_entity(part):
                          winner_data['winner_name'] = part
                          break
                  for part in reversed(parts):
                      if re.search(r'\d{5}', part):
                          city = re.sub(r'^\d{5}\s*', '', part).strip(' 
  -')
                          if city:
                              winner_data['winner_city'] = city
                          break

          value_patterns = [
              r'Montant\s*(?:HT|TTC)?\s*[:\s]*\s*([\d\s]+(?:[,\.]\d+)?)\s*
  (?:EUR|euros?)',
              r'Valeur\s*(?:totale)?\s*[:\s]*\s*([\d\s]+(?:[,\.]\d+)?)\s*(
  ?:EUR|euros?)',
              r'([\d\s]+(?:[,\.]\d+)?)\s*(?:EUR|euros?)\s*(?:HT|TTC)?',
          ]
          for pattern in value_patterns:
              value_match = re.search(pattern, section_4_text, re.I)
              if value_match:
                  value_str = value_match.group(1)
                  value_str = value_str.replace(' ', '').replace(',', '.')
                  try:
                      winner_data['award_value'] = float(value_str)
                      break
                  except ValueError:
                      pass
          return winner_data

      def extract_buyer_section1(self, soup: BeautifulSoup) -> Dict:
          buyer_data = {
              'buyer_name': None,
              'buyer_address': None,
              'buyer_city': None,
          }
          section_1 =
  soup.find(string=re.compile(r'SECTION\s*1|IDENTIFICATION|ACHETEUR',
  re.I))
          if not section_1:
              return buyer_data
          section_1_container = section_1.find_parent(['div', 'section'])
          if not section_1_container:
              return buyer_data
          section_1_text = section_1_container.get_text(separator='\n',
  strip=True)

          name_patterns = [

  r'(?:Nom\s+(?:officiel|de\s+l\'acheteur)?)[:\s]*\n?\s*([^\n]+)',
              r'(?:Pouvoir\s+adjudicateur)[:\s]*\n?\s*([^\n]+)',
              r'(?:Acheteur)[:\s]*\n?\s*([^\n]+)',
          ]
          for pattern in name_patterns:
              match = re.search(pattern, section_1_text, re.I)
              if match:
                  buyer_data['buyer_name'] = match.group(1).strip()
                  break

          address_match = re.search(
              r'(?:Adresse\s*(?:postale)?)[:\s]*\n?\s*([^\n]+)',
              section_1_text,
              re.I
          )
          if address_match:
              buyer_data['buyer_address'] = address_match.group(1).strip()

          city_match = re.search(r'(\d{5})\s*([A-Za-z][a-z\-\s]+)',
  section_1_text)
          if city_match:
              buyer_data['buyer_city'] = city_match.group(2).strip()
          return buyer_data

      def extract_cpv_codes(self, soup: BeautifulSoup) -> List[str]:
          cpv_codes = []
          text = soup.get_text()
          cpv_matches = re.findall(r'\b(\d{8}(?:-\d)?)\b', text)
          for code in cpv_matches:
              if code[:2] in ['03', '09', '14', '15', '16', '18', '19',
  '22', '24',
                             '30', '31', '32', '33', '34', '35', '37',
  '38', '39',
                             '42', '43', '44', '45', '48', '50', '51',
  '55', '60',
                             '63', '64', '65', '66', '70', '71', '72',
  '73', '75',
                             '76', '77', '79', '80', '85', '90', '92',
  '98']:
                  if code not in cpv_codes:
                      cpv_codes.append(code)
          return cpv_codes[:10]

      def extract_title(self, soup: BeautifulSoup) -> Optional[str]:
          title_selectors = [
              'h1.notice-title',
              'h1',
              '.titre-avis',
              '[class*="title"]',
          ]
          for selector in title_selectors:
              element = soup.select_one(selector)
              if element:
                  title = element.get_text(strip=True)
                  if title and len(title) > 10:
                      return title[:500]
          return None

      def parse_award_notice(self, html: str, url: str) -> 
  Optional[AwardData]:
          soup = BeautifulSoup(html, 'html.parser')
          native_id = self.extract_native_id(url)
          if not native_id:
              logger.warning(f"Could not extract native ID from {url}")
              return None

          claude_data = self.extract_award_with_claude(html, url)
          buyer_data = self.extract_buyer_section1(soup)
          winner_data = self.extract_resultat_section4(soup, html)
          cpv_codes = self.extract_cpv_codes(soup)
          title = self.extract_title(soup)

          final_buyer_name = (claude_data or {}).get('buyer_name') or
  buyer_data.get('buyer_name')
          final_winner_name = (claude_data or {}).get('winner_name') or
  winner_data.get('winner_name')

          final_buyer_name, final_winner_name =
  validate_and_swap_if_needed(
              final_buyer_name, final_winner_name
          )

          if final_winner_name != ((claude_data or {}).get('winner_name')
  or winner_data.get('winner_name')):
              self.stats['swapped'] += 1

          award = AwardData(
              native_id=native_id,
              title=title or (claude_data or {}).get('title'),
              buyer_name=final_buyer_name,
              buyer_address=(claude_data or {}).get('buyer_address') or
  buyer_data.get('buyer_address'),
              buyer_city=(claude_data or {}).get('buyer_city') or
  buyer_data.get('buyer_city'),
              winner_name=final_winner_name,
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
          return award

      def save_award(self, award: AwardData) -> bool:
          try:
              data = {
                  'native_id': award.native_id,
                  'source': award.source,
                  'title': award.title,
                  'buyer_name': award.buyer_name,
                  'buyer_address': award.buyer_address,
                  'buyer_city': award.buyer_city,
                  'winner_name': award.winner_name,
                  'winner_address': award.winner_address,
                  'winner_city': award.winner_city,
                  'winner_country': award.winner_country,
                  'award_value': award.award_value,
                  'currency': award.currency,
                  'cpv_codes': award.cpv_codes,
                  'cpv_primary': award.cpv_primary,
                  'published_at': award.published_at.isoformat() if
  award.published_at else None,
                  'detail_url': award.detail_url,
                  'short_description': award.short_description,
                  'content_hash': award.content_hash,
              }
              self.supabase.table('france_boamp_daily_normalized').upsert(
                  data,
                  on_conflict='native_id'
              ).execute()
              return True
          except Exception as e:
              logger.error(f"Failed to save award {award.native_id}: {e}")
              return False

      def scrape_date(self, date: datetime) -> int:
          logger.info(f"Scraping awards for {date.strftime('%Y-%m-%d')}")
          search_url = self.get_search_url(date)
          html = self.fetch_page(search_url)
          if not html:
              logger.error(f"Failed to fetch search page for {date}")
              return 0

          notice_links = self.extract_notice_links(html)
          logger.info(f"Found {len(notice_links)} award notices")
          saved_count = 0

          for i, url in enumerate(notice_links):
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
                      logger.warning(f"Skipping {url}: No buyer or winner 
  extracted")
                      self.stats['skipped'] += 1
                      continue

                  if self.save_award(award):
                      saved_count += 1
                      self.stats['saved'] += 1
                      logger.info(f"[{i+1}/{len(notice_links)}] Saved: 
  {award.native_id} - Buyer: {award.buyer_name} - Winner: 
  {award.winner_name}")
                  else:
                      self.stats['errors'] += 1
              except Exception as e:
                  logger.error(f"Error processing {url}: {e}")
                  self.stats['errors'] += 1
          return saved_count

      def scrape_date_range(self, start_date: datetime, end_date: 
  datetime) -> Dict:
          current_date = start_date
          total_saved = 0
          while current_date <= end_date:
              saved = self.scrape_date(current_date)
              total_saved += saved
              current_date += timedelta(days=1)
          return {
              'total_saved': total_saved,
              'stats': self.stats,
          }

      def print_stats(self):
          logger.info("=" * 50)
          logger.info("SCRAPING STATISTICS")
          logger.info("=" * 50)
          logger.info(f"Pages fetched:       {self.stats['fetched']}")
          logger.info(f"Successfully parsed: {self.stats['parsed']}")
          logger.info(f"Saved to database:   {self.stats['saved']}")
          logger.info(f"Buyer/Winner swaps:  {self.stats['swapped']}")
          logger.info(f"Skipped (no data):   {self.stats['skipped']}")
          logger.info(f"Errors:              {self.stats['errors']}")
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
      result = scraper.scrape_date_range(start_date, end_date)
      scraper.print_stats()
      logger.info(f"Completed! Total saved: {result['total_saved']}")


  if __name__ == '__main__':
      main()
