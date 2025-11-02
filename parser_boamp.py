from bs4 import BeautifulSoup
import re

def parse_boamp_tender(html):
    """Extract all fields from BOAMP tender HTML"""
    soup = BeautifulSoup(html, 'lxml')
    
    data = {}
    
    # Basic info
    title_tag = soup.find('title')
    data['title'] = title_tag.text if title_tag else None
    
    # Extract tender ID from title or find it
    annonce = soup.find(text=re.compile('Annonce n°'))
    if annonce:
        data['notice_number'] = annonce.find_next('strong').text if annonce.find_next('strong') else None
    
    # Notice type
    doc_titre = soup.find(id='doc_titre')
    data['notice_type'] = doc_titre.text.strip() if doc_titre else None
    
    # Buyer information
    data['buyer_name'] = extract_field(soup, 'Nom officiel', section='section_1')
    data['buyer_type'] = extract_field(soup, 'Forme juridique')
    data['buyer_activity'] = extract_field(soup, 'Activité du pouvoir adjudicateur')
    
    # Procedure information
    data['tender_title'] = extract_field(soup, 'Titre', section='section_2')
    data['description'] = extract_field(soup, 'Description', section='section_2')
    data['procedure_id'] = extract_field(soup, 'Identifiant de la procédure')
    data['internal_id'] = extract_field(soup, 'Identifiant interne')
    data['procedure_type'] = extract_field(soup, 'Type de procédure')
    
    # CPV codes
    cpv_elements = soup.find_all(text='cpv')
    cpv_codes = []
    for cpv in cpv_elements:
        code = cpv.find_next('span')
        if code and code.text.isdigit():
            cpv_codes.append(code.text)
    data['cpv_codes'] = ','.join(set(cpv_codes)) if cpv_codes else None
    
    # Values
    estimated_value = extract_field(soup, 'Valeur estimée hors TVA')
    data['estimated_value'] = parse_amount(estimated_value) if estimated_value else None
    
    # Extract contract amounts from description
    amounts = re.findall(r'(\d[\d\s,]*)\s*euro\(s\)\s*[HT|Ht]', str(soup))
    data['contract_amounts'] = ','.join([a.replace(' ', '').replace(',', '') for a in amounts[:3]]) if amounts else None
    
    # Winner information (Section 8 - Organizations)
    winner = extract_winner_info(soup)
    if winner:
        data.update(winner)
    
    # Dates
    date_envoi = extract_field(soup, "Date d'envoi de l'avis")
    data['published_date'] = date_envoi
    
    # Department
    dept = soup.find(text=re.compile('Département'))
    if dept:
        dept_num = dept.find_next('strong')
        data['department'] = dept_num.text if dept_num else None
    
    return data

def extract_field(soup, field_name, section=None):
    """Extract a field value by its label"""
    if section:
        section_div = soup.find(id=re.compile(section))
        if section_div:
            soup = section_div
    
    field = soup.find(text=re.compile(field_name))
    if field:
        # Get the next span/text after the label
        next_elem = field.find_next(['span', 'div'])
        if next_elem and next_elem.text.strip() and next_elem.text.strip() != ':':
            return next_elem.text.strip()
    return None

def extract_winner_info(soup):
    """Extract winner/contractor information"""
    winner_data = {}
    
    # Find the section with "Lauréat de ces lots"
    laureat = soup.find(text=re.compile('Lauréat de ces lots'))
    if laureat:
        # Go back to find the organization section
        org_section = laureat.find_parent('div', class_='section')
        if org_section:
            parent = org_section.find_parent('div', class_='section')
            if parent:
                # Extract winner details from this section
                winner_data['winner_name'] = extract_field(parent, 'Nom officiel')
                winner_data['winner_email'] = extract_field(parent, 'Adresse électronique')
                winner_data['winner_phone'] = extract_field(parent, 'Téléphone')
                winner_data['winner_city'] = extract_field(parent, 'Ville')
                winner_data['winner_postal_code'] = extract_field(parent, 'Code postal')
                winner_data['winner_country'] = extract_field(parent, 'Pays')
                winner_data['winner_size'] = extract_field(parent, "Taille de l'opérateur économique")
    
    return winner_data

def parse_amount(amount_str):
    """Parse amount string to float"""
    if not amount_str:
        return None
    # Remove spaces, commas and convert
    clean = amount_str.replace(' ', '').replace(',', '')
    try:
        return float(clean)
    except:
        return None

# Test
if __name__ == "__main__":
    with open('sample_tender.html', 'r') as f:
        html = f.read()
    
    result = parse_boamp_tender(html)
    
    print("Extracted fields:")
    for key, value in result.items():
        print(f"{key}: {value}")
