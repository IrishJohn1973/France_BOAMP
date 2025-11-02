# France BOAMP Scraper

Official French procurement bulletin scraper using OpenDataSoft API.

**Source:** https://www.boamp.fr  
**Total Records:** 402,997 tenders available  
**Type:** API Integration (no authentication required)

**Production Status:** ✅ Active  
**Success Rate:** 100%  
**Last Run:** November 2, 2025  
**Records Processed:** 1,000 tenders

## Database Tables
- `france_boamp_parsed` - Parsed tender data with raw HTML

## Fields Extracted
- Title (100%)
- Notice Number (100%)
- Notice Type (100%)
- Department (100%)
- Contract Amounts (7% - award notices only)
- Full HTML (100%)
