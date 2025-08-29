#!/usr/bin/env python3
"""
Advanced NARPM Members API Scraper
Full-featured version with CSV export, logging, and comprehensive error handling
"""

import requests
import json
import time
from datetime import datetime
import csv
import os
from typing import List, Dict, Optional
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('narpm_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NARPMScraper:
    def __init__(self, limit: int = 20, delay: float = 0.8):
        """
        Initialize the NARPM scraper
        
        Args:
            limit: Records per API call (20 for balanced performance)
            delay: Delay between API calls in seconds
        """
        self.base_url = "https://api.blankethomes.com/narpm-members"
        self.limit = limit
        self.delay = delay
        self.all_data = []
        
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'origin': 'https://www.narpm.org',
            'priority': 'u=1, i',
            'referer': 'https://www.narpm.org/',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }
    
    def fetch_page(self, offset: int, retry_count: int = 0) -> Optional[Dict]:
        """
        Fetch a single page of data with retry logic
        
        Args:
            offset: Starting record number
            retry_count: Current retry attempt
            
        Returns:
            API response data or None if failed
        """
        url = f"{self.base_url}?offset={offset}&limit={self.limit}"
        
        try:
            logger.info(f"Fetching data: offset={offset}, limit={self.limit}")
            
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✅ Successfully fetched {len(data.get('data', data)) if isinstance(data, (dict, list)) else 1} records")
                return data
            
            elif response.status_code == 429:  # Rate limited
                wait_time = min(10 * (2 ** retry_count), 60)  # Exponential backoff, max 60s
                logger.warning(f"Rate limited. Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                if retry_count < 3:
                    return self.fetch_page(offset, retry_count + 1)
                return None
            
            elif response.status_code in [500, 502, 503, 504]:  # Server errors
                if retry_count < 2:
                    wait_time = 5 * (retry_count + 1)
                    logger.warning(f"Server error {response.status_code}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    return self.fetch_page(offset, retry_count + 1)
                else:
                    logger.error(f"❌ Server error {response.status_code} after retries: {response.text}")
                    return None
            
            else:
                logger.error(f"❌ HTTP {response.status_code}: {response.text[:200]}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error("⏰ Request timed out")
            if retry_count < 2:
                time.sleep(2)
                return self.fetch_page(offset, retry_count + 1)
            return None
            
        except requests.exceptions.ConnectionError:
            logger.error("🔌 Connection error")
            if retry_count < 2:
                time.sleep(5)
                return self.fetch_page(offset, retry_count + 1)
            return None
            
        except Exception as e:
            logger.error(f"💥 Unexpected error: {str(e)}")
            return None
    
    def scrape_all_pages(self, total_pages: int = 456) -> List[Dict]:
        """
        Scrape all pages of data
        
        Args:
            total_pages: Total number of pages to scrape (default based on 456 * 12 records)
            
        Returns:
            List of all records
        """
        # Calculate actual pages needed based on limit
        estimated_total_records = total_pages * 12  # 5,472 records
        actual_pages = (estimated_total_records + self.limit - 1) // self.limit
        
        logger.info(f"🚀 Starting to scrape with limit={self.limit}")
        logger.info(f"📊 Will make up to {actual_pages} API calls to get ~{estimated_total_records} records")
        
        successful_calls = 0
        failed_calls = 0
        empty_responses = 0
        
        for page in range(actual_pages):
            offset = page * self.limit
            
            logger.info(f"📄 Processing page {page + 1}/{actual_pages} (offset: {offset})")
            
            # Fetch page data
            page_data = self.fetch_page(offset)
            
            if page_data:
                # Handle different response formats
                if isinstance(page_data, dict) and 'data' in page_data:
                    records = page_data['data']
                elif isinstance(page_data, list):
                    records = page_data
                else:
                    records = [page_data] if page_data else []
                
                if records:
                    self.all_data.extend(records)
                    successful_calls += 1
                    logger.info(f"✅ Added {len(records)} records. Total so far: {len(self.all_data)}")
                else:
                    empty_responses += 1
                    logger.warning(f"📭 Empty response #{empty_responses} - might have reached the end")
                    if empty_responses >= 3:  # Stop after 3 consecutive empty responses
                        logger.info("🛑 Stopping due to consecutive empty responses")
                        break
            else:
                failed_calls += 1
                logger.error(f"❌ Failed to fetch page {page + 1}")
                if failed_calls >= 10:  # Stop if too many failures
                    logger.error("🛑 Too many failures, stopping scraper")
                    break
            
            # Rate limiting delay
            if page < actual_pages - 1:
                time.sleep(self.delay)
            
            # Progress update every 20 pages
            if (page + 1) % 20 == 0:
                progress_pct = ((page + 1) / actual_pages) * 100
                logger.info(f"🔄 Progress: {page + 1}/{actual_pages} pages ({progress_pct:.1f}%) - {len(self.all_data)} total records")
        
        logger.info(f"✅ Scraping completed!")
        logger.info(f"📊 Total records collected: {len(self.all_data)}")
        logger.info(f"✅ Successful API calls: {successful_calls}")
        logger.info(f"❌ Failed API calls: {failed_calls}")
        
        return self.all_data
    
    def save_to_json(self, filename: Optional[str] = None) -> str:
        """Save data to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"narpm_members_{timestamp}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    'scraped_at': datetime.now().isoformat(),
                    'total_records': len(self.all_data),
                    'scraper_config': {
                        'limit': self.limit,
                        'delay': self.delay
                    },
                    'data': self.all_data
                }, f, indent=2, ensure_ascii=False)
            
            file_size = os.path.getsize(filename) / (1024 * 1024)  # MB
            logger.info(f"💾 JSON saved: {filename} ({file_size:.1f} MB)")
            return filename
        except Exception as e:
            logger.error(f"❌ Failed to save JSON: {str(e)}")
            return ""
    
    def save_to_csv(self, filename: Optional[str] = None) -> str:
        """Save data to CSV file"""
        if not self.all_data:
            logger.warning("No data to save")
            return ""
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"narpm_members_{timestamp}.csv"
        
        try:
            # Get all unique keys from all records
            all_keys = set()
            for record in self.all_data:
                if isinstance(record, dict):
                    all_keys.update(record.keys())
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                if all_keys:
                    writer = csv.DictWriter(f, fieldnames=sorted(all_keys))
                    writer.writeheader()
                    writer.writerows(self.all_data)
            
            file_size = os.path.getsize(filename) / (1024 * 1024)  # MB
            logger.info(f"📊 CSV saved: {filename} ({file_size:.1f} MB)")
            return filename
        except Exception as e:
            logger.error(f"❌ Failed to save CSV: {str(e)}")
            return ""
    
    def get_summary_stats(self) -> Dict:
        """Get summary statistics of the scraped data"""
        if not self.all_data:
            return {}
        
        stats = {
            'total_records': len(self.all_data),
            'scraped_at': datetime.now().isoformat(),
            'sample_record': self.all_data[0] if self.all_data else None,
            'scraper_config': {
                'limit': self.limit,
                'delay': self.delay
            }
        }
        
        # Try to get field statistics
        if self.all_data and isinstance(self.all_data[0], dict):
            stats['fields'] = list(self.all_data[0].keys())
            stats['field_count'] = len(stats['fields'])
            
            # Get field value counts for interesting fields
            field_stats = {}
            interesting_fields = ['state', 'status', 'type', 'category']
            for field in interesting_fields:
                if field in stats['fields']:
                    values = [record.get(field) for record in self.all_data if record.get(field)]
                    if values:
                        unique_values = list(set(values))
                        field_stats[field] = {
                            'unique_count': len(unique_values),
                            'sample_values': unique_values[:5]  # First 5 unique values
                        }
            
            if field_stats:
                stats['field_statistics'] = field_stats
        
        return stats

def main():
    """Main function to run the scraper"""
    
    print("🏠 NARPM Members Advanced Scraper")
    print("=" * 50)
    
    # Configuration options
    print("\n📋 Configuration Options:")
    print("1. Default: limit=20 (balanced performance) ✅")
    print("2. Fast: limit=100 (fewer API calls)")
    print("3. Small batches: limit=12 (original spec)")
    
    choice = input("\nChoose option (1-3) or press Enter for default (1): ").strip()
    
    if choice == "2":
        limit = 100
        delay = 1.5
    elif choice == "3":
        limit = 12
        delay = 0.5
    else:
        limit = 20  # Default as requested
        delay = 0.8
    
    # Export format options
    print(f"\n📁 Export format:")
    print("1. JSON only")
    print("2. CSV only") 
    print("3. Both JSON and CSV ✅")
    
    export_choice = input("\nChoose format (1-3) or press Enter for both (3): ").strip()
    
    # Calculate and show the plan
    estimated_total_records = 456 * 12  # 5,472 records
    estimated_api_calls = (estimated_total_records + limit - 1) // limit
    estimated_time = (estimated_api_calls * delay) / 60  # minutes
    
    print(f"\n📊 Scraping Plan:")
    print(f"   • Limit per request: {limit}")
    print(f"   • Estimated API calls: {estimated_api_calls}")
    print(f"   • Estimated time: {estimated_time:.1f} minutes")
    print(f"   • Target records: ~{estimated_total_records}")
    
    confirm = input(f"\n🚀 Start scraping? (y/n): ").strip().lower()
    if confirm not in ['y', 'yes', '']:
        print("❌ Scraping cancelled")
        return
    
    # Initialize and run scraper
    scraper = NARPMScraper(limit=limit, delay=delay)
    
    # Start scraping
    start_time = time.time()
    print(f"\n⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    data = scraper.scrape_all_pages(total_pages=456)
    
    end_time = time.time()
    duration = end_time - start_time
    
    if data:
        # Save results based on user choice
        files_saved = []
        
        if export_choice in ['1', '3', '']:  # JSON
            json_file = scraper.save_to_json()
            if json_file:
                files_saved.append(json_file)
        
        if export_choice in ['2', '3', '']:  # CSV
            csv_file = scraper.save_to_csv()
            if csv_file:
                files_saved.append(csv_file)
        
        # Show results
        stats = scraper.get_summary_stats()
        
        print("\n" + "=" * 60)
        print("🎉 SCRAPING COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"📊 Total Records: {stats.get('total_records', 0):,}")
        print(f"⏱️  Total Time: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        print(f"📈 Rate: {stats.get('total_records', 0)/duration:.1f} records/second")
        print(f"💾 Files Saved: {', '.join(files_saved)}")
        print(f"📝 Log File: narpm_scraper.log")
        
        if stats.get('field_statistics'):
            print(f"\n📋 Field Statistics:")
            for field, info in stats.get('field_statistics', {}).items():
                print(f"   • {field}: {info['unique_count']} unique values")
        
        if stats.get('sample_record'):
            print(f"\n🔍 Sample Record Fields ({len(stats.get('fields', []))} total):")
            for field in stats.get('fields', [])[:10]:
                print(f"   • {field}")
            if len(stats.get('fields', [])) > 10:
                print(f"   ... and {len(stats.get('fields', [])) - 10} more fields")
        
        print("\n✅ Check the log file for detailed scraping information.")
    
    else:
        print("❌ No data was scraped. Check narpm_scraper.log for detailed error information.")

def quick_test():
    """Quick test function to verify API connectivity"""
    print("🧪 Running API connectivity test...")
    
    scraper = NARPMScraper(limit=5)
    test_data = scraper.fetch_page(0)
    
    if test_data:
        print("✅ API test successful!")
        if isinstance(test_data, dict) and 'data' in test_data:
            sample_count = len(test_data['data'])
        else:
            sample_count = 1
        print(f"📊 Got {sample_count} sample records")
        return True
    else:
        print("❌ API test failed! Check your internet connection.")
        return False

if __name__ == "__main__":
    # Uncomment to run connectivity test first
    # if quick_test():
    #     print()
    #     main()
    # else:
    #     print("❌ Skipping scraper due to connectivity issues.")
    
    main()