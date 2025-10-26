#!/usr/bin/env python3
"""
Reference Exchange Rate Provider
Fetches USD/TWD reference rates from external source.
Independent script without database dependencies.
"""


from bs4 import BeautifulSoup
import asyncio
import httpx
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

#TODO: Historical data needs adjustment on business days following holidays
async def fetch_quote_closest_to_15_00_async(date: str, logger: Optional[logging.Logger] = None) -> Optional[Dict]:
    """
    Async function to directly fetch the quote closest to 15:00 from reference source.
    
    Args:
        date: Date in YYYY-MM-DD format
        logger: Optional logger instance for logging events
        
    Returns:
        Quote closest to 15:00 with full details
    """
    # Create default logger if none provided
    if logger is None:
        logger = logging.getLogger('reference_rate_fetcher')
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
    
    try:
        logger.info(f"Async fetching quote closest to 15:00 for {date}...")
        
        url = f"https://rate.bot.com.tw/xrt/quote/{date}/USD/spot"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
        }
        
        logger.debug(f"Fetching URL: {url}")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            html_content = response.text

        logger.debug(f"Received {len(html_content)} characters from reference source")
        # Parse and find closest to 15:00
        closest_quote = parse_and_find_15_00_quote_sync(html_content, date, logger)
        
        if closest_quote:
            time_str = closest_quote.get('datetime', 'Unknown')
            time_diff = closest_quote.get('time_diff_minutes', 0)
            logger.info(f"Successfully fetched quote for {date} at {time_str}: "
                       f"{closest_quote['spot_buy']}/{closest_quote['spot_sell']} "
                       f"(±{time_diff:.1f}min from 15:00)")
        
        return closest_quote
        
    except Exception as e:
        logger.error(f"Error fetching quote for {date}: {e}")
        return None


def parse_and_find_15_00_quote_sync(html_content: str, date: str, logger: Optional[logging.Logger] = None) -> Optional[Dict]:
    """
    Parse HTML and directly find the quote closest to 15:00.

    Args:
        html_content: Raw HTML from reference source
        date: Date in YYYY-MM-DD format
        logger: Optional logger instance

    Returns:
        Quote data closest to 15:00
    """
    # Create default logger if none provided
    if logger is None:
        logger = logging.getLogger('reference_rate_parser')
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
    
    try:
        # logger.info(f"Parsing HTML: {html_content}")
        logger.debug(f"Starting to parse HTML content for {date} (length: {len(html_content)} chars)")
        
        # Extract all timestamps and rates from the historical page
        # Look for table rows with time and exchange rate data

        html_content = BeautifulSoup(html_content, 'html.parser')

        # tr_tag = html_content.find_all('td',  class_='text-right rate-content-cash')

        # 2. Locate and extract all <td> tags
        # all_tds = tr_tag.find_all('td')
        t_body_tag = html_content.find('tbody')
        row_tags = t_body_tag.find_all('tr')
        # entry_tags = t_body_tag.find_all('td')
        # time_tags = t_body_tag.find_all('td',  class_='text-center')
        # rate_tags = t_body_tag.find_all('td',  class_='text-right print_table-cell rate-content-sight')

        quote_data = []
        # print(t_body_tag)
        for entry in row_tags :
            columns = entry.find_all('td')
            dt = datetime.strptime(columns[0].text, '%Y/%m/%d %H:%M:%S')
            cash_bid = float(columns[2].text)
            cash_ask = float(columns[3].text)
            rate_bid = float(columns[4].text)
            rate_ask = float(columns[5].text)
            quote_data.append({'date': dt, 'cash_bid': cash_bid, 'cash_ask': cash_ask, 'rate_bid': rate_bid, 'rate_ask': rate_ask})


        # Target: 15:00:00
        target_seconds = 15 * 3600  # 15:00 in seconds from midnight

        
        # closest_time = None
        min_diff = float('inf')
        target_data = None

        for data in quote_data:
            date = data['date']

            time_seconds = date.hour * 3600 + date.minute * 60 + date.second

            if time_seconds > target_seconds:
                break

            diff = abs(time_seconds - target_seconds)

            if diff < min_diff:
                min_diff = diff
                target_data = data
                # logger.debug(f"New closest time: {date} (diff: {diff/60:.1f} minutes)")

        
        if not target_data:
            logger.error(f"Could not find suitable time for {date}")
            return None
        
        logger.debug(f"Selected closest time: {target_data['date']} (±{min_diff/60:.1f} min from 15:00)")

        quote_dt = target_data["date"]

        quote_timestamp =  int(quote_dt.timestamp() * 1000)
        update_time = datetime.now()
        update_time_ts = int( update_time.timestamp() * 1000)
        quote_data = {
            'timestamp': quote_timestamp,
            'datetime': quote_dt ,
            'update_timestamp': update_time_ts,
            'update_datetime': update_time,
            'cash_buy': target_data['cash_bid'],
            'cash_sell': target_data['cash_ask'],
            'spot_buy': target_data['rate_bid'],
            'spot_sell': target_data['rate_ask'],
            'mid_price': round ((target_data['rate_bid'] +target_data['rate_ask'])/2 , 4),
            'spread': round(-target_data['rate_bid'] + target_data['rate_ask'], 4),
            'spread_bps': int(((target_data['rate_ask'] - target_data['rate_bid']) / target_data['rate_bid']) * 10000),
            'target_time': '15:00:00',
            'source': 'async_direct_fetch'
        }
        
        logger.info(f"Found quote at {quote_dt}, mid: {quote_data['mid_price']} ")
        return quote_data
        
    except Exception as e:
        logger.error(f"Error parsing async quote: {e}")
        return None




# Test function with custom logger
async def demo_with_custom_logger():
    """Test function with custom logger configuration."""
    # Create a custom logger with detailed formatting
    logger = logging.getLogger('custom_reference_rate_test')
    logger.setLevel(logging.DEBUG)

    # Clear any existing handlers
    logger.handlers.clear()

    # Create console handler with custom format
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    print("Testing Reference Rate Fetcher with Custom Logger")
    print("="*60)

    test_date = "2024-09-25"
    print(f"\nFetching quote for {test_date} with detailed logging...")
    
    quote = await fetch_quote_closest_to_15_00_async(test_date, logger)
    
    if quote:
        print(f"\nSUCCESS: Retrieved quote data:")
        print(f"   Time: {quote['datetime']}")
        print(f"   Rates: {quote['spot_buy']}/{quote['spot_sell']}")
        print(f"   Spread: {quote['spread']:.4f} TWD ({quote['spread_bps']} bps)")
    else:
        print(f"\nFAILED: Could not retrieve quote for {test_date}")


# Simple test function for direct 15:00 quote fetching
async def demo_15_00_fetching():
    """Test function specifically for 15:00 quote fetching."""
    print("Testing Direct 15:00 Quote Fetching")
    print("="*50)

    test_dates = ["2024-09-24", "2024-09-23", "2024-09-22"]

    for date in test_dates:
        print(f"\nTesting {date}...")
        quote = await fetch_quote_closest_to_15_00_async(date)
        
        if quote:
            time_str = quote.get('closest_time', 'Unknown')
            time_diff = quote.get('time_diff_minutes', 0)
            print(f"{date} at {time_str}: {quote['spot_buy']}/{quote['spot_sell']} (±{time_diff:.1f}min from 15:00)")
        else:
            print(f"Failed to get quote for {date}")


if __name__ == "__main__":
    # Choose which demo to run
        asyncio.run(demo_with_custom_logger())