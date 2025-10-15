import yfinance as yf
import pandas as pd
from curl_cffi.requests import Session
from score import multibagger_score_two_dim
from data_fetcher import fetch_quantitative_data
import schedule
import time
import argparse
from datetime import datetime
import requests
import io
import zipfile

def download_equity_list():
    """
    Downloads the latest EQUITY_L.csv file from the NSE archives.
    It handles both direct CSV and zipped CSV files.
    """
    url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    filename = "EQUITY_L.csv"
    
    try:
        print(f"Downloading latest equity list from {url}...")
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()  # Raise an exception for bad status codes

        content_type = response.headers.get('Content-Type', '').lower()

        if 'application/zip' in content_type:
            print("Zip file detected. Extracting...")
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # Find the first CSV file in the zip archive
                try:
                    csv_filename_in_zip = next(name for name in z.namelist() if name.upper().endswith('.CSV'))
                    with z.open(csv_filename_in_zip) as zf, open(filename, 'wb') as f:
                        f.write(zf.read())
                    print(f"Successfully extracted and saved '{csv_filename_in_zip}' as '{filename}'.")
                except StopIteration:
                    print("Error: Zip file does not contain a CSV file.")
                    return False
        elif 'text/csv' in content_type:
            print("CSV file detected. Saving directly.")
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f"Successfully downloaded and saved '{filename}'.")
        else:
            # Fallback for unexpected content types, e.g. 'application/octet-stream'
            print(f"Uncertain content type '{content_type}'. Attempting to save as CSV.")
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f"Successfully downloaded and saved '{filename}'.")
            
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the equity list: {e}")
        return False
    except zipfile.BadZipFile:
        print("Error: Downloaded file is not a valid zip file.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during download: {e}")
        return False

def run_scanner():
    """
    Scans for penny stocks based on defined criteria, calculates their scores,
    and saves the results to a CSV file named with the current date.
    """
    print(f"--- Starting Penny Stock Scan at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    # 0. Download the latest equity list
    if not download_equity_list():
        print("Failed to download equity list. Attempting to use existing local file...")

    # 1. Load stock symbols from the CSV file
    try:
        symbols_df = pd.read_csv("EQUITY_L.csv")
        symbols = symbols_df["SYMBOL"].tolist()
        print(f"Loaded {len(symbols)} symbols from EQUITY_L.csv")
    except FileNotFoundError:
        print("Error: EQUITY_L.csv not found and download failed. Aborting scan.")
        return
    except Exception as e:
        print(f"Error reading EQUITY_L.csv: {e}. The file might be corrupted or in an unexpected format.")
        return

    penny_stocks_results = []
    
    # Define market cap threshold for penny stocks (e.g., 500 Crore INR)
    MARKET_CAP_THRESHOLD = 500 * 10**7 
    STOCK_PRICE_THRESHOLD = 100
    PROMOTER_HOLDING_THRESHOLD = 50

    # Create a single session to reuse
    session = Session(impersonate="chrome110")
    session.verify = False

    # 2. Iterate through each symbol
    for symbol in symbols:
        ticker = f"{symbol}.NS"
        try:
            print(f"\nProcessing: {ticker}...")
            
            # 3. Fetch initial and detailed data for every stock
            stock_info = yf.Ticker(ticker, session=session).info
            price = stock_info.get("regularMarketPrice", stock_info.get("currentPrice"))
            market_cap = stock_info.get("marketCap")

            if price is None or market_cap is None:
                print(f"  Skipping {ticker}: Missing essential price or market cap data.")
                continue

            quantitative_data = fetch_quantitative_data(ticker)
            metrics = quantitative_data['metrics']
            promoter_holding = metrics.get('promoter_holding', 0)

            # 4. Check if the stock meets the criteria and set a flag
            is_low_price = price < STOCK_PRICE_THRESHOLD
            is_small_cap = market_cap < MARKET_CAP_THRESHOLD
            has_high_promoter_holding = promoter_holding and promoter_holding > PROMOTER_HOLDING_THRESHOLD
            is_penny_stock = is_low_price and is_small_cap and has_high_promoter_holding

            if is_penny_stock:
                print(f"  PENNY STOCK: {ticker} (Price: {price:.2f}, Mkt Cap: {market_cap/10**7:.2f} Cr, Promoter: {promoter_holding:.2f}%)")
            
            # 5. Always calculate score
            stock_for_scoring = {
                "quantitative": metrics,
                "qualitative": {}
            }
            score, percentage_score, breakdown = multibagger_score_two_dim(stock_for_scoring)
            
            print(f"  >> {ticker} | Price: {price:.2f} | Score: {percentage_score:.2f}%")

            # 6. Always store results for final CSV
            result_data = {
                "Symbol": ticker,
                "Price": price,
                "Market Cap (Cr)": market_cap / 10**7,
                "Score (%)": percentage_score,
                "Penny_stock": is_penny_stock
            }
            breakdown_with_suffix = {f"{key} (s)": v for key, v in breakdown.items()}
            
            result_data.update(metrics)
            result_data.update(breakdown_with_suffix)
            penny_stocks_results.append(result_data)
            
        except Exception as e:
            print(f"  An error occurred processing {ticker}: {e}")
            # Optionally, save partial data or just the error
            error_data = {"Symbol": ticker, "Error": str(e)}
            penny_stocks_results.append(error_data)
            continue
            
    # 8. Save all identified penny stocks to a new CSV file
    if penny_stocks_results:
        output_df = pd.DataFrame(penny_stocks_results)
        date_str = datetime.now().strftime("%d_%m_%y")
        output_filename = f"penny_stock_scores_{date_str}.csv"
        output_df.to_csv(output_filename, index=False)
        print(f"\nProcessing complete. Found {len(penny_stocks_results)} penny stocks.")
        print(f"Results saved to {output_filename}")
    else:
        print("\nProcessing complete. No penny stocks found matching the criteria.")
    
    print(f"--- Scan Finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")


if __name__ == "__main__":
    run_scanner()
