import pandas as pd
from score import multibagger_score_two_dim
from data_fetcher_backtest import fetch_quantitative_data

if __name__ == "__main__":
    # --- Backtesting Configuration ---
    BACKTEST_DATE = '2024-12-28' # Last trading day of 2023
    
    # --- Penny Stock Scanner Main Logic ---
    
    # 1. Load stock symbols from the CSV file
    try:
        symbols_df = pd.read_csv("EQUITY_L.csv")
        symbols = symbols_df["SYMBOL"].tolist()
        print(f"Loaded {len(symbols)} symbols from EQUITY_L.csv for backtesting on {BACKTEST_DATE}")
    except FileNotFoundError:
        print("Error: EQUITY_L.csv not found. Please place it in the directory.")
        exit()

    penny_stocks_results = []
    
    # Define market cap threshold for penny stocks (e.g., 500 Crore INR)
    MARKET_CAP_THRESHOLD = 10500 * 10**7 
    STOCK_PRICE_THRESHOLD = 2200
    PROMOTER_HOLDING_THRESHOLD = 20

    # 2. Iterate through each symbol
    for symbol in symbols:
        ticker = f"{symbol}.NS"
        try:
            print(f"\nProcessing: {ticker} for date {BACKTEST_DATE}...")
            
            # 3. Fetch full quantitative data for the backtest date
            quantitative_data = fetch_quantitative_data(ticker, backtest_date=BACKTEST_DATE)
            
            if not quantitative_data or not quantitative_data.get('metrics'):
                print(f"  Skipping {ticker}: No data returned from fetcher.")
                continue

            metrics = quantitative_data['metrics']
            price = metrics.get('stock_price')
            market_cap = metrics.get('market_cap')

            # 4. Pre-filter with a lightweight initial check
            if price is None or market_cap is None:
                print(f"  Skipping {ticker}: Missing historical price or market cap data for {BACKTEST_DATE}.")
                continue

            # Check initial penny stock criteria
            if price < STOCK_PRICE_THRESHOLD and market_cap < MARKET_CAP_THRESHOLD:
                print(f"  Passed pre-filter (Price: {price:.2f}, Mkt Cap: {market_cap/10**7:.2f} Cr).")

                # 5. Apply final filter on promoter holding (using live data as a fallback)
                promoter_holding = metrics.get('promoter_holding', 0)
                if promoter_holding and promoter_holding > PROMOTER_HOLDING_THRESHOLD:
                    print(f"  PENNY STOCK IDENTIFIED: {ticker} (Promoter Holding: {promoter_holding:.2f}%)")
                    
                    # 6. Calculate score
                    stock_for_scoring = {
                        "quantitative": metrics,
                        "qualitative": {}
                    }
                    score, percentage_score, breakdown = multibagger_score_two_dim(stock_for_scoring)
                    
                    # Print real-time results
                    print(f"  >> {ticker} | Price: {price:.2f} | Score: {percentage_score:.2f}%")

                    # 7. Store results for final CSV
                    result_data = {
                        "Symbol": ticker,
                        "Price": price,
                        "Market Cap (Cr)": market_cap / 10**7,
                        "Score (%)": percentage_score,
                    }
                    # Add (s) to score breakdown keys
                    breakdown_with_suffix = {f"{key} (s)": v for key, v in breakdown.items()}
                    
                    # Add all raw metrics and the score breakdown to the results
                    result_data.update(metrics)
                    result_data.update(breakdown_with_suffix)
                    penny_stocks_results.append(result_data)
                    
                else:
                    print(f"  Skipping {ticker}: Fails promoter holding check ({promoter_holding=}).")
            else:
                print(f"  Skipping {ticker}: Fails pre-filter criteria.")
                
        except Exception as e:
            print(f"  An error occurred processing {ticker}: {e}")
            continue
            
    # 8. Save all identified penny stocks to a new CSV file
    if penny_stocks_results:
        output_df = pd.DataFrame(penny_stocks_results)
        output_filename = f"penny_stock_scores_backtest_{BACKTEST_DATE}.csv"
        output_df.to_csv(output_filename, index=False)
        print(f"\nProcessing complete. Found {len(penny_stocks_results)} penny stocks.")
        print(f"Results saved to {output_filename}")
    else:
        print("\nProcessing complete. No penny stocks found matching the criteria.")
