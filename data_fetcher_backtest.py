import yfinance as yf
import pandas as pd
import numpy as np
from curl_cffi.requests import Session
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from utils import create_quantitative_skeleton, cagr

def fetch_data_from_screener(ticker, session, backtest_date=None):
    """
    Fetches promoter holding data from Screener.in.
    If backtest_date is provided, it fetches the data for the quarter preceding that date.
    """
    screener_data = {
        "promoter_holding": np.nan,
        "promoter_holding_growth": np.nan,
    }
    try:
        base_ticker = ticker.replace('.NS', '')
        screener_url = f"https://www.screener.in/company/{base_ticker}/consolidated/"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        
        response = session.get(screener_url, headers=headers, impersonate="chrome110", verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        shareholding_section = soup.find('section', id='shareholding')
        if not shareholding_section:
            raise ValueError("Shareholding section not found")

        table = shareholding_section.find('table')
        if not table:
            raise ValueError("Shareholding table not found")

        # Find Promoter row
        promoter_row = None
        for row in table.find_all('tr'):
            first_cell = row.find('td')
            if first_cell and 'promoter' in first_cell.get_text(strip=True).lower():
                promoter_row = row
                break
        
        if not promoter_row:
            raise ValueError("Promoter holding row not found")

        data_cells = promoter_row.find_all('td')
        
        # --- Logic for both Live and Backtest ---
        header_cells = table.find('thead').find_all('th')
        # Get date columns, skipping the first 'Shareholder' column
        date_columns = [pd.to_datetime(th.text.strip(), format='%b %Y', errors='coerce') for th in header_cells[1:]]
        
        target_idx = -1 # Default to last column for live data
        
        if backtest_date:
            backtest_date_ts = pd.to_datetime(backtest_date)
            # Find the index of the last date column that is before or on the backtest date
            valid_indices = [i for i, d in enumerate(date_columns) if pd.notna(d) and d <= backtest_date_ts]
            if not valid_indices:
                raise ValueError(f"No shareholding data available on or before {backtest_date}")
            target_idx = valid_indices[-1]

        # Extract values using the determined index
        # Add 1 to index to account for the 'Promoter' label cell in data_cells
        if len(data_cells) > target_idx + 1:
            try:
                current_holding_str = data_cells[target_idx + 1].text.strip().replace('%', '').strip()
                screener_data['promoter_holding'] = float(current_holding_str)
            except (ValueError, IndexError):
                pass # Keep as NaN

        # For growth, look at the previous column
        if target_idx > 0 and len(data_cells) > target_idx:
            try:
                previous_holding_str = data_cells[target_idx].text.strip().replace('%', '').strip()
                previous_holding = float(previous_holding_str)
                if screener_data['promoter_holding'] is not np.nan:
                     screener_data['promoter_holding_growth'] = screener_data['promoter_holding'] - previous_holding
            except (ValueError, IndexError):
                pass # Keep as NaN

    except Exception as e:
        print(f"Could not fetch promoter data for {ticker} from Screener.in: {e}")

    return screener_data

def fetch_quantitative_data(ticker_symbol, backtest_date=None, period="5y"):
    """
    Fetches and calculates a comprehensive set of quantitative financial metrics for a given stock ticker.
    If a backtest_date is provided, it calculates metrics using only data available up to that date.
    """
    fin_data = create_quantitative_skeleton()
    raw_data = {}
    try:
        session = Session(impersonate="chrome110")
        session.verify = False  # Explicitly disable SSL verification for the session
        stock = yf.Ticker(ticker_symbol, session=session)
        
        # --- Basic Info & Screener Data ---
        screener_data = fetch_data_from_screener(ticker_symbol, session, backtest_date=backtest_date)
        fin_data.update(screener_data)

        # --- Fetch all financial statements ---
        financials = stock.financials
        balance_sheet = stock.balance_sheet
        cash_flow = stock.cashflow
        quarterly_fin = stock.quarterly_financials
        quarterly_bal = stock.quarterly_balance_sheet

        # --- Live Mode (use yfinance 'info' for speed) ---
        if not backtest_date:
            info = stock.info
            fin_data.update({
                'market_cap': info.get("marketCap"),
                'stock_price': info.get("regularMarketPrice", info.get("currentPrice")),
                'eps': info.get("trailingEps"),
                'roe': info.get("returnOnEquity"),
                'roce': info.get("returnOnAssets"), # Using ROA as proxy for ROCE
                'operating_margin': info.get("operatingMargins"),
                'net_margin': info.get("profitMargins"),
                'gross_margin': info.get("grossMargins"),
                'de_ratio': info.get("debtToEquity"),
                'current_ratio': info.get("currentRatio"),
                'quick_ratio': info.get("quickRatio"),
                'free_cash_flow': info.get("freeCashflow"),
                'pe_ratio': info.get("trailingPE"),
                'pb_ratio': info.get("priceToBook"),
                'ev_ebitda': info.get("enterpriseToEbitda"),
                'beta': info.get("beta"),
                'dividend_yield': info.get("dividendYield"),
                'liquidity': info.get("averageDailyVolume10Day"),
            })
        # --- Backtest Mode (manually calculate all metrics) ---
        else:
            backtest_date_ts = pd.to_datetime(backtest_date)

            # 1. Filter statements to be before the backtest date
            financials = financials.loc[:, financials.columns < backtest_date_ts]
            balance_sheet = balance_sheet.loc[:, balance_sheet.columns < backtest_date_ts]
            cash_flow = cash_flow.loc[:, cash_flow.columns < backtest_date_ts]
            quarterly_fin = quarterly_fin.loc[:, quarterly_fin.columns < backtest_date_ts]
            quarterly_bal = quarterly_bal.loc[:, quarterly_bal.columns < backtest_date_ts]

            if quarterly_fin.empty or quarterly_bal.empty:
                print(f"  - Warning: Not enough quarterly data for {ticker_symbol} to proceed. Skipping.")
                return {"metrics": create_quantitative_skeleton(), "raw_data": raw_data}

            # 2. Get historical price and volume
            hist_start_date = backtest_date_ts - pd.DateOffset(years=1, days=15)
            hist_data = stock.history(start=hist_start_date, end=backtest_date)
            if hist_data.empty:
                raise ValueError(f"No historical price data for {ticker_symbol} before {backtest_date}")
            
            latest_price = hist_data['Close'].iloc[-1]
            fin_data['stock_price'] = latest_price
            fin_data['liquidity'] = hist_data['Volume'].tail(10).mean()
            
            # 3. Get shares outstanding (a reasonable approximation from live data)
            shares_outstanding = stock.info.get("sharesOutstanding")
            if not shares_outstanding:
                raise ValueError("Shares outstanding not available.")
            
            fin_data['market_cap'] = shares_outstanding * latest_price

            # 4. Conditionally Calculate or Annualize TTM metrics
            q_fin_T = quarterly_fin.T
            q_cash_flow_T = stock.quarterly_cashflow.T # Use unfiltered for TTM calculation
            num_quarters = len(q_fin_T.get('Total Revenue', pd.Series(dtype=float)).dropna())

            if num_quarters > 0:
                # Take available quarters and annualize
                scaling_factor = 4 / num_quarters
                
                ttm_revenue = q_fin_T.get('Total Revenue', pd.Series(dtype=float)).dropna().head(num_quarters).sum() * scaling_factor
                ttm_net_income = q_fin_T.get('Net Income', pd.Series(dtype=float)).dropna().head(num_quarters).sum() * scaling_factor
                ttm_ebit = q_fin_T.get('EBIT', pd.Series(dtype=float)).dropna().head(num_quarters).sum() * scaling_factor
                
                # For cash flow items, use the same logic
                cf_num_quarters = len(q_cash_flow_T.get('Total Cash From Operating Activities', pd.Series(dtype=float)).dropna())
                if cf_num_quarters > 0:
                    cf_scaling_factor = 4 / cf_num_quarters
                    ttm_op_cash_flow = q_cash_flow_T.get('Total Cash From Operating Activities', pd.Series(dtype=float)).dropna().head(cf_num_quarters).sum() * cf_scaling_factor
                    ttm_capex = q_cash_flow_T.get('Capital Expenditure', pd.Series(dtype=float)).dropna().head(cf_num_quarters).sum() * cf_scaling_factor
                    ttm_d_and_a = q_cash_flow_T.get('Depreciation And Amortization', pd.Series(dtype=float)).dropna().head(cf_num_quarters).sum() * cf_scaling_factor
                else:
                    ttm_op_cash_flow, ttm_capex, ttm_d_and_a = [np.nan] * 3
            else:
                # No quarterly data at all, set to NaN
                ttm_revenue, ttm_net_income, ttm_ebit, ttm_op_cash_flow, ttm_capex, ttm_d_and_a = [np.nan] * 6

            # 5. Get latest balance sheet figures from filtered data
            latest_bs = balance_sheet.iloc[:, 0]
            total_debt = latest_bs.get('Total Debt', 0)
            shareholder_equity = latest_bs.get('Stockholders Equity', 0)
            cash_and_equivalents = latest_bs.get('Cash And Cash Equivalents', 0)
            total_current_assets = latest_bs.get('Total Current Assets', 0)
            total_current_liabilities = latest_bs.get('Total Current Liabilities', 0)
            inventory = latest_bs.get('Inventory', 0)

            # 6. Calculate all ratios manually
            if ttm_revenue > 0:
                fin_data['operating_margin'] = ttm_ebit / ttm_revenue if ttm_ebit is not None else np.nan
                fin_data['net_margin'] = ttm_net_income / ttm_revenue if ttm_net_income is not None else np.nan
            
            if shareholder_equity > 0:
                fin_data['roe'] = ttm_net_income / shareholder_equity if ttm_net_income is not None else np.nan
                fin_data['de_ratio'] = total_debt / shareholder_equity if total_debt is not None else np.nan
                fin_data['pb_ratio'] = fin_data['market_cap'] / shareholder_equity if fin_data['market_cap'] is not None else np.nan

            if ttm_net_income is not None and shares_outstanding > 0:
                fin_data['eps'] = ttm_net_income / shares_outstanding
                if fin_data['eps'] > 0 and latest_price is not None:
                    fin_data['pe_ratio'] = latest_price / fin_data['eps']

            if total_current_liabilities > 0:
                fin_data['current_ratio'] = total_current_assets / total_current_liabilities if total_current_assets is not None else np.nan
                fin_data['quick_ratio'] = (total_current_assets - inventory) / total_current_liabilities if total_current_assets is not None and inventory is not None else np.nan

            if ttm_op_cash_flow is not None and ttm_capex is not None:
                fin_data['free_cash_flow'] = ttm_op_cash_flow + ttm_capex # Capex is negative

            # EV/EBITDA
            if fin_data['market_cap'] is not None and total_debt is not None and cash_and_equivalents is not None:
                enterprise_value = fin_data['market_cap'] + total_debt - cash_and_equivalents
                if ttm_ebit is not None and ttm_d_and_a is not None:
                    ebitda = ttm_ebit + ttm_d_and_a
                    if ebitda > 0:
                        fin_data['ev_ebitda'] = enterprise_value / ebitda

            # Beta calculation
            try:
                market_hist = yf.download('^NSEI', start=hist_start_date, end=backtest_date, progress=False)
                market_returns = market_hist['Close'].pct_change().dropna()
                stock_returns = hist_data['Close'].pct_change().dropna()
                
                df_beta = pd.DataFrame({'stock': stock_returns, 'market': market_returns}).dropna()
                
                covariance = df_beta.cov().iloc[0, 1]
                market_variance = df_beta['market'].var()
                
                if market_variance != 0:
                    fin_data['beta'] = covariance / market_variance
            except Exception as e:
                print(f"  Could not calculate Beta: {e}")

            # Dividend Yield
            try:
                dividends_last_year = hist_data['Dividends'][hist_data.index > (backtest_date_ts - pd.DateOffset(years=1))].sum()
                if latest_price > 0 and dividends_last_year > 0:
                    fin_data['dividend_yield'] = dividends_last_year / latest_price
            except Exception as e:
                print(f"  Could not calculate Dividend Yield: {e}")

        # --- Store raw data ---
        raw_data['annual_financials'] = financials
        raw_data['annual_balance_sheet'] = balance_sheet
        raw_data['annual_cash_flow'] = cash_flow
        raw_data['quarterly_financials'] = quarterly_fin
        raw_data['quarterly_balance_sheet'] = quarterly_bal
        
        # Transpose for easier calculations
        financials_T = financials.T
        balance_sheet_T = balance_sheet.T
        cash_flow_T = cash_flow.T
        quarterly_fin_T = quarterly_fin.T
        
        # --- Annual Metrics (for growth rates) ---
        if not financials_T.empty:
            revenue_hist = financials_T.get('Total Revenue', pd.Series(dtype=float)).dropna()
            profit_hist = financials_T.get('Net Income', pd.Series(dtype=float)).dropna()
            eps_hist = financials_T.get('Basic EPS', pd.Series(dtype=float)).dropna()
            
            if not revenue_hist.empty:
                fin_data['revenue'] = revenue_hist.iloc[0]
                if len(revenue_hist) > 1:
                    fin_data['revenue_growth_5y'] = cagr(revenue_hist.iloc[0], revenue_hist.iloc[-1], len(revenue_hist) - 1)

            if not profit_hist.empty:
                fin_data['profit'] = profit_hist.iloc[0]
                if len(profit_hist) > 1:
                    fin_data['profit_growth_5y'] = cagr(profit_hist.iloc[0], profit_hist.iloc[-1], len(profit_hist) - 1)
                
            if not eps_hist.empty and len(eps_hist) > 1:
                fin_data['eps_growth_5y'] = cagr(eps_hist.iloc[0], eps_hist.iloc[-1], len(eps_hist) - 1)
            
            if fin_data.get('profit_growth_5y') is not None and fin_data.get('revenue_growth_5y') is not None and fin_data['revenue_growth_5y'] != 0:
                fin_data['operating_leverage'] = fin_data['profit_growth_5y'] / fin_data['revenue_growth_5y']
            
            if fin_data.get('eps_growth_5y', 0) > 0 and fin_data.get('pe_ratio') is not None:
                fin_data['peg_ratio'] = fin_data['pe_ratio'] / (fin_data['eps_growth_5y'] * 100)
                
            # Interest Coverage and Cash Conversion
            if not cash_flow_T.empty and not balance_sheet_T.empty:
                ebit = financials_T.get('EBIT', pd.Series(dtype=float)).dropna()
                interest_expense = financials_T.get('Interest Expense', pd.Series(dtype=float)).abs().dropna()
                op_cash_flow = cash_flow_T.get('Total Cash From Operating Activities', pd.Series(dtype=float)).dropna()

                if not ebit.empty and not interest_expense.empty:
                    latest_ebit = ebit.iloc[0]
                    latest_interest = interest_expense.reindex(ebit.index, method='nearest').iloc[0]
                    if latest_interest != 0:
                        fin_data['interest_coverage'] = latest_ebit / latest_interest
                
                if not op_cash_flow.empty and not profit_hist.empty:
                    latest_op_cash = op_cash_flow.iloc[0]
                    latest_profit = profit_hist.reindex(op_cash_flow.index, method='nearest').iloc[0]
                    if latest_profit != 0:
                        fin_data['cash_conversion_ratio'] = latest_op_cash / latest_profit

        # --- Quarterly Metrics ---
        if not quarterly_fin_T.empty:
            q_revenue = quarterly_fin_T.get('Total Revenue', pd.Series(dtype=float)).dropna()
            q_profit = quarterly_fin_T.get('Net Income', pd.Series(dtype=float)).dropna()
            q_eps = quarterly_fin_T.get('Basic EPS', pd.Series(dtype=float)).dropna()

            if not q_revenue.empty:
                fin_data['quarterly_revenue'] = q_revenue.tolist()
                if len(q_revenue) > 1:
                    fin_data['quarterly_revenue_growth'] = q_revenue.pct_change(fill_method=None).dropna().tolist()
            if not q_profit.empty:
                fin_data['quarterly_profit'] = q_profit.tolist()
                if len(q_profit) > 1:
                    fin_data['quarterly_profit_growth'] = q_profit.pct_change(fill_method=None).dropna().tolist()
            if not q_eps.empty:
                fin_data['quarterly_eps'] = q_eps.tolist()
                if len(q_eps) > 1:
                    fin_data['quarterly_eps_growth'] = q_eps.pct_change(fill_method=None).dropna().tolist()
        
        if not quarterly_bal.T.empty:
            quarterly_bal_T = quarterly_bal.T
            q_debt = quarterly_bal_T.get('Total Debt', pd.Series(dtype=float)).dropna()
            q_equity = quarterly_bal_T.get('Stockholders Equity', pd.Series(dtype=float)).dropna()
            q_current_assets = quarterly_bal_T.get('Total Current Assets', pd.Series(dtype=float)).dropna()
            q_current_liab = quarterly_bal_T.get('Total Current Liabilities', pd.Series(dtype=float)).dropna()
            q_ebit = quarterly_fin_T.get('EBIT', pd.Series(dtype=float)).dropna()
            q_interest = quarterly_fin_T.get('Interest Expense', pd.Series(dtype=float)).abs().dropna()

            common_index = q_debt.index.intersection(q_equity.index)
            if not common_index.empty:
                q_de_ratio = q_debt.loc[common_index] / q_equity.loc[common_index]
                fin_data['quarterly_de_ratio'] = q_de_ratio.replace([np.inf, -np.inf], np.nan).dropna().tolist()
            
            common_index_cr = q_current_assets.index.intersection(q_current_liab.index)
            if not common_index_cr.empty:
                q_current_ratio = q_current_assets.loc[common_index_cr] / q_current_liab.loc[common_index_cr]
                fin_data['quarterly_current_ratio'] = q_current_ratio.replace([np.inf, -np.inf], np.nan).dropna().tolist()
            
            common_index_ic = q_ebit.index.intersection(q_interest.index)
            if not common_index_ic.empty:
                q_interest_coverage = q_ebit.loc[common_index_ic] / q_interest.loc[common_index_ic]
                fin_data['quarterly_interest_coverage'] = q_interest_coverage.replace([np.inf, -np.inf], np.nan).dropna().tolist()

    except Exception as e:
        print(f"An error occurred for ticker {ticker_symbol}: {e}")
        # Return empty structures on error
        return {"metrics": fin_data, "raw_data": raw_data}

    return {"metrics": fin_data, "raw_data": raw_data}
