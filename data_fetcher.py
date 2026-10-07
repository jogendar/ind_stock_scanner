import yfinance as yf
import pandas as pd
import numpy as np
import time
from curl_cffi.requests import Session
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from utils import create_quantitative_skeleton, cagr


SCREENER_TIMEOUT_SECONDS = 6
SCREENER_FAILURE_LIMIT = 3
SCREENER_COOLDOWN_SECONDS = 300

_screener_consecutive_request_failures = 0
_screener_retry_after = 0.0


def _reset_screener_circuit_breaker():
    """Reset process-local Screener availability state (primarily for tests)."""
    global _screener_consecutive_request_failures, _screener_retry_after
    _screener_consecutive_request_failures = 0
    _screener_retry_after = 0.0


def _screener_request_is_paused():
    """Return True while repeated request failures are in their cooldown."""
    global _screener_retry_after
    if not _screener_retry_after:
        return False
    if time.monotonic() < _screener_retry_after:
        return True
    _screener_retry_after = 0.0
    return False


def _record_screener_request_failure():
    global _screener_consecutive_request_failures, _screener_retry_after
    _screener_consecutive_request_failures += 1
    if _screener_consecutive_request_failures < SCREENER_FAILURE_LIMIT:
        return
    _screener_consecutive_request_failures = 0
    _screener_retry_after = time.monotonic() + SCREENER_COOLDOWN_SECONDS
    print(
        "Screener.in is unavailable; pausing promoter requests for "
        f"{SCREENER_COOLDOWN_SECONDS // 60} minutes."
    )


def _record_screener_request_success():
    global _screener_consecutive_request_failures, _screener_retry_after
    _screener_consecutive_request_failures = 0
    _screener_retry_after = 0.0


def first_available_series(frame, names):
    """Return the first populated Yahoo statement row from ``names``."""
    for name in names:
        series = frame.get(name, pd.Series(dtype=float)).dropna()
        if not series.empty:
            return series
    return pd.Series(dtype=float)

def fetch_data_from_screener(ticker, session):
    """Fetches promoter holding data from Screener.in."""
    screener_data = {
        "promoter_holding": np.nan,
        "promoter_holding_growth": np.nan,
    }
    if _screener_request_is_paused():
        return screener_data

    base_ticker = ticker.replace('.NS', '')
    screener_url = f"https://www.screener.in/company/{base_ticker}/consolidated/"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    try:
        response = session.get(
            screener_url,
            headers=headers,
            impersonate="chrome110",
            verify=False,
            timeout=SCREENER_TIMEOUT_SECONDS,
        )
    except Exception as e:
        print(f"Could not fetch promoter data for {ticker} from Screener.in: {e}")
        _record_screener_request_failure()
        return screener_data

    try:
        response.raise_for_status()
    except Exception as e:
        print(f"Could not fetch promoter data for {ticker} from Screener.in: {e}")
        status_code = getattr(response, "status_code", None)
        if status_code in (403, 429) or (
            isinstance(status_code, int) and status_code >= 500
        ):
            _record_screener_request_failure()
        else:
            _record_screener_request_success()
        return screener_data

    _record_screener_request_success()
    try:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the shareholding pattern section
        shareholding_section = soup.find('section', id='shareholding')
        if not shareholding_section:
            raise ValueError("Shareholding section not found on Screener.in")

        # Find the table within the section
        table = shareholding_section.find('table')
        if not table:
            raise ValueError("Shareholding table not found on Screener.in")

        # Find the row for Promoter Holding
        promoter_row = None
        for row in table.find_all('tr'):
            first_cell = row.find('td')
            if first_cell and 'promoter' in first_cell.get_text(strip=True).lower():
                promoter_row = row
                break
        
        if not promoter_row:
            raise ValueError("Promoter holding row not found in the table.")

        data_cells = promoter_row.find_all('td')
        
        # The last few cells contain the holding percentages
        holdings = []
        for cell in data_cells[1:]: # Skip the label cell
            try:
                # Clean up text (e.g., '73.30 %') and convert to float
                value_text = cell.text.strip().replace('%', '').strip()
                if value_text:
                    holdings.append(float(value_text))
            except (ValueError, TypeError):
                continue

        if len(holdings) >= 2:
            # Last value is the most recent quarter
            screener_data['promoter_holding'] = holdings[-1]
            # Growth is the difference between the last two quarters
            screener_data['promoter_holding_growth'] = holdings[-1] - holdings[-2]
        elif len(holdings) == 1:
            screener_data['promoter_holding'] = holdings[0]
            
    except Exception as e:
        print(f"Could not fetch promoter data for {ticker} from Screener.in: {e}")

    return screener_data

def fetch_quantitative_data(ticker_symbol, period="5y"):
    """
    Fetches and calculates a comprehensive set of quantitative financial metrics for a given stock ticker.
    """
    fin_data = create_quantitative_skeleton()
    raw_data = {}
    try:
        session = Session(impersonate="chrome110")
        session.verify = False # Explicitly disable SSL verification for the session
        stock = yf.Ticker(ticker_symbol, session=session)
        info = stock.info

        # --- Basic Info & Screener Data ---
        screener_data = fetch_data_from_screener(ticker_symbol, session)
        fin_data.update(screener_data)
        
        fin_data['market_cap'] = info.get("marketCap", np.nan)
        fin_data['stock_price'] = info.get("regularMarketPrice", info.get("currentPrice", np.nan))
        fin_data['eps'] = info.get("trailingEps", np.nan)
        fin_data['roe'] = info.get("returnOnEquity", np.nan)
        # Keep return on assets as a fallback.  A true EBIT/capital-employed
        # calculation replaces it below when the statements contain the
        # necessary rows.
        fin_data['roce'] = info.get("returnOnAssets", np.nan)
        fin_data['operating_margin'] = info.get("operatingMargins", np.nan)
        fin_data['net_margin'] = info.get("profitMargins", np.nan)
        fin_data['gross_margin'] = info.get("grossMargins", np.nan)
        fin_data['de_ratio'] = info.get("debtToEquity", np.nan)
        fin_data['current_ratio'] = info.get("currentRatio", np.nan)
        fin_data['quick_ratio'] = info.get("quickRatio", np.nan)
        fin_data['free_cash_flow'] = info.get("freeCashflow", np.nan)
        fin_data['pe_ratio'] = info.get("trailingPE", np.nan)
        fin_data['pb_ratio'] = info.get("priceToBook", np.nan)
        fin_data['ev_ebitda'] = info.get("enterpriseToEbitda", np.nan)
        fin_data['beta'] = info.get("beta", np.nan)
        fin_data['dividend_yield'] = info.get("dividendYield", np.nan)
        fin_data['liquidity'] = info.get("averageDailyVolume10Day", np.nan)

        # --- Historical Data ---
        financials = stock.financials
        balance_sheet = stock.balance_sheet
        cash_flow = stock.cashflow
        quarterly_fin = stock.quarterly_financials
        quarterly_bal = stock.quarterly_balance_sheet
        
        # Store raw data
        raw_data['annual_financials'] = financials
        raw_data['annual_balance_sheet'] = balance_sheet
        raw_data['annual_cash_flow'] = cash_flow
        raw_data['quarterly_financials'] = quarterly_fin
        raw_data['quarterly_balance_sheet'] = quarterly_bal
        
        # Transpose for easier calculations
        financials = financials.T.sort_index(ascending=False)
        balance_sheet = balance_sheet.T.sort_index(ascending=False)
        cash_flow = cash_flow.T.sort_index(ascending=False)
        quarterly_fin = quarterly_fin.T.sort_index(ascending=False)
        quarterly_bal = quarterly_bal.T.sort_index(ascending=False)
        
        # --- Annual Metrics ---
        if not financials.empty and not balance_sheet.empty and not cash_flow.empty:
            revenue_hist = financials.get('Total Revenue', pd.Series(dtype=float)).dropna()
            profit_hist = financials.get('Net Income', pd.Series(dtype=float)).dropna()
            eps_hist = financials.get('Basic EPS', pd.Series(dtype=float)).dropna()
            ebit = financials.get('EBIT', pd.Series(dtype=float)).dropna()
            interest_expense = financials.get('Interest Expense', pd.Series(dtype=float)).abs().dropna()
            op_cash_flow = first_available_series(
                cash_flow,
                ('Operating Cash Flow', 'Total Cash From Operating Activities'),
            )

            total_assets = first_available_series(balance_sheet, ('Total Assets',))
            current_liabilities = first_available_series(
                balance_sheet,
                ('Current Liabilities', 'Total Current Liabilities'),
            )
            capital_dates = ebit.index.intersection(total_assets.index).intersection(
                current_liabilities.index
            )
            if not capital_dates.empty:
                latest_date = capital_dates.max()
                capital_employed = (
                    total_assets.loc[latest_date] - current_liabilities.loc[latest_date]
                )
                if pd.notna(capital_employed) and capital_employed > 0:
                    fin_data['roce'] = ebit.loc[latest_date] / capital_employed
            
            if not revenue_hist.empty:
                fin_data['revenue'] = revenue_hist.iloc[0]
                fin_data['revenue_growth_5y'] = cagr(revenue_hist.iloc[0], revenue_hist.iloc[-1], len(revenue_hist) - 1)

            if not profit_hist.empty:
                fin_data['profit'] = profit_hist.iloc[0]
                fin_data['profit_growth_5y'] = cagr(profit_hist.iloc[0], profit_hist.iloc[-1], len(profit_hist) - 1)
                
            if not eps_hist.empty and len(eps_hist) > 1:
                fin_data['eps_growth_5y'] = cagr(eps_hist.iloc[0], eps_hist.iloc[-1], len(eps_hist) - 1)
            
            if fin_data['profit_growth_5y'] is not None and fin_data['revenue_growth_5y'] is not None and fin_data['revenue_growth_5y'] != 0:
                fin_data['operating_leverage'] = fin_data['profit_growth_5y'] / fin_data['revenue_growth_5y']
            
            if fin_data['eps_growth_5y'] is not None and fin_data['eps_growth_5y'] > 0:
                fin_data['peg_ratio'] = fin_data['pe_ratio'] / (fin_data['eps_growth_5y'] * 100)
                
            if not ebit.empty and not interest_expense.empty:
                latest_ebit = ebit.iloc[0]
                latest_interest = interest_expense.reindex(ebit.index, method='nearest').iloc[0]
                if latest_interest != 0:
                    fin_data['interest_coverage'] = latest_ebit / latest_interest
            
            if not op_cash_flow.empty and not profit_hist.empty:
                latest_op_cash = op_cash_flow.iloc[0]
                latest_profit = profit_hist.iloc[0]
                if latest_profit != 0:
                    fin_data['cash_conversion_ratio'] = latest_op_cash / latest_profit

        # --- Quarterly Metrics ---
        if not quarterly_fin.empty and not quarterly_bal.empty:
            q_revenue = quarterly_fin.get('Total Revenue', pd.Series(dtype=float)).dropna()
            q_profit = quarterly_fin.get('Net Income', pd.Series(dtype=float)).dropna()
            q_eps = quarterly_fin.get('Basic EPS', pd.Series(dtype=float)).dropna()
            q_debt = quarterly_bal.get('Total Debt', pd.Series(dtype=float)).dropna()
            q_equity = quarterly_bal.get('Stockholders Equity', pd.Series(dtype=float)).dropna()
            q_current_assets = first_available_series(
                quarterly_bal,
                ('Current Assets', 'Total Current Assets'),
            )
            q_current_liab = first_available_series(
                quarterly_bal,
                ('Current Liabilities', 'Total Current Liabilities'),
            )
            q_ebit = quarterly_fin.get('EBIT', pd.Series(dtype=float)).dropna()
            q_interest = quarterly_fin.get('Interest Expense', pd.Series(dtype=float)).abs().dropna()

            if not q_revenue.empty:
                fin_data['quarterly_revenue'] = q_revenue.tolist()
                fin_data['quarterly_revenue_growth'] = q_revenue.pct_change(fill_method=None).dropna().tolist()
            if not q_profit.empty:
                fin_data['quarterly_profit'] = q_profit.tolist()
                fin_data['quarterly_profit_growth'] = q_profit.pct_change(fill_method=None).dropna().tolist()
            if not q_eps.empty:
                fin_data['quarterly_eps'] = q_eps.tolist()
                fin_data['quarterly_eps_growth'] = q_eps.pct_change(fill_method=None).dropna().tolist()

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
