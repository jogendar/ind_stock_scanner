import numpy as np

def create_quantitative_skeleton():
    """Creates a dictionary to hold financial metrics."""
    skeleton = {
        "market_cap": np.nan,
        "stock_price": np.nan,
        "eps": np.nan,
        "eps_growth_5y": np.nan,
        "roe": np.nan,
        "roce": np.nan,  # Note: ROCE is not directly available, using ROE as a proxy
        "operating_margin": np.nan,
        "net_margin": np.nan,
        "gross_margin": np.nan,
        "de_ratio": np.nan,
        "interest_coverage": np.nan,
        "current_ratio": np.nan,
        "quick_ratio": np.nan,
        "free_cash_flow": np.nan,
        "cash_conversion_ratio": np.nan,
        "revenue": np.nan,
        "revenue_growth_5y": np.nan,
        "profit": np.nan,
        "profit_growth_5y": np.nan,
        "operating_leverage": np.nan,
        "pe_ratio": np.nan,
        "peg_ratio": np.nan,
        "pb_ratio": np.nan,
        "ev_ebitda": np.nan,
        "beta": np.nan,
        "dividend_yield": np.nan,
        "liquidity": np.nan,
        "promoter_holding": np.nan,
        "promoter_holding_growth": np.nan,
        # Quarterly trends will be lists
        "quarterly_revenue": [],
        "quarterly_profit": [],
        "quarterly_eps": [],
        "quarterly_revenue_growth": [],
        "quarterly_profit_growth": [],
        "quarterly_eps_growth": [],
        "quarterly_de_ratio": [],
        "quarterly_current_ratio": [],
        "quarterly_interest_coverage": [],
    }
    return skeleton

def cagr(end, start, periods):
    """Helper function to calculate Compound Annual Growth Rate."""
    if start is None or end is None or start == 0 or periods <= 0:
        return np.nan
    return (end / start) ** (1 / periods) - 1
