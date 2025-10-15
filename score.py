import numpy as np

def multibagger_score_two_dim(stock):
    """Calculate a two-dimensional multibagger score based on fundamental factors."""

    factor_scores = {}

    # -------------------- Quantitative Factors --------------------
    quantitative = stock.get('quantitative', {})

    # Helper function to handle NaN values
    def get_val(key, default=np.nan):
        val = quantitative.get(key)
        return default if val is None or np.isnan(val) else val

    # Company Size & Stage
    mc = get_val('market_cap', 0)
    factor_scores['market_cap'] = 5 if mc < 100e9 else 4 if mc < 500e9 else 3 if mc < 2000e9 else 2 if mc < 5000e9 else 0

    sp = get_val('stock_price', 0)
    factor_scores['stock_price'] = 5 if sp < 500 else 4 if sp < 1000 else 3 if sp < 2000 else 0

    # Financial Health
    eps_growth = get_val('eps_growth_5y', 0) * 100  # Convert to percentage
    factor_scores['eps_growth_5y'] = 5 if eps_growth > 20 else 3 if eps_growth > 10 else 0

    roe = get_val('roe', 0) * 100
    factor_scores['roe'] = 5 if roe > 15 else 3 if roe > 10 else 0

    roce = get_val('roce', 0) * 100 # Assuming ROCE is also in decimal
    factor_scores['roce'] = 5 if roce > 15 else 3 if roce > 10 else 0

    op_margin = get_val('operating_margin', 0) * 100
    factor_scores['operating_margin'] = 5 if op_margin > 20 else 3 if op_margin > 10 else 0

    net_margin = get_val('net_margin', 0) * 100
    factor_scores['net_margin'] = 5 if net_margin > 15 else 3 if net_margin > 7 else 0

    de_ratio = get_val('de_ratio', np.inf)
    factor_scores['de_ratio'] = 5 if de_ratio < 0.5 else 3 if de_ratio < 1 else 0

    interest_coverage = get_val('interest_coverage', 0)
    factor_scores['interest_coverage'] = 5 if interest_coverage > 3 else 3 if interest_coverage > 1.5 else 0

    current_ratio = get_val('current_ratio', 0)
    factor_scores['current_ratio'] = 5 if current_ratio > 1.5 else 3 if current_ratio > 1.2 else 0

    free_cash_flow = get_val('free_cash_flow', 0)
    factor_scores['free_cash_flow'] = 5 if free_cash_flow > 0 else 0

    revenue_growth = get_val('revenue_growth_5y', 0) * 100
    factor_scores['revenue_growth_5y'] = 5 if revenue_growth > 15 else 3 if revenue_growth > 10 else 0

    profit_growth = get_val('profit_growth_5y', 0) * 100
    factor_scores['profit_growth_5y'] = 5 if profit_growth > 20 else 3 if profit_growth > 10 else 0

    # TODO: Add logic to populate 'cash_flow_growth' in the fetch_quantitative_data function
    cash_flow_growth = get_val('cash_flow_growth', 0)
    factor_scores['cash_flow_growth'] = 5 if cash_flow_growth > 15 else 3 if cash_flow_growth > 10 else 0

    pe = get_val('pe_ratio', np.inf)
    factor_scores['pe_ratio'] = 5 if pe < 15 else 3 if pe < 25 else 0

    peg = get_val('peg_ratio', np.inf)
    factor_scores['peg_ratio'] = 5 if peg < 1 else 3 if peg < 2 else 0

    pb = get_val('pb_ratio', np.inf)
    factor_scores['pb_ratio'] = 5 if pb < 1.5 else 3 if pb < 2.5 else 0

    ev_ebitda = get_val('ev_ebitda', np.inf)
    factor_scores['ev_ebitda'] = 5 if ev_ebitda < 10 else 3 if ev_ebitda < 15 else 0

    liquidity = get_val('liquidity', 0)
    factor_scores['liquidity'] = 5 if liquidity > 100000 else 3 if liquidity > 50000 else 0  # Example threshold

    promoter_holding = get_val('promoter_holding', 0)
    factor_scores['promoter_holding'] = 5 if promoter_holding > 50 else 3 if promoter_holding > 30 else 0
    
    promoter_holding_growth = get_val('promoter_holding_growth', 0)
    factor_scores['promoter_holding_growth'] = 5 if promoter_holding_growth > 0 else 3 if promoter_holding_growth == 0 else 0

    # -------------------- Qualitative Factors --------------------
    qualitative = stock.get('qualitative', {})
    for factor, value in qualitative.items():
        # assume already 0-5 score
        factor_scores[factor] = value

    # Total Score
    total_score = sum(factor_scores.values())
    max_possible_score = len(factor_scores) * 5
    percentage_score = (total_score / max_possible_score) * 100 if max_possible_score > 0 else 0
    return total_score, percentage_score, factor_scores
