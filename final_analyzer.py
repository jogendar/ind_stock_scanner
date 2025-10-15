import json
import csv
import os
from openai import OpenAI

# Securely get the API key from an environment variable
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("OpenAI API key not found. Please set the OPENAI_API_KEY environment variable.")

client = OpenAI(api_key=api_key)

def get_analysis_for_stock(stock_data):
    """
    Analyzes a single stock's data to determine its multibagger potential.
    """
    prompt = f"""
    You are a stock analysis AI.
    Analyze the following company data and score its multibagger potential (0-100).
    {stock_data}

    Output JSON:
    {{
        "score": number,
        "verdict": "Buy / Hold / Avoid",
        "reasoning": "Brief explanation"
    }}
    """
    try:
        # Using the model previously mentioned
        response = client.chat.completions.create(
            model="gpt-5-nano",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"An error occurred while analyzing {stock_data.get('Symbol', 'Unknown')}: {e}")
        return None

def multibagger_analysis(stock_data_array):
    """
    Takes an array of stock data, analyzes each, prints the result, and saves to a CSV file.
    """
    results = []
    for stock_data in stock_data_array:
        symbol = stock_data.get('Symbol', 'Unknown')
        print(f"Analyzing {symbol}...")
        analysis_json_str = get_analysis_for_stock(stock_data)
        if analysis_json_str:
            try:
                analysis_data = json.loads(analysis_json_str)
                result = {
                    'Symbol': stock_data.get('Symbol', 'N/A'),
                    'Score': analysis_data.get('score', 'N/A'),
                    'Verdict': analysis_data.get('verdict', 'N/A'),
                    'Reasoning': analysis_data.get('reasoning', 'N/A')
                }
                results.append(result)
                # Print output for each stock
                print(f"  - Symbol: {result['Symbol']}")
                print(f"  - Score: {result['Score']}")
                print(f"  - Verdict: {result['Verdict']}")
                print(f"  - Reasoning: {result['Reasoning']}")
            except json.JSONDecodeError:
                print(f"Could not decode JSON for {symbol}")
                print(f"Raw response: {analysis_json_str}")

    # Save results to CSV
    if results:
        with open('analysis_results.csv', 'w', newline='') as csvfile:
            fieldnames = ['Symbol', 'Score', 'Verdict', 'Reasoning']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(results)
        print("\nAnalysis complete. Results saved to analysis_results.csv")
    else:
        print("\nNo analysis was performed.")

if __name__ == "__main__":
    try:
        with open('penny_stock_scores.json', 'r') as f:
            all_stocks = json.load(f)
        multibagger_analysis(all_stocks)
    except FileNotFoundError:
        print("Error: 'penny_stock_scores.json' not found.")
    except json.JSONDecodeError:
        print("Error: Could not decode JSON from 'penny_stock_scores.json'.")
