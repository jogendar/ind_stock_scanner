import csv
import json

def convert_csv_to_json(csv_file_path, json_file_path):
    data = []
    with open(csv_file_path, 'r') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        
        # Identify columns to remove
        columns_to_remove = ['Conclusion']
        for header in headers:
            if header.strip().endswith('(s)'):
                columns_to_remove.append(header)

        # Get indices of columns to remove
        indices_to_remove = {headers.index(col) for col in columns_to_remove if col in headers}
        
        # Filter headers
        filtered_headers = [header for i, header in enumerate(headers) if i not in indices_to_remove]

        for row in csv_reader:
            row_data = {}
            filtered_row = [cell for i, cell in enumerate(row) if i not in indices_to_remove]
            
            for i, header in enumerate(filtered_headers):
                # Try to convert to number if possible
                try:
                    row_data[header] = float(filtered_row[i])
                except (ValueError, IndexError):
                    row_data[header] = filtered_row[i] if i < len(filtered_row) else None
            
            data.append(row_data)

    with open(json_file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)

# File paths
csv_file = 'penny_stock_scores.csv'
json_file = 'penny_stock_scores.json'

# Convert the CSV to JSON
convert_csv_to_json(csv_file, json_file)

print(f'Successfully converted {csv_file} to {json_file}')
