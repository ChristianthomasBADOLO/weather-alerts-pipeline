import csv
import json
from datetime import datetime

def clean_date(date_string):
    if date_string:
        try:
            return datetime.fromisoformat(date_string.replace('Z', '+00:00')).isoformat()
        except ValueError:
            return ''
    return ''

def is_valid_coordinates(coord_string):
    if not coord_string:
        return False
    try:
        coords = json.loads(coord_string)
        return isinstance(coords, list) and len(coords) > 0 and all(isinstance(c, list) and len(c) == 2 for c in coords)
    except json.JSONDecodeError:
        return False

def clean_csv(input_file, output_file, selected_columns):
    with open(input_file, 'r', newline='') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        
        fieldnames = [col for col in selected_columns if col in reader.fieldnames]
        
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            # Ne traiter que les lignes avec des coordonnées valides
            if is_valid_coordinates(row.get('coordinates', '')):
                cleaned_row = {}
                for field in fieldnames:
                    if field in ['sent', 'effective', 'expires']:
                        cleaned_row[field] = clean_date(row[field])
                    elif field in ['SAME_codes', 'UGC_codes', 'affectedZones']:
                        cleaned_row[field] = ','.join(filter(None, row[field].split(',')))
                    elif field == 'coordinates':
                        coords = json.loads(row[field])
                        cleaned_row[field] = json.dumps(coords)
                    else:
                        cleaned_row[field] = row[field]
                
                writer.writerow(cleaned_row)

    print(f"Fichier nettoyé sauvegardé sous : {output_file}")

# Colonnes à conserver
selected_columns = [
    'id', 'event', 'severity', 'areaDesc', 'sent', 'effective', 'expires',
    'status', 'messageType', 'category', 'urgency', 'certainty',
    'SAME_codes', 'UGC_codes', 'coordinates'
]

# Utilisation du script
input_file = 'weather_alerts_20241014_111433.csv'  # Votre fichier CSV d'origine
output_file = 'cleaned_weather_data.csv'  # Le nouveau fichier CSV nettoyé
clean_csv(input_file, output_file, selected_columns)