import csv
import json
import requests
from datetime import datetime

def create_weather_alerts_csv(filename=None):
    BASE_URL = "https://api.weather.gov"
    alerts_url = f"{BASE_URL}/alerts/active"

    # Récupération des alertes
    response = requests.get(alerts_url)
    if response.status_code != 200:
        print(f"Erreur lors de la récupération des alertes: {response.status_code}")
        return None

    data = response.json()

    # Traitement des alertes
    alerts = []
    for feature in data['features']:
        properties = feature['properties']
        alert = {
            'id': properties['id'],
            'areaDesc': properties['areaDesc'],
            'SAME_codes': ','.join(properties.get('geocode', {}).get('SAME', [])),
            'UGC_codes': ','.join(properties.get('geocode', {}).get('UGC', [])),
            'affectedZones': ','.join(properties.get('affectedZones', [])),
            'sent': properties['sent'],
            'effective': properties['effective'],
            'onset': properties['onset'],
            'expires': properties['expires'],
            'ends': properties['ends'],
            'status': properties['status'],
            'messageType': properties['messageType'],
            'category': properties['category'],
            'severity': properties['severity'],
            'certainty': properties['certainty'],
            'urgency': properties['urgency'],
            'event': properties['event'],
            'headline': properties['headline'],
        }
        alerts.append(alert)

    # Génération du nom de fichier si non fourni
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"weather_alerts_{timestamp}.csv"

    # Sauvegarde dans un fichier CSV
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = alerts[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for alert in alerts:
            writer.writerow(alert)

    print(f"Les alertes ont été enregistrées dans {filename}")
    return filename

