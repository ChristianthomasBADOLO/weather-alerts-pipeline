import pandas as pd
import json
import requests

def create_weather_alerts_csv():
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
        geometry = feature.get('geometry')
        
        # Extraction des coordonnées si la géométrie n'est pas nulle
        coordinates = []
        if geometry :
            coordinates = geometry['coordinates'][0]
        
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
            'coordinates': json.dumps(coordinates)  # Stockage des coordonnées en tant que chaîne JSON
        }
        
        alerts.append(alert)

    # Création du DataFrame
    df = pd.DataFrame(alerts)
    
    return df