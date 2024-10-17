import csv
import json
import requests
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

def create_weather_alerts_snowflake():
    BASE_URL = os.getenv('API_BASE_URL')
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
        if geometry:
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
            'coordinates': json.dumps(coordinates)
        }
        
        alerts.append(alert)

    # Création d'un DataFrame pandas
    df = pd.DataFrame(alerts)

    # Connexion à Snowflake
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

    try:
        # Création de la table si elle n'existe pas
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_alerts (
                id STRING,
                areaDesc STRING,
                SAME_codes STRING,
                UGC_codes STRING,
                affectedZones STRING,
                sent TIMESTAMP_NTZ,
                effective TIMESTAMP_NTZ,
                onset TIMESTAMP_NTZ,
                expires TIMESTAMP_NTZ,
                ends TIMESTAMP_NTZ,
                status STRING,
                messageType STRING,
                category STRING,
                severity STRING,
                certainty STRING,
                urgency STRING,
                event STRING,
                headline STRING,
                coordinates STRING
            )
            """)

        # Insertion des données dans Snowflake
        success, nchunks, nrows, _ = write_pandas(conn, df, 'WEATHER_ALERTS')
        
        print(f"Données insérées avec succès dans Snowflake. {nrows} lignes insérées.")

    finally:
        conn.close()

if __name__ == "__main__":
    create_weather_alerts_snowflake()