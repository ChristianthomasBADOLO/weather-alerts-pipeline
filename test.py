import requests
import csv
from datetime import datetime
import os
from dotenv import load_dotenv
def get_weather_alerts_to_csv(api_key, lat, lon, output_file):
    # Construire l'URL de l'API
    base_url = "https://api.weatherbit.io/v2.0/alerts"
    params = {
        "lat": lat,
        "lon": lon,
        "key": api_key
    }

    try:
        # Faire la requête à l'API
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Lève une exception pour les codes d'erreur HTTP
        data = response.json()

        # Préparer les données pour le CSV
        csv_data = []
        for alert in data.get("alerts", []):
            csv_data.append({
                "city_name": data.get("city_name"),
                "country_code": data.get("country_code"),
                "state_code": data.get("state_code"),
                "lat": data.get("lat"),
                "lon": data.get("lon"),
                "timezone": data.get("timezone"),
                "title": alert.get("title"),
                "description": alert.get("description"),
                "severity": alert.get("severity"),
                "effective_utc": alert.get("effective_utc"),
                "expires_utc": alert.get("expires_utc"),
                "regions": ", ".join(alert.get("regions", []))
            })

        # Écrire les données dans un fichier CSV
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            if csv_data:
                writer = csv.DictWriter(file, fieldnames=csv_data[0].keys())
                writer.writeheader()
                writer.writerows(csv_data)

        print(f"Les données ont été enregistrées dans {output_file}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la requête à l'API: {e}")
        return False
    except IOError as e:
        print(f"Erreur lors de l'écriture du fichier: {e}")
        return False
    except Exception as e:
        print(f"Une erreur inattendue s'est produite: {e}")
        return False

# Exemple d'utilisation
api_key = os.getenv("api_weatherbit")
latitude = 28.5384
longitude = -81.3789
output_file = os.path.join(os.path.expanduser("~"), "Desktop", f"weather_alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

success = get_weather_alerts_to_csv(api_key, latitude, longitude, output_file)
if success:
    print("Le fichier CSV a été créé avec succès.")
else:
    print("Il y a eu un problème lors de la création du fichier CSV.")