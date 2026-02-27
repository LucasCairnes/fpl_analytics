import requests
import json
import pandas as pd

def fetch_fpl_data():
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        filename = "fpl_raw_data.json"
        
        with open(filename, "w") as file:
            json.dump(data, file)
        
        print(f"Data fetched and saved to {filename}")

        players_df = pd.DataFrame(data['elements'])
        print(players_df.head())

    else:
        print(f"Failed to fetch data. Error code: {response.status_code}")

if __name__ == "__main__":
    fetch_fpl_data()