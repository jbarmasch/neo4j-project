import requests
import json
import os
from config import OMDB_API_KEYS, IMDB_RATINGS_JSON

def fetch_imdb_rating(imdb_id, api_key):
    """Fetch the IMDb rating for a movie using the OMDb API."""
    url = f"http://www.omdbapi.com/"
    params = {"apikey": api_key, "i": f"tt{imdb_id}"}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if data.get("Response") == "True":
            return data.get("imdbRating")
    return None

def load_ratings_from_json():
    """Load existing IMDb ratings from the JSON file."""
    try:
        with open(IMDB_RATINGS_JSON, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}
    
def load_existing_ratings(filename):
    """Load existing ratings from the JSON file or return an empty dictionary if the file doesn't exist."""
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f)
    return {}

def append_to_json_file(filename, new_data):
    """Append new data to the JSON file without overwriting."""
    existing_data = load_existing_ratings(filename)
    existing_data.update(new_data)
    with open(filename, "w") as f:
        json.dump(existing_data, f, indent=4)

def save_ratings_to_json(ratings):
    """Save IMDb ratings to the JSON file."""
    with open(IMDB_RATINGS_JSON, "w") as file:
        json.dump(ratings, file, indent=4)
