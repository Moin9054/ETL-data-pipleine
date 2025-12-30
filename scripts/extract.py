import os
import json
import requests
from datetime import datetime

TMDB_BASE_URL = "https://api.themoviedb.org/3"
API_KEY = os.environ.get("TMDB_API_KEY")
RAW_DATA_PATH = "/opt/airflow/data/raw"

def get_api_key():
    api_key = os.environ.get("TMDB_API_KEY")
    if not api_key or api_key == "your_tmdb_api_key_here":
        raise ValueError("TMDB_API_KEY not set. Please add your API key to .env file")
    return api_key

def fetch_movies(endpoint, params=None):
    api_key = get_api_key()
    url = f"{TMDB_BASE_URL}/{endpoint}"
    default_params = {"api_key": api_key, "language": "en-US"}
    if params:
        default_params.update(params)
    
    all_movies = []
    for page in range(1, 6):
        default_params["page"] = page
        response = requests.get(url, params=default_params)
        response.raise_for_status()
        data = response.json()
        all_movies.extend(data.get("results", []))
    
    return all_movies

def extract_popular_movies():
    movies = fetch_movies("movie/popular")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = f"{RAW_DATA_PATH}/popular_movies_{timestamp}.json"
    
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(movies, f)
    
    return filepath

def extract_top_rated_movies():
    movies = fetch_movies("movie/top_rated")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = f"{RAW_DATA_PATH}/top_rated_movies_{timestamp}.json"
    
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(movies, f)
    
    return filepath

def extract_trending_movies():
    movies = fetch_movies("trending/movie/week")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = f"{RAW_DATA_PATH}/trending_movies_{timestamp}.json"
    
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(movies, f)
    
    return filepath

def extract_genres():
    api_key = get_api_key()
    url = f"{TMDB_BASE_URL}/genre/movie/list"
    response = requests.get(url, params={"api_key": api_key, "language": "en-US"})
    response.raise_for_status()
    genres = response.json().get("genres", [])
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = f"{RAW_DATA_PATH}/genres_{timestamp}.json"
    
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(genres, f)
    
    return filepath
