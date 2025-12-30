import os
import json
import pandas as pd
from datetime import datetime

PROCESSED_DATA_PATH = "/opt/airflow/data/processed"

def load_json_file(filepath):
    with open(filepath, "r") as f:
        return json.load(f)

def transform_movies(popular_path, top_rated_path, trending_path, genres_path):
    popular = load_json_file(popular_path)
    top_rated = load_json_file(top_rated_path)
    trending = load_json_file(trending_path)
    genres_list = load_json_file(genres_path)
    
    genre_map = {g["id"]: g["name"] for g in genres_list}
    
    all_movies = []
    seen_ids = set()
    
    for movie in popular:
        movie["category"] = "popular"
        if movie["id"] not in seen_ids:
            all_movies.append(movie)
            seen_ids.add(movie["id"])
    
    for movie in top_rated:
        if movie["id"] not in seen_ids:
            movie["category"] = "top_rated"
            all_movies.append(movie)
            seen_ids.add(movie["id"])
        else:
            for m in all_movies:
                if m["id"] == movie["id"]:
                    m["category"] = "popular,top_rated"
                    break
    
    for movie in trending:
        if movie["id"] not in seen_ids:
            movie["category"] = "trending"
            all_movies.append(movie)
            seen_ids.add(movie["id"])
        else:
            for m in all_movies:
                if m["id"] == movie["id"]:
                    if "trending" not in m["category"]:
                        m["category"] += ",trending"
                    break
    
    df = pd.DataFrame(all_movies)
    
    columns_to_keep = [
        "id", "title", "original_title", "overview", "release_date",
        "popularity", "vote_average", "vote_count", "genre_ids",
        "original_language", "adult", "poster_path", "backdrop_path", "category"
    ]
    
    df = df[[col for col in columns_to_keep if col in df.columns]]
    
    df["genre_names"] = df["genre_ids"].apply(
        lambda ids: ",".join([genre_map.get(gid, "Unknown") for gid in ids]) if isinstance(ids, list) else ""
    )
    
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    df["release_year"] = df["release_date"].dt.year
    
    df["vote_average"] = pd.to_numeric(df["vote_average"], errors="coerce").fillna(0)
    df["vote_count"] = pd.to_numeric(df["vote_count"], errors="coerce").fillna(0)
    df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce").fillna(0)
    
    df["rating_category"] = pd.cut(
        df["vote_average"],
        bins=[0, 4, 6, 7.5, 10],
        labels=["Poor", "Average", "Good", "Excellent"]
    )
    
    df["popularity_rank"] = df["popularity"].rank(ascending=False, method="dense").astype(int)
    
    df["poster_url"] = df["poster_path"].apply(
        lambda x: f"https://image.tmdb.org/t/p/w500{x}" if pd.notna(x) else None
    )
    
    df["extracted_at"] = datetime.now().isoformat()
    
    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    movies_filepath = f"{PROCESSED_DATA_PATH}/movies_{timestamp}.csv"
    df.to_csv(movies_filepath, index=False)
    
    genres_df = pd.DataFrame(genres_list)
    genres_filepath = f"{PROCESSED_DATA_PATH}/genres_{timestamp}.csv"
    genres_df.to_csv(genres_filepath, index=False)
    
    return {"movies_path": movies_filepath, "genres_path": genres_filepath}

def generate_stats(movies_path):
    df = pd.read_csv(movies_path)
    
    stats = {
        "total_movies": len(df),
        "avg_rating": round(df["vote_average"].mean(), 2),
        "avg_popularity": round(df["popularity"].mean(), 2),
        "total_votes": int(df["vote_count"].sum()),
        "movies_by_category": df["category"].value_counts().to_dict(),
        "movies_by_rating": df["rating_category"].value_counts().to_dict(),
        "top_genres": {},
        "top_languages": df["original_language"].value_counts().head(5).to_dict(),
        "generated_at": datetime.now().isoformat()
    }
    
    all_genres = []
    for genres in df["genre_names"].dropna():
        all_genres.extend(genres.split(","))
    genre_counts = pd.Series(all_genres).value_counts().head(10).to_dict()
    stats["top_genres"] = genre_counts
    
    top_10_movies = df.nlargest(10, "vote_average")[["title", "vote_average", "release_year"]].to_dict("records")
    stats["top_10_rated_movies"] = top_10_movies
    
    most_popular = df.nlargest(10, "popularity")[["title", "popularity", "vote_average"]].to_dict("records")
    stats["top_10_popular_movies"] = most_popular
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stats_filepath = f"{PROCESSED_DATA_PATH}/daily_stats_{timestamp}.json"
    
    with open(stats_filepath, "w") as f:
        json.dump(stats, f, indent=2, default=str)
    
    return stats_filepath
