import os
import sqlite3
import pandas as pd
import json
from datetime import datetime

DB_PATH = "/opt/airflow/data/movies.db"

def get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

def init_database():
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS movies (
            id INTEGER PRIMARY KEY,
            title TEXT,
            original_title TEXT,
            overview TEXT,
            release_date TEXT,
            release_year INTEGER,
            popularity REAL,
            vote_average REAL,
            vote_count INTEGER,
            genre_ids TEXT,
            genre_names TEXT,
            original_language TEXT,
            adult BOOLEAN,
            poster_path TEXT,
            backdrop_path TEXT,
            poster_url TEXT,
            category TEXT,
            rating_category TEXT,
            popularity_rank INTEGER,
            extracted_at TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS genres (
            id INTEGER PRIMARY KEY,
            name TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stat_date TEXT,
            total_movies INTEGER,
            avg_rating REAL,
            avg_popularity REAL,
            total_votes INTEGER,
            stats_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_rating ON movies(vote_average)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_popularity ON movies(popularity)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_year ON movies(release_year)')
    
    conn.commit()
    conn.close()
    
    return True

def load_movies(movies_csv_path):
    init_database()
    
    df = pd.read_csv(movies_csv_path)
    df["updated_at"] = datetime.now().isoformat()
    
    if "genre_ids" in df.columns:
        df["genre_ids"] = df["genre_ids"].astype(str)
    
    conn = get_connection()
    
    for _, row in df.iterrows():
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM movies WHERE id = ?', (row["id"],))
        exists = cursor.fetchone()
        
        if exists:
            cursor.execute('''
                UPDATE movies SET
                    title = ?, original_title = ?, overview = ?, release_date = ?,
                    release_year = ?, popularity = ?, vote_average = ?, vote_count = ?,
                    genre_ids = ?, genre_names = ?, original_language = ?, adult = ?,
                    poster_path = ?, backdrop_path = ?, poster_url = ?, category = ?,
                    rating_category = ?, popularity_rank = ?, extracted_at = ?, updated_at = ?
                WHERE id = ?
            ''', (
                row.get("title"), row.get("original_title"), row.get("overview"),
                str(row.get("release_date")), row.get("release_year"), row.get("popularity"),
                row.get("vote_average"), row.get("vote_count"), row.get("genre_ids"),
                row.get("genre_names"), row.get("original_language"), row.get("adult"),
                row.get("poster_path"), row.get("backdrop_path"), row.get("poster_url"),
                row.get("category"), row.get("rating_category"), row.get("popularity_rank"),
                row.get("extracted_at"), row.get("updated_at"), row["id"]
            ))
        else:
            cursor.execute('''
                INSERT INTO movies (
                    id, title, original_title, overview, release_date, release_year,
                    popularity, vote_average, vote_count, genre_ids, genre_names,
                    original_language, adult, poster_path, backdrop_path, poster_url,
                    category, rating_category, popularity_rank, extracted_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row["id"], row.get("title"), row.get("original_title"), row.get("overview"),
                str(row.get("release_date")), row.get("release_year"), row.get("popularity"),
                row.get("vote_average"), row.get("vote_count"), row.get("genre_ids"),
                row.get("genre_names"), row.get("original_language"), row.get("adult"),
                row.get("poster_path"), row.get("backdrop_path"), row.get("poster_url"),
                row.get("category"), row.get("rating_category"), row.get("popularity_rank"),
                row.get("extracted_at"), row.get("updated_at")
            ))
    
    conn.commit()
    conn.close()
    
    return len(df)

def load_genres(genres_csv_path):
    init_database()
    
    df = pd.read_csv(genres_csv_path)
    conn = get_connection()
    
    for _, row in df.iterrows():
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO genres (id, name, updated_at)
            VALUES (?, ?, ?)
        ''', (row["id"], row["name"], datetime.now().isoformat()))
    
    conn.commit()
    conn.close()
    
    return len(df)

def load_daily_stats(stats_json_path):
    init_database()
    
    with open(stats_json_path, "r") as f:
        stats = json.load(f)
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO daily_stats (
            stat_date, total_movies, avg_rating, avg_popularity, total_votes, stats_json
        ) VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        datetime.now().strftime("%Y-%m-%d"),
        stats.get("total_movies"),
        stats.get("avg_rating"),
        stats.get("avg_popularity"),
        stats.get("total_votes"),
        json.dumps(stats)
    ))
    
    conn.commit()
    conn.close()
    
    return True
