import requests
import pandas as pd
import time
from datetime import datetime

# Securely store Client ID (do not share publicly; consider .env file for production)
CLIENT_ID = "135a34ce6bf83662fd5541e1feb780d0"

# Function to fetch anime details by ID
def fetch_anime_details(anime_id):
    url = f"https://api.myanimelist.net/v2/anime/{anime_id}?fields=id,title,main_picture,alternative_titles,start_date,end_date,synopsis,mean,rank,popularity,num_list_users,num_scoring_users,media_type,genres,num_episodes"
    headers = {"X-MAL-CLIENT-ID": CLIENT_ID}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching details for anime ID {anime_id}: {e}")
        return None

# Fetch top anime by popularity (fixed: ranking_type=bypopularity)
url = "https://api.myanimelist.net/v2/anime/ranking?ranking_type=bypopularity&limit=30&fields=id,title,mean,popularity,num_list_users,media_type"
headers = {"X-MAL-CLIENT-ID": CLIENT_ID}
anime_list = []

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
except requests.RequestException as e:
    print(f"Error fetching top anime: {e}")
    exit()

# Extract initial data
for anime in data.get("data", []):
    node = anime.get("node", {})
    anime_id = node.get("id")
    title = node.get("title")
    mean_rating = node.get("mean")
    popularity_rank = node.get("popularity")
    members = node.get("num_list_users")
    media_type = node.get("media_type")
    
    # Fetch additional details (genres, episodes, etc.)
    details = fetch_anime_details(anime_id)
    if details:
        genres = [g["name"] for g in details.get("genres", [])] if details.get("genres") else []
        episodes = details.get("num_episodes")
        start_date = details.get("start_date")
        end_date = details.get("end_date")
        synopsis = details.get("synopsis")
    else:
        genres = []
        episodes = None
        start_date = None
        end_date = None
        synopsis = None
    
    anime_list.append({
        "anime_id": anime_id,
        "title": title,
        "mean_rating": mean_rating,
        "popularity_rank": popularity_rank,
        "members": members,
        "media_type": media_type,
        "genres": genres,
        "episodes": episodes,
        "start_date": start_date,
        "end_date": end_date,
        "synopsis": synopsis
    })
    time.sleep(0.2)  # Respect rate limit (~5 requests/second)

# Convert to DataFrame
df = pd.DataFrame(anime_list)

# Clean data
df = df.dropna(subset=["mean_rating"])  # Remove anime with missing ratings
df["genres"] = df["genres"].apply(lambda x: x if x else ["Unknown"])  # Handle empty genres
df = df.drop_duplicates(subset=["anime_id"])  # Remove duplicates
df["episodes"] = df["episodes"].fillna(0)  # Fill missing episodes with 0
df["synopsis"] = df["synopsis"].fillna("No synopsis available")  # Fill missing synopsis

# Save to CSV with timestamp for weekly updates
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
df.to_csv(f"top_anime_popularity_{timestamp}.csv", index=False)
print(f"Saved data to top_anime_popularity_{timestamp}.csv")

# Prepare feature matrix for cosine similarity (one-hot encode genres)
all_genres = set(genre for genres in df["genres"] for genre in genres)
for genre in all_genres:
    df[genre] = df["genres"].apply(lambda x: 1 if genre in x else 0)

# Save feature matrix (for cosine similarity)
feature_columns = list(all_genres)
feature_matrix = df[feature_columns]
feature_matrix["anime_id"] = df["anime_id"]  # Keep ID for mapping
feature_matrix.to_csv("anime_features.csv", index=False)
print("Saved feature matrix to anime_features.csv")

# Print sample for verification
print("\nSample DataFrame (Top 5):")
print(df[["anime_id", "title", "genres", "mean_rating", "popularity_rank", "members"]].head())