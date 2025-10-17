import requests
import pandas as pd
import time

# MyAnimeList API credentials (Client ID is sufficient for public data)
CLIENT_ID = '135a34ce6bf83662fd5541e1feb780d0'

# Fields to fetch (based on API-supported fields matching your requirements)
fields = 'id,title,alternative_titles,media_type,num_episodes,status,start_date,end_date,mean,rank,popularity,num_list_users,num_scoring_users,genres,studios,producers,licensors,source,average_episode_duration,rating,main_picture,start_season,broadcast'

# List to store all anime data
anime_list = []

# Pagination parameters
offset = 0
limit = 500  # Max limit for ranking endpoint
url_base = f"https://api.myanimelist.net/v2/anime/ranking?ranking_type=bypopularity&limit={limit}&fields={fields}"

headers = {"X-MAL-CLIENT-ID": CLIENT_ID}

print("Starting data fetch...")

while True:
    url = f"{url_base}&offset={offset}"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"Error fetching data at offset {offset}: {e}")
        break

    if not data.get('data'):
        print("No more data available.")
        break

    for item in data['data']:
        node = item['node']
        # Extract and flatten data
        anime_dict = {
            'anime_id': node.get('id'),
            'title': node.get('title'),
            'alternative_titles_synonyms': ','.join(node.get('alternative_titles', {}).get('synonyms', [])) if node.get('alternative_titles') else '',
            'alternative_titles_japanese': node.get('alternative_titles', {}).get('ja', '') if node.get('alternative_titles') else '',
            'alternative_titles_english': node.get('alternative_titles', {}).get('en', '') if node.get('alternative_titles') else '',
            'type': node.get('media_type', ''),
            'episodes': node.get('num_episodes', 0),
            'status': node.get('status', ''),
            'aired_start': node.get('start_date', ''),
            'aired_end': node.get('end_date', ''),
            'premiered': f"{node.get('start_season', {}).get('season', '')} {node.get('start_season', {}).get('year', '')}" if node.get('start_season') else '',
            'broadcast': node.get('broadcast', {}).get('string', '') if node.get('broadcast') else '',
            'producers': ','.join([p['name'] for p in node.get('producers', [])]) if node.get('producers') else '',
            'licensors': ','.join([l['name'] for l in node.get('licensors', [])]) if node.get('licensors') else '',
            'studios': ','.join([s['name'] for s in node.get('studios', [])]) if node.get('studios') else '',
            'source': node.get('source', ''),
            'genres': ','.join([g['name'] for g in node.get('genres', [])]) if node.get('genres') else '',
            'duration_min': round(node.get('average_episode_duration', 0) / 60) if node.get('average_episode_duration') else 0,
            'rating': node.get('rating', ''),
            'score': node.get('mean', 0.0),
            'scored_by': node.get('num_scoring_users', 0),
            'ranked': node.get('rank', 0),
            'popularity': node.get('popularity', 0),
            'members': node.get('num_list_users', 0),
            'photo_url': node.get('main_picture', {}).get('medium', '') if node.get('main_picture') else ''
        }
        anime_list.append(anime_dict)

    if 'next' not in data.get('paging', {}):
        print("Reached the end of the list.")
        break

    offset += limit
    time.sleep(0.2)  # Delay to respect rate limits (~5 requests/second)

# Convert to DataFrame and save to CSV
if anime_list:
    df = pd.DataFrame(anime_list)
    df.to_csv('all_anime_data.csv', index=False)
    print(f"Saved {len(df)} anime entries to all_anime_data.csv")
else:
    print("No data fetched.")