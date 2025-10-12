import requests
import pandas as pd

anime_list = []

for i in range(1, 6):  # pages
    url = f"https://api.jikan.moe/v4/anime?page={i}"
    res = requests.get(url).json()
    for item in res['data']:
        anime_list.append({
            'title': item['title'],
            'type': item.get('type', ''),
            'rating': item.get('score', 0),
            'genres': ', '.join([g['name'] for g in item['genres']])
        })

df = pd.DataFrame(anime_list)
print(df.head())