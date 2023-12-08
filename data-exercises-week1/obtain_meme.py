import requests
from PIL import Image
from pathlib import Path
from typing import Any, Dict
from uuid import uuid4
import random

URL = "https://api.imgflip.com/get_memes"
memes_folder: Path = Path('./memes')

def _fetch_random_meme_url() -> str:
    url: str = URL
    response: requests.Response = requests.get(url)
    data: Dict[str, Any] = response.json()

    meme_url: list = data['data']['memes']
    img_url = random.choice(meme_url)['url']
    return img_url

def _generate_unique_filename() -> str:
    unique_id: str = str(uuid4().int)[-3:]
    return f"imagen_meme-{unique_id}.jpeg"

def _save_image_from_url(url: str, folder: Path, filename: str) -> None:
    image_response: requests.Response = requests.get(url)
    image_data: bytes = image_response.content

    with open(folder / filename, 'wb') as file:
        file.write(image_data)

    print(f"Image stored at '{folder / filename}'")

def save_random_meme() -> None:
    memes_folder: Path = Path('./memes')
    meme_url: str = _fetch_random_meme_url()
    unique_filename: str = _generate_unique_filename()
    _save_image_from_url(meme_url, memes_folder, unique_filename)

if __name__ == "__main__":
    save_random_meme()

