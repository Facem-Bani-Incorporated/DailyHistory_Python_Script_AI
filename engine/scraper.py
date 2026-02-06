import httpx
import cloudinary
import cloudinary.uploader
from datetime import datetime
from typing import Optional, List
from core.config import config
from core.logger import setup_logger

logger = setup_logger("Scraper")


class WikiScraper:
    def __init__(self):
        try:
            self.headers = {"User-Agent": config.USER_AGENT}
            cloudinary.config(
                cloud_name=config.CLOUDINARY_CLOUD_NAME,
                api_key=config.CLOUDINARY_API_KEY,
                api_secret=config.CLOUDINARY_API_SECRET,
                secure=True
            )
        except Exception as e:
            logger.critical(f"🚨 Cloudinary Config Failed: {e}")

    async def fetch_today(self) -> List[dict]:
        now = datetime.now()
        url = f"{config.WIKI_BASE_URL}/feed/onthisday/events/{now.month}/{now.day}"

        async with httpx.AsyncClient(headers=self.headers, timeout=30.0) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                # Combinăm evenimentele selectate cu cele normale
                return data.get('selected', []) + data.get('events', [])
            except httpx.HTTPError as e:
                logger.error(f"❌ Wiki API Network Error: {e}")
            except Exception as e:
                logger.error(f"❌ Wiki Unexpected Error: {e}")
            return []

    async def fetch_gallery_urls(self, title_slug: str, limit: int = 5) -> List[str]:
        if not title_slug:
            return []

        image_urls = []
        url = f"{config.WIKI_BASE_URL}/page/media-list/{title_slug.replace(' ', '_')}"

        async with httpx.AsyncClient(headers=self.headers, timeout=15.0) as client:
            try:
                res = await client.get(url)
                if res.status_code == 200:
                    items = res.json().get('items', [])
                    for item in items:
                        if item.get('type') == 'image':
                            # Luăm cel mai bun URL disponibil
                            img_src = item.get('srcset', [{}])[0].get('src') or item.get('title')
                            if img_src:
                                full_url = f"https:{img_src}" if img_src.startswith("//") else img_src
                                if ".svg" not in full_url.lower():
                                    image_urls.append(full_url)
                        if len(image_urls) >= limit:
                            break
            except Exception as e:
                logger.warning(f"⚠️ Could not fetch gallery for {title_slug}: {e}")

        # Fallback dacă Wiki nu are poze
        if not image_urls:
            image_urls.append("https://images.unsplash.com/photo-1447069387593-a5de0862481e?w=800")
        return image_urls

    def upload_to_cloudinary(self, image_url: str, public_id: str) -> Optional[str]:
        if not image_url or "via.placeholder" in image_url:
            return None
        try:
            result = cloudinary.uploader.upload(
                image_url,
                public_id=f"history_app/{public_id}",
                overwrite=True,
                transformation=[{'width': 1000, 'crop': "limit", 'quality': "auto"}]
            )
            return result.get('secure_url')
        except Exception as e:
            logger.error(f"⚠️ Cloudinary Upload Fail ({public_id}): {e}")
            return None