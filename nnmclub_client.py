import os
import re
import cloudscraper
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

class NnmclubClient:
    def __init__(self):
        # Используем cloudscraper для автоматического обхода защиты Cloudflare для гостей
        self.session = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True})
        ua = os.environ.get("NNMCLUB_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        self.session.headers.update({
            "User-Agent": ua
        })
        self.base_url = "https://nnmclub.to/forum"

    def parse_topic_title(self, title):
        match = re.search(r'^(.+?)(?:\s+/\s+(.+?))?(?:\s+/\s+.*?)?\s*\((\d{4})(?:-\d{4})?\)', title)
        if match:
            ru_title = match.group(1).strip()
            orig_title = match.group(2).strip() if match.group(2) else ""
            return ru_title, orig_title, match.group(3)
        return None, None, None

    def get_topics_from_forum(self, forum_id, pages=1):
        topics = []
        for page in range(pages):
            start = page * 50
            url = f"{self.base_url}/viewforum.php?f={forum_id}&start={start}"
            res = self.session.get(url)
            soup = BeautifulSoup(res.text, 'lxml')
            
            for row in soup.select('table.forumline tr'):
                a_tag = row.select_one('a.topictitle')
                if not a_tag: continue
                title = a_tag.text.strip()
                if re.search(r'DVD(-?Video|5|9)', title, re.IGNORECASE): continue
                
                href = a_tag.get('href', '')
                if 'viewtopic.php?t=' in href:
                    topic_match = re.search(r't=(\d+)', href)
                    if not topic_match: continue
                    topic_id = int(topic_match.group(1))
                    
                    seed_tag = row.select_one('span[title="Seeders"] b') or row.select_one('.seed b') or row.select_one('.seedmed b')
                    leech_tag = row.select_one('span[title="Leechers"] b') or row.select_one('.leech b') or row.select_one('.leechmed b')
                    
                    seeds = int(seed_tag.text) if seed_tag and seed_tag.text.isdigit() else 0
                    leeches = int(leech_tag.text) if leech_tag and leech_tag.text.isdigit() else 0
                    
                    size_gb = 0.0
                    size_tag = row.select_one('div.gensmall a.gensmall')
                    if size_tag and ('GB' in size_tag.text or 'MB' in size_tag.text or 'ГБ' in size_tag.text or 'МБ' in size_tag.text):
                        nums = re.findall(r'[\d\.]+', size_tag.text.replace(',', '.'))
                        if nums:
                            val = float(nums[0])
                            size_gb = val if ('GB' in size_tag.text or 'ГБ' in size_tag.text) else val / 1024

                    topics.append({'topic_id': topic_id, 'title': title, 'seeds': seeds, 'leeches': leeches, 'size_gb': size_gb})
        return topics
    def get_topic_details(self, topic_id):
        url = f"{self.base_url}/viewtopic.php?t={topic_id}"
        res = self.session.get(url)
        soup = BeautifulSoup(res.text, 'lxml')
        
        magnet_tag = soup.find('a', href=re.compile(r'^magnet:\?xt='))
        magnet = magnet_tag['href'] if magnet_tag else ""
        
        size_text = ""
        for span in soup.select('span.genmed b'):
            if 'GB' in span.text or 'MB' in span.text or 'ГБ' in span.text or 'МБ' in span.text:
                size_text = span.text
                break
        
        size_gb = 0.0
        if size_text:
            nums = re.findall(r'[\d\.]+', size_text.replace(',', '.'))
            if nums:
                val = float(nums[0])
                size_gb = val if 'GB' in size_text or 'ГБ' in size_text else val / 1024

        def get_text_after(label_pattern):
            tag = soup.find(string=re.compile(label_pattern))
            if tag and tag.parent:
                cur = tag.parent
                if cur.name in ('span', 'b', 'strong'):
                    nxt = cur.next_sibling
                    if nxt and isinstance(nxt, str):
                        return nxt.strip().lstrip(':').strip()
            return ""

        quality = get_text_after(r'(?i)качество( видео)?')
        translation = get_text_after(r'(?i)перевод')
        file_format = get_text_after(r'(?i)формат')
                
        return {'magnet': magnet, 'size_gb': size_gb, 'quality': quality, 'file_format': file_format, 'translation': translation}
