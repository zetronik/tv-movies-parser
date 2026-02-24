import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import re

load_dotenv()

class RutrackerClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        })
        self.login_username = os.environ.get("RUTRACKER_LOGIN")
        self.login_password = os.environ.get("RUTRACKER_PASSWORD")

    def login(self):
        """Авторизация на Rutracker"""
        if not self.login_username or not self.login_password:
            raise ValueError("Rutracker credentials are not set in the environment variables.")

        url = "https://rutracker.org/forum/login.php"
        data = {
            "login_username": self.login_username,
            "login_password": self.login_password,
            "login": "Вход"
        }
        
        response = self.session.post(url, data=data)
        response.raise_for_status()
        
        if 'bb_session' in self.session.cookies or 'profile.php?mode=viewprofile' in response.text:
            return True
        return False

    def search_movie(self, title, original_title, year):
        """Поиск фильма на трекере по оригинальному названию и году"""
        url = "https://rutracker.org/forum/tracker.php"
        nm = f"{original_title} {year}"
        params = {
            "nm": nm
        }
        
        response = self.session.get(url, params=params)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "lxml")
        rows = soup.find_all("tr", class_="tCenter")
        
        results = []
        
        for row in rows:
            title_tag = row.find("a", class_="tLink")
            if not title_tag:
                 title_tag = row.find("a", class_="tt-text")
            
            if not title_tag:
                continue
                
            topic_title = title_tag.text.strip()
            topic_url = f"https://rutracker.org/forum/{title_tag['href']}"
            topic_id = title_tag['href'].split("=")[-1]
            
            # Извлечение сидов
            seeds = 0
            seed_elem = row.find(class_=re.compile(r'seedmed')) or row.find(title="Сиды")
            if seed_elem:
                seed_digits = re.sub(r'\D', '', seed_elem.text)
                if seed_digits:
                    seeds = int(seed_digits)
            
            # Извлечение личей
            leeches = 0
            leech_elem = row.find(class_=re.compile(r'leechmed')) or row.find(title="Личи")
            if leech_elem:
                leech_digits = re.sub(r'\D', '', leech_elem.text)
                if leech_digits:
                    leeches = int(leech_digits)
            
            results.append({
                "topic_id": topic_id,
                "topic_url": topic_url,
                "topic_title": topic_title,
                "seeds": seeds,
                "leeches": leeches
            })
            
        return results

    def _extract_meta(self, soup, keywords):
        if isinstance(keywords, str):
            keywords = [keywords]
        
        # Ищем тег, содержащий любой из синонимов (без учета регистра)
        tag = soup.find(lambda t: getattr(t, 'name', None) in ['span', 'b', 'strong'] and t.text and any(k.lower() in t.text.lower() for k in keywords))
        
        if not tag:
            return None
        
        value = ""
        # Собираем весь текст после найденного тега до первого переноса строки <br>
        for sibling in tag.next_siblings:
            if getattr(sibling, 'name', None) == 'br':
                break
            # Останавливаемся, если началось следующее поле (новый тег b, span или strong)
            if getattr(sibling, 'name', None) in ['span', 'b', 'strong']:
                break
            if isinstance(sibling, str):
                value += sibling
            else:
                value += sibling.get_text()
                
        # Очищаем результат от двоеточий, лишних пробелов и неразрывных пробелов
        clean_value = value.strip(' :\\n\\r\\t\\xa0')
        return clean_value if clean_value else None

    def get_forums_from_category(self, category_id):
        """Собирает ID всех подразделов (форумов) из указанной категории (например, Кино = 2)."""
        url = f"https://rutracker.org/forum/index.php?c={category_id}"
        response = self.session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        
        forum_ids = []
        # Ищем все ссылки на форумы (подразделы)
        for a_tag in soup.select('a'):
            href = a_tag.get('href')
            if href and 'viewforum.php?f=' in href:
                forum_id = href.split('f=')[-1]
                forum_ids.append(forum_id)
        return list(set(forum_ids)) # Убираем дубликаты

    def get_topic_details(self, topic_id):
        """Заходит в топик и собирает магнит, сиды, личи и размер."""
        url = f"https://rutracker.org/forum/viewtopic.php?t={topic_id}"
        response = self.session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        
        details = {
            'magnet': None, 'size_gb': 0.0, 'seeds': 0, 'leeches': 0,
            'quality': None, 'format': None, 'translation': None
        }
        
        # Магнет ссылка
        magnet_a = soup.find("a", class_="magnet-link")
        if magnet_a: details['magnet'] = magnet_a.get('href')
        
        # Сиды и Личи (на рутрекере хранятся в span с классами seed и leech)
        seed_span = soup.find("span", class_="seed")
        if seed_span: details['seeds'] = int(re.sub(r'[^0-9]', '', seed_span.text) or 0)
        
        leech_span = soup.find("span", class_="leech")
        if leech_span: details['leeches'] = int(re.sub(r'[^0-9]', '', leech_span.text) or 0)
            
        # Размер файла
        size_span = soup.find("span", id="tor-size-humn")
        if size_span:
            raw_size = size_span.text.replace('\xa0', ' ').replace('&nbsp;', ' ').strip()
            match = re.search(r'([\d\.]+)\s*([A-Za-zА-Яа-я]+)', raw_size)
            if match:
                val = float(match.group(1))
                unit = match.group(2).upper()
                if unit in ['GB', 'ГБ']: details['size_gb'] = val
                elif unit in ['MB', 'МБ']: details['size_gb'] = val / 1024.0
                
        # Качество берем из заголовка как резерв (последнее значение в скобках [])
        title_tag = soup.select_one('h1.maintitle a')
        if title_tag:
            q_match = re.search(r'\[[^\]]+,\s*([^,\]]+)\]', title_tag.text)
            if q_match: details['quality'] = q_match.group(1).strip()
            
        return details

    def parse_topic_title(self, title):
        """
        Извлекает русское название, оригинальное название и год из стандартного заголовка Rutracker.
        Пример: Зеленая миля / The Green Mile (Фрэнк Дарабонт) [1999, США, BDRip]
        """
        # Регулярное выражение для поиска названий и года в квадратных скобках
        match = re.search(r'^(.+?)(?:\s+/\s+(.+?))?(?:\s+\(.*\))?\s+\[(\d{4})', title)
        if match:
            ru_title = match.group(1).strip()
            # Если оригинального названия нет, group(2) будет None
            orig_title = match.group(2).strip() if match.group(2) else ""
            year = match.group(3)
            return ru_title, orig_title, year
        return None, None, None

    def get_topics_from_forum(self, forum_id, pages=1):
        """
        Проходит по страницам форума и собирает ссылки на топики и их заголовки.
        """
        topics = []
        for page in range(pages):
            # Рутрекер использует смещение (start) кратное 50 для пагинации
            start = page * 50
            url = f"https://rutracker.org/forum/viewforum.php?f={forum_id}&start={start}"
            
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Ищем все ссылки на топики в таблице
            for a_tag in soup.select('a.tt-text'):
                title = a_tag.text.strip()
                href = a_tag.get('href')
                if href and 'viewtopic.php?t=' in href:
                    topic_id = href.split('t=')[-1]
                    topics.append({
                        'topic_id': topic_id,
                        'title': title
                    })
        return topics
