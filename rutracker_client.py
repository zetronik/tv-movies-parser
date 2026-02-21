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

    def get_topic_details(self, topic_url):
        """Получает детальную информацию со страницы раздачи"""
        response = self.session.get(topic_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "lxml")
        
        details = {
            'magnet': None,
            'size': None,
            'quality': None,
            'format': None,
            'translation': None
        }

        # Magnet ссылка
        magnet_a = soup.find("a", class_="magnet-link")
        if magnet_a and magnet_a.has_attr('href'):
             details['magnet'] = magnet_a['href']
        
        # Размер
        size_span = soup.find("span", id="tor-size-humn")
        if size_span:
            raw_size = size_span.text
            # Очистка неразрывных пробелов
            clean_size = raw_size.replace('\xa0', ' ').replace('&nbsp;', ' ').strip()
            details['size'] = clean_size

        # Детальная информация с синонимами
        details['quality'] = self._extract_meta(soup, ["Качество", "Тип релиза", "Релиз"])
        
        # Запасной план для извлечения качества из заголовка
        if not details['quality']:
            title_tag = soup.select_one('h1.maintitle a')
            if title_tag:
                # Ищем последний элемент внутри квадратных скобок (например, [1988, комедия, BDRemux 1080p])
                match = re.search(r'\[[^\]]+,\s*([^,\]]+)\]', title_tag.text)
                if match:
                    details['quality'] = match.group(1).strip()
                    
        details['format'] = self._extract_meta(soup, ["Формат", "Контейнер", "Расширение"])
        details['translation'] = self._extract_meta(soup, ["Перевод", "Озвучивание", "Озвучка"])

        return details
