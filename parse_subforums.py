import cloudscraper
from bs4 import BeautifulSoup

s = cloudscraper.create_scraper()
targets = {
    'Горячие новинки': 216,
    'Классика кино': 318,
    'Зарубежное кино': 224,
    'Сериалы': 768
}

res = []
for name, fid in targets.items():
    r = s.get(f'https://nnmclub.to/forum/viewforum.php?f={fid}')
    soup = BeautifulSoup(r.text, 'lxml')
    for a in soup.find_all('a', class_='forumlink'):
        href = a.get('href', '')
        if 'f=' in href:
            res.append(f"{a.text.strip()} -> {href}")

with open('subforums.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(res))
