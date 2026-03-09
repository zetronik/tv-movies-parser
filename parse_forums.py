import bs4

with open('nnm_index.html', encoding='utf-8', errors='ignore') as f:
    soup = bs4.BeautifulSoup(f, 'lxml')

forums = soup.find_all('a', class_='forumlink')
res = [f"{f.text.strip()} -> {f.get('href', '')}" for f in forums if 'f=' in f.get('href', '')]

with open('forums.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(res))
