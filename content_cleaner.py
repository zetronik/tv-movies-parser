from bs4 import BeautifulSoup
import markdownify

def clean_html_to_markdown(html: str) -> str:
    """Очищает HTML-код от лишних тегов и конвертирует его в Markdown.

    Удаляет интерактивные, стилистические и навигационные элементы, чтобы
    снизить объем потребляемых токенов LLM-модели, сохраняя при этом текстовые
    данные, списки и таблицы.

    Args:
        html: Сырой HTML-код страницы.

    Returns:
        Очищенный текст в формате Markdown.
    """
    if not html:
        return ""

    soup = BeautifulSoup(html, "lxml")

    # Находим и удаляем неинформативные теги
    for tag_name in ["script", "style", "nav", "footer", "header", "aside"]:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    # Конвертируем очищенный HTML в markdown
    markdown_text = markdownify.markdownify(str(soup), heading_style="ATX")
    return markdown_text.strip()
