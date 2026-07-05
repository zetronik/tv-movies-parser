from pydantic import BaseModel, Field
from typing import Optional

class TorrentExtraction(BaseModel):
    """Схема для извлечения данных о торрент-раздаче с помощью LLM."""

    ru_title: str = Field(
        description="Русское название фильма или сериала."
    )
    orig_title: Optional[str] = Field(
        default=None,
        description="Оригинальное название фильма или сериала (если есть, обычно на английском/латинице)."
    )
    year: int = Field(
        default=0,
        description="Год выпуска фильма или сериала (четырехзначное число)."
    )
    size_gb: float = Field(
        default=0.0,
        description="Размер раздачи/торрента в гигабайтах (число с плавающей точкой)."
    )
    quality: str = Field(
        default="",
        description="Качество видео раздачи (например, BDRip, WEB-DL, 1080p, HDRip и т.д.)."
    )
    magnet_link: str = Field(
        default="",
        description="Magnet-ссылка для скачивания раздачи, обязательно должна начинаться с 'magnet:?xt='."
    )
