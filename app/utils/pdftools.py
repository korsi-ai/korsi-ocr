from io import BytesIO
from pathlib import Path

import pdf2image
from PIL import Image


def number_of_pages(path: Path) -> int:
    """
    Get number of pages in PDF file using pdf2image.pdfinfo_from_path for efficiency.
    """
    info = pdf2image.pdfinfo_from_path(str(path))
    return int(info.get("Pages", 0))


def extract_pdf_pages(path: Path) -> list[Image.Image]:
    return pdf2image.convert_from_path(str(path))


def extract_pdf_pages_with_index(path: Path) -> list[tuple[int, Image.Image]]:
    return list(enumerate(pdf2image.convert_from_path(str(path))))


def extract_pdf_bytes_pages(pdf_bytes: BytesIO) -> list[Image.Image]:
    pdf_bytes.seek(0)
    return pdf2image.convert_from_bytes(pdf_bytes.read())


def extract_pdf_bytes_pages_with_index(
    pdf_bytes: BytesIO,
) -> list[tuple[int, Image.Image]]:
    pdf_bytes.seek(0)
    return list(enumerate(pdf2image.convert_from_bytes(pdf_bytes.read())))
