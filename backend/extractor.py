"""
extractor.py - PDF content extraction using PyMuPDF + pdfplumber.
Provides both text extraction (for table metadata) and page-image rendering
(base64 PNG) for vision-based LLM calls.
"""

import re
import base64
import fitz  # PyMuPDF
import pdfplumber


def clean_text(text):
    text = text or ""
    text = re.sub(r"\s+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_table_cell(x):
    if x is None:
        return ""
    x = str(x).strip()
    x = re.sub(r"\s+", " ", x)
    return x


def extract_tables_with_pdfplumber(pdf_path, page_number_1_based):
    tables_out = []
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[page_number_1_based - 1]
        tables = page.extract_tables()

        for t_idx, table in enumerate(tables):
            if not table:
                continue

            norm = []
            for row in table:
                if row is None:
                    continue
                row = [normalize_table_cell(c) for c in row]
                if any(cell != "" for cell in row):
                    norm.append(row)

            if not norm:
                continue

            header = norm[0]
            rows = norm[1:] if len(norm) > 1 else []
            non_empty_header = sum(1 for h in header if h != "")
            if non_empty_header <= 1:
                rows = norm
                header = []

            tables_out.append({
                "table_index": t_idx,
                "header": header,
                "rows": rows,
                "raw_table": norm
            })

    return tables_out


def extract_pdf_content(pdf_path, max_pages=None):
    doc = fitz.open(pdf_path)
    total_pages = len(doc) if max_pages is None else min(len(doc), max_pages)

    pages = []
    for i in range(total_pages):
        page_number = i + 1
        page = doc[i]
        text = clean_text(page.get_text("text"))

        try:
            tables = extract_tables_with_pdfplumber(pdf_path, page_number)
        except Exception:
            tables = []

        pages.append({
            "page_number": page_number,
            "text": text,
            "tables": tables,
            "has_tables": len(tables) > 0
        })

    doc.close()
    return pages


def render_pages_as_base64(pdf_path: str, page_numbers_1based: list, dpi: int = 150) -> list:
    """
    Render specified PDF pages (1-based) as PNG images and return a list of
    base64-encoded strings, one per page. Used for vision-based LLM calls.
    """
    doc = fitz.open(pdf_path)
    mat = fitz.Matrix(dpi / 72, dpi / 72)  # scale factor from 72 DPI baseline
    images_b64 = []

    for page_num in page_numbers_1based:
        idx = page_num - 1
        if idx < 0 or idx >= len(doc):
            continue
        page = doc[idx]
        pix = page.get_pixmap(matrix=mat)
        png_bytes = pix.tobytes("png")
        images_b64.append(base64.b64encode(png_bytes).decode("utf-8"))

    doc.close()
    return images_b64
