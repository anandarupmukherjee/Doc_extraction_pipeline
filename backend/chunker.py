"""
chunker.py - Text chunking with overlap
Ported from LLM_PDF_working.ipynb
"""


def chunk_text(text, chunk_chars=2200, overlap_chars=250):
    text = (text or "").strip()
    if not text:
        return []

    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks = []
    current = ""

    for para in paragraphs:
        if len(current) + len(para) + 2 <= chunk_chars:
            current = current + ("\n\n" if current else "") + para
        else:
            if current:
                chunks.append(current)
            current = para

    if current:
        chunks.append(current)

    final_chunks = []
    for chunk in chunks:
        if len(chunk) <= chunk_chars:
            final_chunks.append(chunk)
        else:
            start = 0
            while start < len(chunk):
                end = min(start + chunk_chars, len(chunk))
                final_chunks.append(chunk[start:end])
                if end == len(chunk):
                    break
                start = max(0, end - overlap_chars)

    return final_chunks


def table_to_text(table_obj, max_rows=40):
    header = table_obj.get("header", []) or []
    rows = table_obj.get("rows", []) or []
    raw_table = table_obj.get("raw_table", []) or []

    lines = []
    if header:
        lines.append("HEADER: " + " | ".join(header))

    usable_rows = rows if rows else raw_table
    usable_rows = usable_rows[:max_rows]

    for i, row in enumerate(usable_rows, start=1):
        row = row or []
        lines.append(f"ROW {i}: " + " | ".join(row))

    return "\n".join(lines).strip()
