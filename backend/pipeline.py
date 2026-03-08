"""
pipeline.py - Direct PDF → LLM pipeline.

Strategy:
  1. Send the raw PDF file directly to the LLM via Ollama's 'files' API
     (same as attaching a file in the Ollama web UI — no image conversion).
  2. Tables are extracted as text (pdfplumber) and sent in a separate call.
  3. Results use the standardised schema:
       mineral, stage, partner_country, companies, mines, locations,
       trade_value_usd, trade_volume_kg_tonnes, page_no, document_name
"""

import json
import os
import uuid
import threading
from datetime import datetime

import pandas as pd

from extractor import extract_pdf_content
from chunker import table_to_text
from prompts import (
    build_triage_prompt,
    build_graph_extraction_prompt,
    extract_json_from_text,
    load_prompts_config,
)
from llm_client import call_ollama

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/app/output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# In-memory job store
# ─────────────────────────────────────────────────────────────

_jobs: dict[str, dict] = {}
_job_lock = threading.Lock()


def create_job() -> str:
    job_id = str(uuid.uuid4())
    with _job_lock:
        _jobs[job_id] = {
            "status": "pending",
            "logs": [],
            "progress": 0,
            "total_pages": 0,
            "total_facts": 0,
            "failed_calls": 0,
            "started_at": None,
            "finished_at": None,
            "jsonl_path": None,
            "csv_path": None,
            "error": None,
        }
    return job_id


def get_job(job_id: str) -> dict | None:
    with _job_lock:
        return _jobs.get(job_id)


def _update_job(job_id: str, **kwargs):
    with _job_lock:
        if job_id in _jobs:
            _jobs[job_id].update(kwargs)


def _append_log(job_id: str, msg: str):
    with _job_lock:
        if job_id in _jobs:
            _jobs[job_id]["logs"].append(msg)


# ─────────────────────────────────────────────────────────────
# Graph validation and flattening
# ─────────────────────────────────────────────────────────────

def is_valid_fact(fact: dict) -> bool:
    if not isinstance(fact, dict):
        return False
    # A valid graph fact should at least have a subject, relation, and object
    return bool(fact.get("relation")) and bool(fact.get("subject")) and bool(fact.get("object"))

def flatten_fact(fact: dict, document_name: str, page_no: str, source_tag: str) -> dict:
    """Flattens the nested graph extractions JSON to a flat dict for CSV storage."""
    flat = {
        "fact_id": fact.get("fact_id"),
        "fact_type": fact.get("fact_type"),
        "stage": fact.get("stage"),
        "chain": fact.get("chain"),
        "evidence_text": fact.get("evidence_text"),
        "confidence": fact.get("confidence"),
        "inference_notes": fact.get("inference_notes"),
        "document_name": document_name,
        "page_no": page_no,
        "_source": source_tag
    }
    
    # Flatten subject
    sub = fact.get("subject", {})
    if isinstance(sub, dict):
        flat["subject_name"] = sub.get("name")
        flat["subject_type"] = sub.get("type")
        
    flat["relation"] = fact.get("relation")
        
    # Flatten object
    obj = fact.get("object", {})
    if isinstance(obj, dict):
        flat["object_name"] = obj.get("name")
        flat["object_type"] = obj.get("type")
        
    # Flatten attributes
    attrs = fact.get("attributes", {})
    if isinstance(attrs, dict):
        for k, v in attrs.items():
            # If it's a list (like esg_flags, aliases), join into comma string
            if isinstance(v, list):
                flat[f"attr_{k}"] = ", ".join(str(x) for x in v)
            else:
                flat[f"attr_{k}"] = v
                
    return flat

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _append_jsonl(records, path):
    with open(path, "a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _extract_graph(system_prompt, user_prompt, document_name, page_no, source_tag, model,
                  pdf_path=None) -> tuple[list, str, dict]:
    """Call LLM, parse graph extractions, return (flattened_rows, raw_json, summary_dict)."""
    raw = call_ollama(system_prompt, user_prompt, model=model, pdf_path=pdf_path)
    parsed = extract_json_from_text(raw)
    
    # The new prompt returns {"document_summary": {}, "extractions": []}
    extractions = parsed.get("extractions", [])
    doc_summary = parsed.get("document_summary", {})

    cleaned = []
    if isinstance(extractions, list):
        for fact in extractions:
            if not is_valid_fact(fact):
                continue
            cleaned.append(flatten_fact(fact, document_name, str(page_no), source_tag))
            
    return cleaned, raw, doc_summary


# ─────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────

def run_pipeline(job_id: str, pdf_path: str, model: str = None):
    try:
        _update_job(job_id, status="running", started_at=datetime.utcnow().isoformat())
        log = lambda msg: _append_log(job_id, msg)

        document_name = os.path.basename(pdf_path)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        jsonl_path  = os.path.join(OUTPUT_DIR, f"facts_{job_id[:8]}_{ts}.jsonl")
        csv_path    = os.path.join(OUTPUT_DIR, f"facts_{job_id[:8]}_{ts}.csv")
        raw_path    = os.path.join(OUTPUT_DIR, f"raw_{job_id[:8]}_{ts}.jsonl")
        failed_path = os.path.join(OUTPUT_DIR, f"failed_{job_id[:8]}_{ts}.jsonl")

        _update_job(job_id, jsonl_path=jsonl_path, csv_path=csv_path)
        for p in [jsonl_path, csv_path, raw_path, failed_path]:
            open(p, "w", encoding="utf-8").close()

        # ── 1. Read PDF & Setup ───────────────
        log(f"[INFO] Reading PDF: {document_name}")
        import fitz
        
        try:
            doc = fitz.open(pdf_path)
        except Exception as e:
            raise RuntimeError(f"Failed to open PDF: {e}")
            
        total_pages = len(doc)
        _update_job(job_id, total_pages=total_pages, progress=5)
        log(f"[INFO] {total_pages} page(s). Starting page-by-page Triage -> Graph Extraction…")

        config = load_prompts_config()
        total_facts  = 0
        failed_calls = 0
        
        from prompts import build_triage_prompt, build_graph_extraction_prompt

        # ── 2. Process Page-by-Page ─────────
        for page_idx in range(total_pages):
            page_num = page_idx + 1
            log(f"[INFO] Processing Page {page_num}/{total_pages}...")
            
            # Extract single page to a temporary PDF
            temp_pdf_path = os.path.join(OUTPUT_DIR, f"temp_{job_id[:8]}_p{page_num}.pdf")
            try:
                temp_doc = fitz.open()
                temp_doc.insert_pdf(doc, from_page=page_idx, to_page=page_idx)
                temp_doc.save(temp_pdf_path)
                temp_doc.close()
            except Exception as e:
                log(f"[ERROR] Failed to extract page {page_num}: {e}")
                failed_calls += 1
                continue

            # Step 2A: Triage
            try:
                triage_sys, triage_usr = build_triage_prompt(page_num, config=config)
                raw_triage = call_ollama(triage_sys, triage_usr, model=model, pdf_path=temp_pdf_path)
                triage_json = extract_json_from_text(raw_triage)
                
                # If triage explicitly says not extractable, we can optionally skip, 
                # but the user rules imply we should pass it to the graph extractor 
                # unless absolutely empty. We'll proceed with suggested_mode.
                suggested_mode = triage_json.get("suggested_mode", "mixed")
                log(f"  [TRIAGE] mode = {suggested_mode}")
                
            except Exception as e:
                log(f"  [ERROR] Triage failed on page {page_num}: {e}")
                failed_calls += 1
                if os.path.exists(temp_pdf_path):
                    os.remove(temp_pdf_path)
                continue
                
            # Step 2B: Graph Extraction
            try:
                graph_sys, graph_usr = build_graph_extraction_prompt(
                    page_number=page_num, 
                    suggested_mode=suggested_mode, 
                    config=config
                )
                
                cleaned_rows, raw_graph, doc_summary = _extract_graph(
                    graph_sys, graph_usr,
                    document_name, page_num, "graph_extraction", model,
                    pdf_path=temp_pdf_path
                )
                
                _append_jsonl([{"source": f"page_{page_num}_graph", "raw_output": raw_graph}], raw_path)

                if cleaned_rows:
                    _append_jsonl(cleaned_rows, jsonl_path)
                    total_facts += len(cleaned_rows)
                    log(f"  [EXTRACTION] → {len(cleaned_rows)} facts found")
                else:
                    log(f"  [EXTRACTION] → No facts found")
                    
                # Optionally append the doc summary notes somewhere
                if doc_summary and doc_summary.get("notes"):
                    log(f"  [NOTES] {doc_summary.get('notes')}")

            except Exception as e:
                failed_calls += 1
                log(f"  [ERROR] Graph extraction failed on page {page_num}: {e}")
                _append_jsonl([{"source": f"page_{page_num}_graph", "error": str(e)}], failed_path)
                
            # Cleanup temp file
            if os.path.exists(temp_pdf_path):
                os.remove(temp_pdf_path)
                
            # Update progress
            progress = int(5 + ((page_idx + 1) / total_pages) * 85)
            _update_job(job_id, progress=progress, total_facts=total_facts)

        # Close main doc
        doc.close()

        _update_job(job_id, progress=90, total_facts=total_facts)

        # ── 3. Write CSV ──────────────────────────────────────
        rows = []
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))

        if rows:
            # New graph schema columns
            ordered_cols = [
                "document_name", "page_no", "fact_type", "stage", "chain", 
                "subject_name", "subject_type", "relation", 
                "object_name", "object_type", "evidence_text", "confidence",
                # The attributes will naturally be appended at the end as attr_*
            ]
            df = pd.json_normalize(rows)
            for col in ordered_cols:
                if col not in df.columns:
                    df[col] = None
            extra = [c for c in df.columns if c not in ordered_cols and c != "_source"]
            df = df[ordered_cols + extra]
            df.to_csv(csv_path, index=True, index_label="counter")

        log(f"[DONE] {total_facts} facts | {failed_calls} failed call(s)")
        _update_job(job_id,
            status="done", progress=100,
            total_facts=total_facts, failed_calls=failed_calls,
            finished_at=datetime.utcnow().isoformat()
        )

    except Exception as e:
        log(f"[FATAL] {e}")
        _update_job(job_id, status="error", error=str(e),
                    finished_at=datetime.utcnow().isoformat())


def start_pipeline(job_id: str, pdf_path: str, model: str = None):
    t = threading.Thread(target=run_pipeline, args=(job_id, pdf_path, model), daemon=True)
    t.start()
