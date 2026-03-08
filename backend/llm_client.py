"""
llm_client.py - Ollama API client.
Supports plain text, vision (images), and direct PDF file uploads
via Ollama's 'files' message field (same as attaching a PDF in the UI).
"""

import base64
import os
import requests

OLLAMA_HOST   = os.environ.get("OLLAMA_HOST",   "http://host.docker.internal:11434")
DEFAULT_MODEL = os.environ.get("OLLAMA_MODEL",  "deepseek-v3.1:671b-cloud")


def call_ollama(
    system_prompt: str,
    user_prompt: str,
    model: str = None,
    temperature: float = 0.0,
    images: list = None,    # base64 PNG strings — kept for compatibility
    pdf_path: str = None,   # path to a PDF file to attach directly
) -> str:
    """
    Send a chat request to Ollama.

    Attachment priority:
      1. pdf_path  → attach the raw PDF via Ollama's 'files' field
                     (identical to dragging a PDF onto the Ollama UI)
      2. images    → base64 PNG image list (legacy / fallback)
      3. neither   → plain text exchange
    """
    model = model or DEFAULT_MODEL
    url   = f"{OLLAMA_HOST}/api/chat"

    user_message: dict = {"role": "user", "content": user_prompt}

    if pdf_path:
        # Read and base64-encode the raw PDF bytes
        with open(pdf_path, "rb") as fh:
            pdf_b64 = base64.b64encode(fh.read()).decode("utf-8")
        # Ollama files API — same as attaching a file in the web UI
        user_message["files"] = [
            {"data": pdf_b64, "content_type": "application/pdf"}
        ]
    elif images:
        user_message["images"] = images

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            user_message,
        ],
        "stream": False,
        "options": {"temperature": temperature},
    }

    try:
        resp = requests.post(url, json=payload, timeout=600)   # 10 min for large PDFs
        resp.raise_for_status()
        data = resp.json()
        return data.get("message", {}).get("content", "").strip()
    except requests.exceptions.Timeout:
        raise RuntimeError("Ollama request timed out after 10 min")
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f"Cannot connect to Ollama at {OLLAMA_HOST}: {e}")
    except Exception as e:
        raise RuntimeError(f"Ollama error: {e}")


def list_ollama_models() -> list:
    """Return list of model names available in local Ollama."""
    url = f"{OLLAMA_HOST}/api/tags"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return [m["name"] for m in resp.json().get("models", [])]
    except Exception:
        return []
