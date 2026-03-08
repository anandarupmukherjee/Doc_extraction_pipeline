# GSCO Standalone Extraction Toolkit

A fully **dockerized** PDF supply chain fact extraction pipeline, using **Ollama** (running on your local machine) as the LLM backend, with a **browser-based UI** for prompt editing and result viewing.

## Architecture

```
Browser (:5000)
   ├── Control Panel  (upload PDF, edit prompts, run pipeline, live log)
   └── Results Page   (filter / search extracted facts, download CSV)

Docker Container (port 5000)
   └── Flask API
         ├── PDF extraction  (PyMuPDF + pdfplumber)
         ├── Chunking + classification
         └── Fact extraction → JSONL + CSV

Host Machine
   └── Ollama (port 11434)  ←── deepseek-v3.1:671b-cloud (cloud, no download)
```

## Requirements

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- [Ollama](https://ollama.com/) running on your Mac (`ollama serve`)

## Quick Start

```bash
# 1. Build and start the container
cd /Users/anandarupmukherjee/Desktop/PILOTS/GSCO_standalone_toolkit
docker compose up --build

# 2. Open the browser
open http://localhost:5000
```

## Usage

1. **Upload PDF** — drag & drop or browse a PDF file on the Control Panel
2. **Select model** — defaults to `deepseek-v3.1:671b-cloud`
3. **Edit prompts** (optional) — click any tab to edit the classifier or extraction prompts, then click **Save Prompts**
4. **Run** — click **Start Pipeline**. Watch live log and progress bar.
5. **View Results** — click the **View Results →** button when done, or navigate to `http://localhost:5000/results`
6. **Download CSV** — click **⬇ Download CSV** on the Results page

## Output Files

Output is written to `./output/` (mounted into the container):

| File | Contents |
|------|----------|
| `facts_<id>_<ts>.jsonl` | Extracted facts (one JSON per line) |
| `facts_<id>_<ts>.csv`  | Same data as a flat CSV |
| `raw_<id>_<ts>.jsonl`  | Raw LLM responses (for debugging) |
| `failed_<id>_<ts>.jsonl` | Chunks/tables that errored |

## Editing Prompts Without Rebuilding

The `prompts_config.json` file is mounted into the container. Any changes saved via the **UI** are immediately written to this file and picked up by the next pipeline run — **no rebuild needed**.

## Project Structure

```
GSCO_standalone_toolkit/
├── Dockerfile
├── docker-compose.yml
├── prompts_config.json          ← editable prompt templates
├── backend/
│   ├── app.py                   ← Flask API
│   ├── pipeline.py              ← orchestrator + background runner
│   ├── extractor.py             ← PDF → pages
│   ├── chunker.py               ← prose chunking
│   ├── prompts.py               ← prompt builders + JSON parser
│   ├── llm_client.py            ← Ollama HTTP client
│   └── requirements.txt
├── frontend/
│   ├── index.html               ← Control Panel
│   ├── results.html             ← Results Viewer
│   ├── style.css
│   └── app.js
├── uploads/                     ← uploaded PDFs (auto-created)
└── output/                      ← extracted facts (auto-created)
```

## Changing the Model

Edit `docker-compose.yml` and change `OLLAMA_MODEL`, then restart:

```bash
docker compose down && docker compose up
```

Or just change the model in the UI dropdown (populated from `/api/models`).
