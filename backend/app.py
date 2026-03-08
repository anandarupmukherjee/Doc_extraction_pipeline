"""
app.py - Flask API server for the GSCO PDF extraction toolkit.
Serves the frontend static files and exposes REST/SSE endpoints.
"""

import json
import os
import time
import threading

from flask import Flask, request, jsonify, send_from_directory, Response, send_file
from flask_cors import CORS

from prompts import load_prompts_config, save_prompts_config
from llm_client import list_ollama_models, DEFAULT_MODEL
from pipeline import create_job, get_job, start_pipeline

UPLOAD_DIR = os.environ.get("UPLOAD_DIR", "/app/uploads")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/app/output")
FRONTEND_DIR = os.environ.get("FRONTEND_DIR", "/app/frontend")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

app = Flask(__name__)
CORS(app)

ALLOWED_EXTENSIONS = {"pdf"}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


# ─────────────────────────────────────────────────────────────
# Frontend static serving
# ─────────────────────────────────────────────────────────────

@app.route('/')
def home():
    return send_from_directory('frontend', 'index.html')


@app.route('/results')
def results():
    return send_from_directory('frontend', 'results.html')


@app.route('/graph')
def graph_ui():
    return send_from_directory('frontend', 'graph.html')


@app.route("/<path:filename>")
def serve_static(filename):
    """Serve any file (CSS, JS, images) from the frontend directory."""
    return send_from_directory(FRONTEND_DIR, filename)


# ─────────────────────────────────────────────────────────────
# API: Models
# ─────────────────────────────────────────────────────────────

@app.route("/api/models", methods=["GET"])
def api_models():
    models = list_ollama_models()
    return jsonify({"models": models, "default": DEFAULT_MODEL})


# ─────────────────────────────────────────────────────────────
# API: Prompts
# ─────────────────────────────────────────────────────────────

@app.route("/api/prompts", methods=["GET"])
def api_get_prompts():
    return jsonify(load_prompts_config())


@app.route("/api/prompts", methods=["PUT", "POST"])
def api_save_prompts():
    data = request.get_json(force=True)
    save_prompts_config(data)
    return jsonify({"status": "saved"})


@app.route("/api/prompts/reset", methods=["POST"])
def api_reset_prompts():
    # Save an empty dict so load_prompts_config will return the defaults
    save_prompts_config({})
    return jsonify({"status": "reset"})


# ─────────────────────────────────────────────────────────────
# API: Upload PDF
# ─────────────────────────────────────────────────────────────

@app.route("/api/upload", methods=["POST"])
def api_upload():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if f.filename == "" or not allowed_file(f.filename):
        return jsonify({"error": "Invalid file — must be a PDF"}), 400

    safe_name = f.filename.replace(" ", "_")
    dest = os.path.join(UPLOAD_DIR, safe_name)
    f.save(dest)
    return jsonify({"filename": safe_name, "path": dest})


# ─────────────────────────────────────────────────────────────
# API: Run pipeline
# ─────────────────────────────────────────────────────────────

@app.route("/api/run", methods=["POST"])
def api_run():
    data = request.get_json(force=True)
    filename = data.get("filename")
    model = data.get("model") or DEFAULT_MODEL

    if not filename:
        return jsonify({"error": "filename is required"}), 400

    pdf_path = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(pdf_path):
        return jsonify({"error": f"File not found: {filename}"}), 404

    job_id = create_job()
    start_pipeline(job_id, pdf_path, model=model)
    return jsonify({"job_id": job_id})


# ─────────────────────────────────────────────────────────────
# API: Status (SSE stream)
# ─────────────────────────────────────────────────────────────

@app.route("/api/status/<job_id>")
def api_status_stream(job_id):
    """Server-Sent Events stream for live log output."""
    def event_stream():
        sent_log_idx = 0
        while True:
            job = get_job(job_id)
            if not job:
                yield f"data: {json.dumps({'error': 'job not found'})}\n\n"
                break

            logs = job.get("logs", [])
            new_logs = logs[sent_log_idx:]
            sent_log_idx = len(logs)

            for msg in new_logs:
                yield f"data: {json.dumps({'log': msg, 'progress': job['progress'], 'total_facts': job['total_facts']})}\n\n"

            # Send periodic heartbeat / progress ping even with no new logs
            if not new_logs:
                yield f"data: {json.dumps({'ping': True, 'progress': job['progress'], 'status': job['status'], 'total_facts': job['total_facts']})}\n\n"

            if job["status"] in ("done", "error"):
                yield f"data: {json.dumps({'status': job['status'], 'progress': job['progress'], 'total_facts': job['total_facts'], 'error': job.get('error')})}\n\n"
                break

            time.sleep(1)

    return Response(event_stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ─────────────────────────────────────────────────────────────
# API: Results
# ─────────────────────────────────────────────────────────────

@app.route("/api/results/<job_id>")
def api_results(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    if job["status"] not in ("done",):
        return jsonify({"error": "job not finished", "status": job["status"]}), 400

    jsonl_path = job.get("jsonl_path")
    if not jsonl_path or not os.path.exists(jsonl_path):
        return jsonify({"facts": []})

    facts = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                facts.append(json.loads(line))

    return jsonify({
        "facts": facts, 
        "total": len(facts),
        "summary": job.get("summary", ""),
        "limitations": job.get("limitations", "")
    })


@app.route("/api/results/<job_id>/csv")
def api_results_csv(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404

    csv_path = job.get("csv_path")
    if not csv_path or not os.path.exists(csv_path):
        return jsonify({"error": "CSV not ready"}), 404

    return send_file(csv_path, as_attachment=True, download_name="extraction_results.csv")


@app.route("/api/jobs/<job_id>")
def api_job_info(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({"error": "not found"}), 404
    return jsonify(job)


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, threaded=True, debug=False)
