import os
import sys
import io
import gc
import runpy
import logging
from contextlib import redirect_stdout, redirect_stderr
from flask import Flask, request, jsonify

app = Flask(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ALLOWED_SCRIPTS = {
    "all-leads.py",
    "enquires.py",
    "requirement_enquiries.py",
    "leads.py",
    "agents.py",
    "inventories-from-firebase.py",
    "new-inventory.py",
    "QC.py",
    "req.py",
    "connecthistory.py",
    "connecthistory_leads.py",
    "truestate-sync.py",
    "plan-upgrade.py",
    "qc-fix-upload.py",
    "update-plan.py",
}

REQUIRED_ENV_VARS = [
    "FIREBASE_PROJECT_ID", "FIREBASE_PRIVATE_KEY",
    "GSPREAD_PROJECT_ID", "GSPREAD_PRIVATE_KEY",
]


@app.route("/api/health", methods=["GET"])
def health():
    env_status = {v: bool(os.getenv(v)) for v in REQUIRED_ENV_VARS}
    return jsonify({"status": "ok", "env": env_status})


@app.route("/api/run", methods=["POST"])
def run_script():
    data = request.get_json(silent=True) or {}
    script_name = data.get("script", "").strip()

    if script_name not in ALLOWED_SCRIPTS:
        return jsonify({"success": False, "output": f"Unknown script: {script_name}"}), 400

    script_path = os.path.join(PROJECT_ROOT, script_name)
    if not os.path.exists(script_path):
        return jsonify({"success": False, "output": f"Script not found: {script_name}"}), 404

    missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
    if missing:
        return jsonify({
            "success": False,
            "output": f"Missing env vars: {', '.join(missing)}"
        }), 500

    # Clean stale logging handlers
    loggers = [logging.root] + [
        logging.getLogger(n) for n in logging.root.manager.loggerDict
    ]
    for lgr in loggers:
        for h in getattr(lgr, "handlers", [])[:]:
            if getattr(getattr(h, "stream", None), "closed", False):
                lgr.removeHandler(h)

    buf = io.StringIO()
    success = True
    error_msg = None

    try:
        sys.path.insert(0, PROJECT_ROOT)
        with redirect_stdout(buf), redirect_stderr(buf):
            runpy.run_path(script_path, run_name="__main__")
    except Exception as e:
        success = False
        error_msg = str(e)
    finally:
        # Remove handlers pointing at our buffer
        for lgr in [logging.root] + [logging.getLogger(n) for n in logging.root.manager.loggerDict]:
            for h in getattr(lgr, "handlers", [])[:]:
                if getattr(h, "stream", None) is buf:
                    lgr.removeHandler(h)
        gc.collect()

    output = buf.getvalue()
    buf.close()

    if not success:
        return jsonify({
            "success": False,
            "output": f"Error: {error_msg}\n\n{output}".strip()
        }), 500

    return jsonify({
        "success": True,
        "output": output.strip() or "Script completed with no output."
    })
