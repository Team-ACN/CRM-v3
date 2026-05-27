from google.cloud.firestore_v1 import bulk_writer
from google.cloud.firestore_v1 import bulk_writer
from google.cloud.firestore_v1 import bulk_writer
import streamlit as st
import os
import time
import sys
import gc
import runpy
import io
from contextlib import redirect_stdout, redirect_stderr
from dotenv import load_dotenv

load_dotenv()

PAGE_TITLE = "ACN Command Center"
PAGE_ICON  = "⚡"

st.set_page_config(
    page_title=PAGE_TITLE,
    layout="wide",
    initial_sidebar_state="collapsed",
    page_icon=PAGE_ICON,
)

# -----------------------------------------------------------------------------
# CSS
# -----------------------------------------------------------------------------
st.markdown("""
<style>
/* ── Page background ───────────────────────────────── */
.stApp { background: #0f1117; }

/* ── Top header bar ────────────────────────────────── */
.cmd-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: linear-gradient(135deg, #1a1d2e 0%, #16213e 100%);
    border: 1px solid #2a2d3e;
    border-radius: 12px;
    padding: 1rem 1.5rem;
    margin-bottom: 1.5rem;
}
.cmd-title { font-size: 1.6rem; font-weight: 700; color: #e2e8f0; margin: 0; }
.cmd-subtitle { font-size: 0.8rem; color: #64748b; margin: 0; }
.db-pill-new {
    background: linear-gradient(135deg, #059669, #10b981);
    color: #fff;
    padding: 4px 14px;
    border-radius: 20px;
    font-size: 0.78rem;
    font-weight: 700;
    letter-spacing: .5px;
    box-shadow: 0 0 10px rgba(16,185,129,.35);
}
.db-pill-old {
    background: linear-gradient(135deg, #d97706, #f59e0b);
    color: #fff;
    padding: 4px 14px;
    border-radius: 20px;
    font-size: 0.78rem;
    font-weight: 700;
    letter-spacing: .5px;
    box-shadow: 0 0 10px rgba(245,158,11,.35);
}

/* ── Section cards ─────────────────────────────────── */
.section-card {
    background: #1a1d2e;
    border: 1px solid #2a2d3e;
    border-radius: 10px;
    padding: 1rem 1.2rem 0.5rem;
    margin-bottom: 1.2rem;
}
.section-label {
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    color: #64748b;
    margin-bottom: 0.7rem;
    display: flex;
    align-items: center;
    gap: 6px;
}

/* ── Button captions ───────────────────────────────── */
.btn-caption {
    font-size: 0.72rem;
    color: #475569;
    text-align: center;
    margin-top: -10px;
    margin-bottom: 8px;
    line-height: 1.3;
}

/* ── Streamlit button overrides ────────────────────── */
div.stButton > button {
    background: #1e2235;
    border: 1px solid #2d3148;
    color: #cbd5e1;
    border-radius: 8px;
    font-size: 0.85rem;
    font-weight: 500;
    padding: 0.55rem 0.5rem;
    transition: all .18s ease;
    width: 100%;
}
div.stButton > button:hover {
    background: #262b44;
    border-color: #4f5d8a;
    color: #e2e8f0;
    transform: translateY(-1px);
    box-shadow: 0 4px 12px rgba(0,0,0,.4);
}
div.stButton > button:active {
    transform: translateY(0);
    background: #1a1f35;
}

/* ── Toggle ────────────────────────────────────────── */
div[data-testid="stToggle"] label {
    font-size: 0.82rem;
    color: #94a3b8;
}

/* ── Footer ────────────────────────────────────────── */
.cmd-footer {
    text-align: center;
    color: #334155;
    font-size: 0.72rem;
    margin-top: 2rem;
    padding-top: 1rem;
    border-top: 1px solid #1e2235;
}

/* ── Status boxes ──────────────────────────────────── */
div[data-testid="stExpander"] {
    background: #13151f;
    border: 1px solid #2a2d3e;
    border-radius: 8px;
}
</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Business Logic
# -----------------------------------------------------------------------------

def run_script(script_name: str, status_container, db: str = "new"):
    """Executes a python script in the current process to save memory."""
    path = os.path.join(os.getcwd(), script_name)

    if not os.path.exists(path):
        status_container.error(f"❌ Script not found: {script_name}")
        return

    prefix = "NEW_" if db == "new" else ""
    required_vars = [
        f"{prefix}FIREBASE_PROJECT_ID", f"{prefix}FIREBASE_PRIVATE_KEY",
        "GSPREAD_PROJECT_ID", "GSPREAD_PRIVATE_KEY",
    ]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        status_container.warning(f"⚠️ Missing Env Vars: {', '.join(missing)}")
        return

    import logging
    loggers = [logging.root] + [logging.getLogger(n) for n in logging.root.manager.loggerDict]
    for logr in loggers:
        if hasattr(logr, "handlers"):
            for handler in logr.handlers[:]:
                stream = getattr(handler, "stream", None)
                if stream and getattr(stream, "closed", False):
                    logr.removeHandler(handler)

    start_time = time.time()

    try:
        status_container.write("⏳ Processing… please wait.")
        log_placeholder = status_container.empty()

        class LiveStream(io.StringIO):
            def __init__(self):
                super().__init__()
                self.last_update = time.time()

            def write(self, s):
                super().write(s)
                now = time.time()
                if now - self.last_update > 0.5:
                    log_placeholder.code(self.getvalue() or "...")
                    self.last_update = now

        f_combined = LiveStream()

        import firebase_admin
        for _app in list(firebase_admin._apps.values()):
            firebase_admin.delete_app(_app)

        original_argv = sys.argv[:]
        sys.argv = [sys.argv[0], "--db", db]

        try:
            with redirect_stdout(f_combined), redirect_stderr(f_combined):
                runpy.run_path(path, run_name="__main__")
        finally:
            sys.argv = original_argv

        duration = time.time() - start_time
        output_text = f_combined.getvalue()
        if output_text.strip():
            log_placeholder.code(output_text)
        else:
            log_placeholder.write("Script executed successfully with no output log.")

        status_container.update(
            label=f"✅ {script_name} completed in {duration:.2f}s",
            state="complete",
            expanded=False,
        )

    except Exception as e:
        status_container.error(f"💥 Execution Error: {str(e)}")
        if "f_combined" in locals():
            err_text = f_combined.getvalue()
            if err_text.strip():
                target = log_placeholder if "log_placeholder" in locals() else status_container
                target.code(f"Error Log:\n{err_text}")
    finally:
        import logging
        if "f_combined" in locals():
            loggers = [logging.root] + [logging.getLogger(n) for n in logging.root.manager.loggerDict]
            for logr in loggers:
                if hasattr(logr, "handlers"):
                    for handler in logr.handlers[:]:
                        if getattr(handler, "stream", None) is f_combined:
                            logr.removeHandler(handler)
            f_combined.close()
        gc.collect()


# -----------------------------------------------------------------------------
# Header
# -----------------------------------------------------------------------------
hdr_left, hdr_right = st.columns([5, 1])
with hdr_left:
    st.markdown("""
        <div style="padding: 0.8rem 0 0.2rem;">
            <p class="cmd-title">⚡ ACN Command Center</p>
            <p class="cmd-subtitle">Firestore → Google Sheets sync pipeline</p>
        </div>
    """, unsafe_allow_html=True)
with hdr_right:
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    _use_new_db  = st.toggle("New DB", value=True, key="db_toggle")
    db_selection = "new" if _use_new_db else "old"
    pill_cls     = "db-pill-new" if _use_new_db else "db-pill-old"
    pill_lbl     = "● NEW DB" if _use_new_db else "● OLD DB"
    st.markdown(f'<div class="{pill_cls}">{pill_lbl}</div>', unsafe_allow_html=True)

st.markdown("---")


# -----------------------------------------------------------------------------
# Section 1 — Leads & Growth
# -----------------------------------------------------------------------------
st.markdown('<p class="section-label">🚀 &nbsp;Leads & Growth</p>', unsafe_allow_html=True)
c1, c2, c3, c4 = st.columns(4)

with c1:
    if st.button("👥 All Leads", use_container_width=True):
        with st.status("Syncing All Leads…", expanded=True) as s:
            run_script("all-leads.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">All lead records → Sheet</p>', unsafe_allow_html=True)

with c2:
    if st.button("❓ Enquiries", use_container_width=True):
        with st.status("Syncing Enquiries…", expanded=True) as s:
            run_script("enquires.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">Buyer enquiry data → Sheet</p>', unsafe_allow_html=True)

with c3:
    if st.button("🎯 Req. Enquiries", use_container_width=True):
        with st.status("Syncing Requirement Enquiries…", expanded=True) as s:
            run_script("requirement_enquiries.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">Requirement enquiries → Sheet</p>', unsafe_allow_html=True)

with c4:
    if st.button("🔐 Tried Access", use_container_width=True):
        with st.status("Syncing Tried Access…", expanded=True) as s:
            run_script("leads.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">Unauthorised access attempts</p>', unsafe_allow_html=True)


st.markdown("---")


# -----------------------------------------------------------------------------
# Section 2 — Inventory
# -----------------------------------------------------------------------------
st.markdown('<p class="section-label">🏢 &nbsp;Inventory Management</p>', unsafe_allow_html=True)
c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    if st.button("📦 Inventories", use_container_width=True):
        with st.status("Syncing Inventories…", expanded=True) as s:
            run_script("inventories-from-firebase.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">Main inventory sheet sync</p>', unsafe_allow_html=True)

with c2:
    if st.button("🆕 New Inventory", use_container_width=True):
        with st.status("Syncing New Inventory…", expanded=True) as s:
            run_script("new-inventory.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">New format inventory sheet</p>', unsafe_allow_html=True)

with c3:
    if st.button("🆕 New Inventory 2", use_container_width=True):
        with st.status("Syncing New Inventory 2…", expanded=True) as s:
            run_script("new-inventory-2.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">Alternate inventory sheet</p>', unsafe_allow_html=True)

with c4:
    if st.button("🔍 QC Properties", use_container_width=True):
        with st.status("Syncing QC Properties…", expanded=True) as s:
            run_script("QC.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">QC-reviewed properties</p>', unsafe_allow_html=True)

with c5:
    if st.button("🆕 Product analysis", use_container_width=True):
        with st.status("Syncing Product analysis…", expanded=True) as s:
            run_script("new-inventory-2.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">Product analysis sheet</p>', unsafe_allow_html=True)

st.markdown("---")


# -----------------------------------------------------------------------------
# Section 3 — System & Data
# -----------------------------------------------------------------------------
st.markdown('<p class="section-label">⚙️ &nbsp;System & Data</p>', unsafe_allow_html=True)
c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    if st.button("📋 Requirements", use_container_width=True):
        with st.status("Syncing Requirements…", expanded=True) as s:
            run_script("req.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">Buyer requirement records</p>', unsafe_allow_html=True)

with c2:
    if st.button("🛡️ Agents", use_container_width=True):
        with st.status("Syncing Agents…", expanded=True) as s:
            run_script("agents.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">Agent roster → Sheet</p>', unsafe_allow_html=True)

with c3:
    if st.button("📞 Agents Calls", use_container_width=True):
        with st.status("Syncing Agent Call History…", expanded=True) as s:
            run_script("connecthistory.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">Agent call history</p>', unsafe_allow_html=True)

with c4:
    if st.button("📞 Leads Calls", use_container_width=True):
        with st.status("Syncing Leads Call History…", expanded=True) as s:
            run_script("connecthistory_leads.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">Lead call history</p>', unsafe_allow_html=True)

with c5:
    if st.button("🔗 TrueState Apex", use_container_width=True):
        with st.status("Syncing TrueState Apex…", expanded=True) as s:
            run_script("truestate-sync.py", s, db=db_selection)
    st.markdown('<p class="btn-caption">Apex CRM sync</p>', unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Footer
# -----------------------------------------------------------------------------
st.markdown(
    f'<p class="cmd-footer">Single-process pipeline &nbsp;|&nbsp; Python {sys.version.split()[0]} &nbsp;|&nbsp; DB: {"New ✦" if db_selection == "new" else "Old ◆"}</p>',
    unsafe_allow_html=True,
)
