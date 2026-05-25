import streamlit as st
import os
import time
import sys
import gc
import runpy
import io
from contextlib import redirect_stdout, redirect_stderr
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PAGE_TITLE = "ACN Command Center"
PAGE_ICON = "⚡"

st.set_page_config(
    page_title=PAGE_TITLE, 
    layout="wide", 
    initial_sidebar_state="collapsed", 
    page_icon=PAGE_ICON
)

# -----------------------------------------------------------------------------
# Business Logic
# -----------------------------------------------------------------------------

def run_script(script_name: str, status_container, db: str = "new"):
    """Executes a python script in the current process to save memory."""
    path = os.path.join(os.getcwd(), script_name)

    if not os.path.exists(path):
        status_container.error(f"❌ Script not found: {script_name}")
        return

    # Environment Check (use correct prefix based on db selection)
    prefix = "NEW_" if db == "new" else ""
    required_vars = [
        f"{prefix}FIREBASE_PROJECT_ID", f"{prefix}FIREBASE_PRIVATE_KEY",
        "GSPREAD_PROJECT_ID", "GSPREAD_PRIVATE_KEY"
    ]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        status_container.warning(f"⚠️ Missing Env Vars: {', '.join(missing)}")
        return

    # Clean up any stale/closed logging handlers from previous runs
    import logging
    loggers = [logging.root] + [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for logr in loggers:
        if hasattr(logr, 'handlers'):
            for handler in logr.handlers[:]:
                stream = getattr(handler, 'stream', None)
                if stream and getattr(stream, 'closed', False):
                    logr.removeHandler(handler)

    start_time = time.time()
    
    try:
        status_container.write("⏳ Processing in-memory... Please wait.")
        log_placeholder = status_container.empty()
        
        class LiveStream(io.StringIO):
            def __init__(self):
                super().__init__()
                self.last_update = time.time()
                
            def write(self, s):
                super().write(s)
                now = time.time()
                # Update UI every 0.5s to prevent Streamlit from choking
                if now - self.last_update > 0.5:
                    log_placeholder.code(self.getvalue() or "...")
                    self.last_update = now

        # Use the same live stream for stdout and stderr to capture all logs
        f_combined = LiveStream()
        
        # Reset Firebase apps so each script re-initializes with correct credentials
        import firebase_admin
        for _app in list(firebase_admin._apps.values()):
            firebase_admin.delete_app(_app)

        # Inject --db flag so scripts' argparse picks up the DB selection
        original_argv = sys.argv[:]
        sys.argv = [sys.argv[0], "--db", db]

        # Redirect stdout/stderr and run the script within the SAME Python process
        try:
            with redirect_stdout(f_combined), redirect_stderr(f_combined):
                runpy.run_path(path, run_name="__main__")
        finally:
            sys.argv = original_argv
            
        duration = time.time() - start_time
        
        # Final update
        output_text = f_combined.getvalue()
        if output_text.strip():
            log_placeholder.code(output_text)
        else:
            log_placeholder.write("Script executed successfully with no output log.")

        
        status_container.update(label=f"✅ {script_name} completed in {duration:.2f}s", state="complete", expanded=False)
            
    except Exception as e:
        status_container.error(f"💥 Execution Error: {str(e)}")
        if 'f_combined' in locals():
            err_text = f_combined.getvalue()
            if err_text.strip():
                if 'log_placeholder' in locals():
                    log_placeholder.code(f"Error Log:\n{err_text}")
                else:
                    status_container.code(f"Error Log:\n{err_text}")
    finally:
        # Crucial for Free Tier: Force memory cleanup and close string buffers
        import logging
        if 'f_combined' in locals():
            # Remove any handlers that captured this stream to prevent 'I/O operation on closed file'
            loggers = [logging.root] + [logging.getLogger(name) for name in logging.root.manager.loggerDict]
            for logr in loggers:
                if hasattr(logr, 'handlers'):
                    for handler in logr.handlers[:]:
                        if getattr(handler, 'stream', None) is f_combined:
                            logr.removeHandler(handler)
            f_combined.close()
        gc.collect()

# -----------------------------------------------------------------------------
# UI Layout
# -----------------------------------------------------------------------------

_title_col, _toggle_col = st.columns([6, 1])
with _title_col:
    st.title("⚡ ACN Command Center")
    st.markdown("Manage and execute your CRM scripts.")
with _toggle_col:
    _use_new_db = st.toggle("New DB", value=True)
    db_selection = "new" if _use_new_db else "old"
st.divider()

# --- Section 1: Leads & Growth ---
st.subheader("🚀 Leads & Growth")
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("👥 Sync All Leads", use_container_width=True):
        with st.status("Running All Leads Sync...", expanded=True) as status:
            run_script("all-leads.py", status, db=db_selection)
            
    if st.button("❓ Sync Enquiries", use_container_width=True):
        with st.status("Running Enquiries Sync...", expanded=True) as status:
            run_script("enquires.py", status, db=db_selection)

with col2:
    if st.button("🎯 Sync Req. Enquiries", use_container_width=True):
        with st.status("Running Requirement Enquiries Sync...", expanded=True) as status:
            run_script("requirement_enquiries.py", status, db=db_selection)
    
    if st.button("🔐 Sync Tried Access", use_container_width=True):
        with st.status("Running Tried Access Sync...", expanded=True) as status:
            run_script("leads.py", status, db=db_selection)

with col3:
    if st.button("🛡️ Sync Agents", use_container_width=True):
        with st.status("Running Agents Sync...", expanded=True) as status:
            run_script("agents.py", status, db=db_selection)

st.divider()

# --- Section 2: Inventory ---
st.subheader("🏢 Inventory Management")
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("📦 Sync Inventories", use_container_width=True):
        with st.status("Running Inventories Sync...", expanded=True) as status:
            run_script("inventories-from-firebase.py", status, db=db_selection)

with col2:
    if st.button("🆕 Sync New Inventory", use_container_width=True):
        with st.status("Running New Inventory Sync...", expanded=True) as status:
            run_script("new-inventory.py", status, db=db_selection)

with col3:
    if st.button("🔍 Sync QC Properties", use_container_width=True):
        with st.status("Running QC Sync...", expanded=True) as status:
            run_script("QC.py", status, db=db_selection)

st.divider()

# --- Section 3: System ---
st.subheader("⚙️ Data & System")
col1, col2 ,col3,col4= st.columns(4)

with col1:
    if st.button("📋 Sync Requirements", use_container_width=True):
        with st.status("Running Requirements Sync...", expanded=True) as status:
            run_script("req.py", status, db=db_selection)
            
with col2:
    if st.button("📞 Sync Agents Call History", use_container_width=True):
        with st.status("Running Call History Sync...", expanded=True) as status:
            run_script("connecthistory.py", status, db=db_selection)

with col3:
    if st.button("📞 Sync Leads Call History", use_container_width=True):
        with st.status("Running Call History Sync...", expanded=True) as status:
            run_script("connecthistory_leads.py", status, db=db_selection)
with col4:
    if st.button("📞 Sync TrueState Apex", use_container_width=True):
        with st.status("Running TrueState Apex Sync...", expanded=True) as status:
            run_script("truestate-sync.py", status, db=db_selection)

# Footer / Info
st.caption(f"Environment: Single-Process Setup | Python: {sys.version.split()[0]}")