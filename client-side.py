import streamlit as st
import subprocess
import os
import time
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
PYTHON_EXECUTABLE = sys.executable
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

def run_script(script_name: str, status_container):
    """Executes a python script with live status updates."""
    path = os.path.join(os.getcwd(), script_name)
    
    if not os.path.exists(path):
        status_container.error(f"❌ Script not found: {script_name}")
        return
    
    # Environment Check
    required_vars = [
        "FIREBASE_PROJECT_ID", "FIREBASE_PRIVATE_KEY", "GSPREAD_PROJECT_ID", "GSPREAD_PRIVATE_KEY"
    ]
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        status_container.warning(f"⚠️ Missing Env Vars: {', '.join(missing)}")
        return

    start_time = time.time()
    terminal_output = []
    
    try:
        # Prepare environment
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"

        process = subprocess.Popen(
            [PYTHON_EXECUTABLE, path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, # Merge stderr into stdout
            text=True,
            bufsize=1, # Line buffered
            env=env
        )
        
        # Live Stream inside st.status
        while True:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break
            
            if line:
                clean_line = line.strip()
                terminal_output.append(clean_line)
                # Write to the expandable status area
                status_container.write(clean_line)

        # Final Cleanup
        return_code = process.poll()
        duration = time.time() - start_time
        
        if return_code == 0:
            status_container.update(label=f"✅ {script_name} completed in {duration:.2f}s", state="complete", expanded=False)
        else:
            status_container.update(label=f"❌ {script_name} failed (Exit Code: {return_code})", state="error", expanded=True)
            
    except Exception as e:
        status_container.error(f"💥 Execution Error: {str(e)}")

# -----------------------------------------------------------------------------
# UI Layout
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# UI Layout
# -----------------------------------------------------------------------------

st.title("⚡ ACN Command Center")
st.markdown("Manage and execute your CRM scripts.")
st.divider()

# --- Section 1: Leads & Growth ---
st.subheader("🚀 Leads & Growth")
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("👥 Sync All Leads", use_container_width=True):
        with st.status("Running All Leads Sync...", expanded=True) as status:
            run_script("all-leads.py", status)
            
    if st.button("❓ Sync Enquiries", use_container_width=True):
        with st.status("Running Enquiries Sync...", expanded=True) as status:
            run_script("enquires.py", status)

with col2:
    if st.button("🎯 Sync Req. Enquiries", use_container_width=True):
        with st.status("Running Requirement Enquiries Sync...", expanded=True) as status:
            run_script("requirement_enquiries.py", status)
    
    if st.button("🔐 Sync Tried Access", use_container_width=True):
        with st.status("Running Tried Access Sync...", expanded=True) as status:
            run_script("leads.py", status)

with col3:
    if st.button("🛡️ Sync Agents", use_container_width=True):
        with st.status("Running Agents Sync...", expanded=True) as status:
            run_script("agents.py", status)

st.divider()

# --- Section 2: Inventory ---
st.subheader("🏢 Inventory Management")
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("📦 Sync Inventories", use_container_width=True):
        with st.status("Running Inventories Sync...", expanded=True) as status:
            run_script("inventories-from-firebase.py", status)

with col2:
    if st.button("🆕 Sync New Inventory", use_container_width=True):
        with st.status("Running New Inventory Sync...", expanded=True) as status:
            run_script("new-inventory.py", status)

with col3:
    if st.button("🔍 Sync QC Properties", use_container_width=True):
        with st.status("Running QC Sync...", expanded=True) as status:
            run_script("QC.py", status)

st.divider()

# --- Section 3: System ---
st.subheader("⚙️ Data & System")
col1, col2 = st.columns(2)

with col1:
    if st.button("📋 Sync Requirements", use_container_width=True):
        with st.status("Running Requirements Sync...", expanded=True) as status:
            run_script("req.py", status)
            
with col2:
    if st.button("📞 Sync Call History", use_container_width=True):
        with st.status("Running Call History Sync...", expanded=True) as status:
            run_script("connecthistory.py", status)

# Footer / Info
st.caption(f"Environment: {os.getcwd()} | Python: {sys.version.split()[0]}")