import streamlit as st
import subprocess
import os
import time
import sys
from dotenv import load_dotenv
import logging

# Configure logging for Render
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get the Python interpreter path
PYTHON_EXECUTABLE = sys.executable

# Streamlit page configuration optimized for Render
st.set_page_config(
    page_title="ACN Script Runner", 
    layout="wide", 
    initial_sidebar_state="collapsed", 
    page_icon="🎯"
)

# Professional enterprise theme
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');
    
    .stApp { 
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #334155 100%);
        font-family: 'Inter', sans-serif;
    }
    
    .main-header {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        border: 1px solid rgba(148, 163, 184, 0.1);
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 16px;
        box-shadow: 0 8px 20px rgba(0, 0, 0, 0.2);
        text-align: center;
        position: relative;
        overflow: hidden;
    }
    
    .main-header::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 2px;
        background: linear-gradient(90deg, #3b82f6, #8b5cf6, #06b6d4, #10b981);
    }
    
    .header-icon {
        font-size: 32px;
        margin-bottom: 8px;
        display: block;
    }
    
    .header-title {
        font-size: 28px;
        font-weight: 700;
        color: #ffffff;
        margin: 0 0 6px 0;
        letter-spacing: -0.02em;
    }
    
    .header-subtitle {
        font-size: 14px;
        color: #94a3b8;
        margin: 0;
        font-weight: 400;
    }
    
    .section-card {
        background: rgba(30, 41, 59, 0.6);
        border: 1px solid rgba(148, 163, 184, 0.1);
        border-radius: 12px;
        padding: 16px;
        margin-bottom: 12px;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
        backdrop-filter: blur(10px);
    }
    
    .section-title {
        font-size: 18px;
        font-weight: 600;
        color: #ffffff;
        margin: 0 0 12px 0;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    
    .section-title::before {
        content: '';
        width: 4px;
        height: 24px;
        background: linear-gradient(135deg, #3b82f6, #8b5cf6);
        border-radius: 2px;
    }
    
    .stButton button {
        background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%);
        border: none;
        color: #ffffff;
        border-radius: 8px;
        padding: 10px 20px;
        font-weight: 600;
        font-size: 14px;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 2px 8px rgba(59, 130, 246, 0.3);
        text-transform: none;
        letter-spacing: 0;
        width: 100%;
        margin-bottom: 6px;
    }
    
    .stButton button:hover {
        background: linear-gradient(135deg, #1d4ed8 0%, #1e40af 100%);
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(59, 130, 246, 0.4);
    }
    
    .stButton button:active {
        transform: translateY(0);
        box-shadow: 0 4px 16px rgba(59, 130, 246, 0.3);
    }
    
    .stTextArea textarea {
        background: rgba(15, 23, 42, 0.8) !important;
        border: 1px solid rgba(148, 163, 184, 0.2) !important;
        border-radius: 12px !important;
        color: #e2e8f0 !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 14px !important;
        padding: 16px !important;
        line-height: 1.6 !important;
    }
    
    .stTextArea textarea:focus {
        border-color: #3b82f6 !important;
        box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1) !important;
    }
    
    h1, h2, h3 {
        font-family: 'Inter', sans-serif !important;
        color: #ffffff !important;
        font-weight: 600 !important;
        letter-spacing: -0.01em !important;
    }
    
    p {
        color: #cbd5e0;
        line-height: 1.6;
    }
    
    code {
        color: #06b6d4;
        background: rgba(6, 182, 212, 0.1);
        padding: 4px 8px;
        border-radius: 6px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 14px;
        border: 1px solid rgba(6, 182, 212, 0.2);
    }
    
    .env-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 8px;
        margin-top: 12px;
    }
    
    .env-item {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 8px 12px;
        background: rgba(15, 23, 42, 0.5);
        border: 1px solid rgba(148, 163, 184, 0.1);
        border-radius: 6px;
        font-size: 12px;
    }
    
    .env-status {
        width: 8px;
        height: 8px;
        border-radius: 50%;
    }
    
    .env-status.success {
        background: #10b981;
    }
    
    .env-status.error {
        background: #ef4444;
    }
    
    .footer {
        background: rgba(15, 23, 42, 0.8);
        border: 1px solid rgba(148, 163, 184, 0.1);
        border-radius: 8px;
        padding: 12px;
        margin-top: 16px;
        text-align: center;
    }
    
    .footer-text {
        color: #64748b;
        font-size: 14px;
        margin: 0;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: rgba(15, 23, 42, 0.5);
    }
    
    ::-webkit-scrollbar-thumb {
        background: rgba(148, 163, 184, 0.3);
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: rgba(148, 163, 184, 0.5);
    }
    </style>
""", unsafe_allow_html=True)

# Function to run external scripts with better error handling
def run_script(script_name):
    path = os.path.join(os.getcwd(), script_name)
    if not os.path.exists(path):
        return f"⚠️ Script not found: `{script_name}`\n\nPath checked: {path}"
    
    # Check for required environment variables
    required_env_vars = [
        "FIREBASE_PROJECT_ID", "FIREBASE_PRIVATE_KEY_ID", "FIREBASE_PRIVATE_KEY",
        "FIREBASE_CLIENT_EMAIL", "FIREBASE_CLIENT_ID",
        "GSPREAD_PROJECT_ID", "GSPREAD_PRIVATE_KEY_ID", "GSPREAD_PRIVATE_KEY",
        "GSPREAD_CLIENT_EMAIL", "GSPREAD_CLIENT_ID"
    ]
    
    missing_vars = []
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        return f"❌ Missing required environment variables: {', '.join(missing_vars)}\n\nPlease ensure all required environment variables are set in your Render dashboard\n\nCurrent working directory: {os.getcwd()}"
    
    start = time.time()
    try:
        with st.spinner(f"⚡ Executing {script_name}"):
            # Set environment variables for better subprocess handling
            env = os.environ.copy()
            env.update({
                "PYTHONUTF8": "1",
                "PYTHONIOENCODING": "utf-8",
                "PYTHONUNBUFFERED": "1"
            })
            
            # Run the script with improved error handling and shorter timeout for Render
            proc = subprocess.run(
                [PYTHON_EXECUTABLE, path], 
                capture_output=True, 
                text=True, 
                encoding="utf-8", 
                env=env,
                timeout=180  # 3 minute timeout for Render free tier
            )
        
        dura = round(time.time() - start, 2)
        
        # Process output
        stdout = proc.stdout.strip() if proc.stdout else ""
        stderr = proc.stderr.strip() if proc.stderr else ""
        
        # Create detailed output
        output_parts = []
        output_parts.append(f"⏱️ Execution time: {dura} seconds")
        output_parts.append(f"📊 Return code: {proc.returncode}")
        
        if proc.returncode == 0:
            output_parts.append("✅ Script executed successfully!")
        else:
            output_parts.append("❌ Script failed!")
        
        if stdout:
            output_parts.append(f"\n📤 STDOUT:\n{stdout}")
        
        if stderr:
            output_parts.append(f"\n⚠️ STDERR:\n{stderr}")
        
        if not stdout and not stderr:
            output_parts.append("\nℹ️ No output received from script")
        
        return "\n".join(output_parts)
        
    except subprocess.TimeoutExpired:
        return f"⏰ Script timed out after 3 minutes: {script_name}\n\nNote: Render free tier has resource limitations. Consider upgrading for longer execution times."
    except Exception as e:
        logger.error(f"Error executing {script_name}: {str(e)}")
        return f"💥 Error executing {script_name}: {str(e)}"

# Professional Header
st.markdown("""
    <div class="main-header">
      <div class="header-icon">🎯</div>
      <h1 class="header-title">ACN Command Center</h1>
      <p class="header-subtitle">Enterprise Script Management Interface</p>
    </div>
""", unsafe_allow_html=True)

# Environment Status Section
st.markdown("""
<div class="section-card">
    <h3 class="section-title">🔧 Environment Status</h3>
""", unsafe_allow_html=True)

# Check environment variables and display status
required_env_vars = [
    "FIREBASE_PROJECT_ID", "FIREBASE_PRIVATE_KEY_ID", "FIREBASE_PRIVATE_KEY",
    "FIREBASE_CLIENT_EMAIL", "FIREBASE_CLIENT_ID",
    "GSPREAD_PROJECT_ID", "GSPREAD_PRIVATE_KEY_ID", "GSPREAD_PRIVATE_KEY",
    "GSPREAD_CLIENT_EMAIL", "GSPREAD_CLIENT_ID"
]

# System information
col1, col2 = st.columns(2)
with col1:
    st.markdown(f"<p><strong>Working Directory:</strong><br><code>{os.getcwd()}</code></p>", unsafe_allow_html=True)
with col2:
    st.markdown(f"<p><strong>Python Version:</strong><br><code>{sys.version.split()[0]}</code></p>", unsafe_allow_html=True)

# Environment variables grid
st.markdown('<div class="env-grid">', unsafe_allow_html=True)
for var in required_env_vars:
    status_class = "success" if os.getenv(var) else "error"
    masked_value = "Configured" if os.getenv(var) else "Not Set"
    
    st.markdown(f"""
    <div class="env-item">
        <div class="env-status {status_class}"></div>
        <div>
            <strong>{var}</strong><br>
            <span style="color: #94a3b8; font-size: 12px;">{masked_value}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)

# Scripts list
dict_scripts = {
    "Leads": {"file": "all-leads.py", "desc": "Sync leads data from Firebase (direct source only)"},
    "Agents": {"file": "agents.py", "desc": "Sync agents data from Firebase"},
    "Enquiries": {"file": "enquires.py", "desc": "Sync enquiries from Firebase"},
    "Tried Access": {"file": "leads.py", "desc": "Sync tried access data from Firebase"},
    "Inventories": {"file": "inventories-from-firebase.py", "desc": "Sync inventories from Firebase"},
    "Requirements": {"file": "req.py", "desc": "Sync requirements from Firebase"},
    "ConnectHistory": {"file": "connecthistory.py", "desc": "Sync connect history from Firebase"},
    "QC Properties": {"file": "QC.py", "desc": "Sync QC properties from Firebase"}
}

# Operations Section
st.markdown("""
<div class="section-card">
    <h3 class="section-title">⚡ Available Operations</h3>
""", unsafe_allow_html=True)

# Create a grid layout for buttons
button_cols = st.columns(2)
for idx, (key, info) in enumerate(dict_scripts.items()):
    with button_cols[idx % 2]:
        if st.button(f"Execute {key}", key=f"btn_{key}", use_container_width=True):
            st.session_state.output = run_script(info['file'])

st.markdown("</div>", unsafe_allow_html=True)

# Output display
if 'output' in st.session_state:
    st.markdown("""
    <div class="section-card">
        <h3 class="section-title">📊 Operation Output</h3>
    """, unsafe_allow_html=True)
    
    st.text_area("Script Output", st.session_state.output, height=300, key="output_area", label_visibility="collapsed")
    
    st.markdown("</div>", unsafe_allow_html=True)

# Professional Footer
st.markdown("""
<div class="footer">
    <p class="footer-text">
        🚀 ACN Command Center | 
        ⚡ Enterprise Script Management | 
        🔒 Secure Cloud Environment
    </p>
</div>
""", unsafe_allow_html=True)
