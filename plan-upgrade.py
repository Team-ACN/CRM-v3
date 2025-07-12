import streamlit as st
import os
import datetime
import pytz
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore

# Set page config - MUST BE THE FIRST STREAMLIT COMMAND
st.set_page_config(
    page_title="Agent Management Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
    page_icon="⭐"
)

# Load environment variables
load_dotenv()

# Initialize Firebase Admin SDK using environment variables
@st.cache_resource
def initialize_firebase():
    try:
        firebase_admin.get_app()
    except ValueError:
        firebase_credentials = {
            "type": "service_account",
            "project_id": os.getenv("FIREBASE_PROJECT_ID"),
            "private_key_id": os.getenv("FIREBASE_PRIVATE_KEY_ID"),
            "private_key": os.getenv("FIREBASE_PRIVATE_KEY").replace("\\n", "\n"),
            "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
            "client_id": os.getenv("FIREBASE_CLIENT_ID"),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.getenv("FIREBASE_CLIENT_CERT_URL")
        }
        cred = credentials.Certificate(firebase_credentials)
        firebase_admin.initialize_app(cred)
    return firestore.client()

# Initialize Firebase and get database reference
db = initialize_firebase()

def standardize_phone_number(phone):
    """Standardize phone number format"""
    phone = ''.join(filter(str.isdigit, phone))
    
    if phone.startswith('91') and len(phone) == 12:
        return '+' + phone
    elif len(phone) == 10:
        return '+91' + phone
    elif phone.startswith('+91'):
        return phone
    else:
        return '+91' + phone

def get_agent_by_phone(phone_number):
    """Fetch agent data by phone number"""
    try:
        phone_number = standardize_phone_number(phone_number)
        agents_ref = db.collection('agents')
        query = agents_ref.where('phonenumber', '==', phone_number).limit(1)
        results = query.get()
        
        if results:
            agent_data = results[0].to_dict()
            agent_data['id'] = results[0].id
            return agent_data, None
        else:
            return None, f"No agent found with phone number {phone_number}"
    except Exception as e:
        return None, f"Error fetching agent data: {str(e)}"

def update_user_plan(phone_number, plan, agent_data):
    """Update user's subscription plan"""
    try:
        current_date = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
        current_timestamp = int(current_date.timestamp())

        # Plan configurations
        plan_configs = {
            "premium": {
                "expiry_days": 365,
                "monthly_credits": 100,
                "user_type": "premium"
            },
            "trial": {
                "expiry_days": 30,
                "monthly_credits": 100,
                "user_type": "trial"
            },
            "basic": {
                "expiry_days": 30,
                "monthly_credits": 5,
                "user_type": "basic"
            }
        }

        config = plan_configs.get(plan)
        if not config:
            return False, "Invalid plan selected"

        # Calculate dates
        next_renewal = current_date + datetime.timedelta(days=30)
        plan_expiry = current_date + datetime.timedelta(days=config["expiry_days"])

        # Prepare update data
        update_data = {
            'nextRenewal': int(next_renewal.timestamp()),
            'userType': config["user_type"],
            'planExpiry': int(plan_expiry.timestamp()),
            'monthlyCredits': config["monthly_credits"],
            'updatedAt': current_timestamp,
            'lastModified': current_timestamp
        }

        # Handle trial usage tracking
        previous_plan = agent_data.get('userType', None)
        if previous_plan == "basic" and plan != "basic":
            update_data['trialUsed'] = True
            update_data['trialStartedAt'] = current_timestamp

        # Update document
        agent_ref = db.collection('agents').document(agent_data['id'])
        agent_ref.update(update_data)
        
        return True, f"Successfully updated plan to {plan.upper()}. New expiry: {plan_expiry.strftime('%Y-%m-%d')}, Credits: {config['monthly_credits']}"
    
    except Exception as e:
        return False, f"Failed to update plan: {str(e)}"

def add_manual_credits(phone_number, credits_to_add, agent_data):
    """Add manual credits to user account"""
    try:
        current_timestamp = int(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).timestamp())
        current_credits = agent_data.get('monthlyCredits', 0)
        new_credits = current_credits + credits_to_add

        update_data = {
            'monthlyCredits': new_credits,
            'updatedAt': current_timestamp,
            'lastModified': current_timestamp
        }

        agent_ref = db.collection('agents').document(agent_data['id'])
        agent_ref.update(update_data)
        
        return True, f"Successfully added {credits_to_add} credits. New total: {new_credits}"
    
    except Exception as e:
        return False, f"Failed to add credits: {str(e)}"

def toggle_blacklist(phone_number, agent_data):
    """Toggle blacklist status of user"""
    try:
        current_timestamp = int(datetime.datetime.now(pytz.timezone('Asia/Kolkata')).timestamp())
        current_blacklist = agent_data.get('blacklisted', False)
        new_blacklist_status = not current_blacklist

        update_data = {
            'blacklisted': new_blacklist_status,
            'updatedAt': current_timestamp,
            'lastModified': current_timestamp
        }

        agent_ref = db.collection('agents').document(agent_data['id'])
        agent_ref.update(update_data)
        
        status = "blacklisted" if new_blacklist_status else "unblacklisted"
        return True, f"Successfully {status} the agent"
    
    except Exception as e:
        return False, f"Failed to update blacklist status: {str(e)}"

def display_agent_info(agent_data, show_credits=True, show_blacklist=True):
    """Display agent information using Streamlit components"""
    # Main agent info container
    with st.container():
        # Agent header
        st.markdown("### 👤 Agent Information")
        
        # Create columns for better layout
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.markdown(f"**Name:** {agent_data.get('name', 'Not Available')}")
            st.markdown(f"**Phone:** `{agent_data.get('phonenumber', 'Not Available')}`")
            
            # Plan badge with color
            plan = agent_data.get('userType', 'Not Available').upper()
            if plan == 'PREMIUM':
                st.markdown(f"**Plan:** :premium[{plan}] 🏆")
            elif plan == 'TRIAL':
                st.markdown(f"**Plan:** :trial[{plan}] ⏱️")
            else:
                st.markdown(f"**Plan:** :basic[{plan}] 📦")
        
        with col2:
            if show_credits:
                credits = agent_data.get('monthlyCredits', 0)
                st.markdown(f"**Monthly Credits:** :blue[{credits}] 💰")
            
            # Format and display expiry date
            plan_expiry = datetime.datetime.fromtimestamp(agent_data.get('planExpiry', 0))
            st.markdown(f"**Plan Expiry:** {plan_expiry.strftime('%Y-%m-%d %H:%M')} 📅")
            
            if show_blacklist:
                blacklist_status = agent_data.get('blacklisted', False)
                if blacklist_status:
                    st.markdown("**Status:** :red[BLACKLISTED] 🚫")
                else:
                    st.markdown("**Status:** :green[ACTIVE] ✅")
        
        st.divider()

# Enhanced CSS with better styling
st.markdown("""
    <style>
    /* Global Styles */
    .stApp { 
        background: linear-gradient(135deg, #1a1a1a 0%, #2d3436 100%);
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    /* Main Header */
    .main-header {
        text-align: center;
        background: rgba(45, 52, 54, 0.7);
        backdrop-filter: blur(20px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 25px;
        padding: 40px;
        margin-bottom: 40px;
        color: white;
    }
    
    .main-header h1 {
        font-size: 3rem;
        font-weight: 700;
        margin: 20px 0;
        background: linear-gradient(45deg, #dfe6e9, #b2bec3);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    /* Button Styling */
    .stButton button {
        background: linear-gradient(45deg, #2d3436, #636e72) !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 12px 25px !important;
        font-weight: 600 !important;
        color: white !important;
        transition: all 0.3s ease !important;
        width: 100% !important;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2) !important;
    }
    
    .stButton button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.25) !important;
        background: linear-gradient(45deg, #636e72, #2d3436) !important;
    }
    
    /* Input Styling */
    .stTextInput input, .stNumberInput input {
        background: rgba(45, 52, 54, 0.7) !important;
        border: 2px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 15px !important;
        color: white !important;
        font-size: 1.1rem !important;
        padding: 15px !important;
        backdrop-filter: blur(10px) !important;
    }
    
    .stTextInput input:focus, .stNumberInput input:focus {
        border-color: #636e72 !important;
        box-shadow: 0 0 20px rgba(99, 110, 114, 0.3) !important;
    }
    
    /* Tab Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 20px;
        background: rgba(45, 52, 54, 0.7);
        border-radius: 20px;
        padding: 10px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 15px;
        color: rgba(255, 255, 255, 0.7);
        font-weight: 600;
        font-size: 1.1rem;
        padding: 15px 25px;
        border: none;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(45deg, #2d3436, #636e72);
        color: white !important;
    }
    
    /* Container styling */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* Success/Error Messages */
    .stSuccess {
        background: rgba(34, 197, 94, 0.1) !important;
        border: 1px solid rgba(34, 197, 94, 0.3) !important;
        border-radius: 15px !important;
        color: #22c55e !important;
    }
    
    .stError {
        background: rgba(239, 68, 68, 0.1) !important;
        border: 1px solid rgba(239, 68, 68, 0.3) !important;
        border-radius: 15px !important;
        color: #ef4444 !important;
    }
    
    .stInfo {
        background: rgba(59, 130, 246, 0.1) !important;
        border: 1px solid rgba(59, 130, 246, 0.3) !important;
        border-radius: 15px !important;
        color: #3b82f6 !important;
    }
    
    .stWarning {
        background: rgba(245, 158, 11, 0.1) !important;
        border: 1px solid rgba(245, 158, 11, 0.3) !important;
        border-radius: 15px !important;
        color: #f59e0b !important;
    }
    </style>
""", unsafe_allow_html=True)

# Main Header
st.markdown("""
    <div class="main-header">
        <div style="font-size: 4rem; margin-bottom: 20px;">⭐</div>
        <h1>Agent Management Dashboard</h1>
        <p style="font-size: 1.2rem; opacity: 0.9; margin: 0;">Manage subscription plans, credits, and user status</p>
    </div>
""", unsafe_allow_html=True)

# Initialize session state
if "current_agent" not in st.session_state:
    st.session_state.current_agent = None
if "selected_plan" not in st.session_state:
    st.session_state.selected_plan = None

# Create tabs for different functionalities
tab1, tab2, tab3 = st.tabs(["📋 Plan Management", "💰 Credit Management", "🚫 Blacklist Management"])

with tab1:
    st.markdown("## 📋 Plan Management")
    
    phone_input = st.text_input(
        "Enter Agent's Phone Number",
        placeholder="e.g., 8118823650 or +918118823650",
        key="plan_phone",
        help="Enter 10-digit Indian mobile number"
    )
    
    if phone_input:
        agent_data, error = get_agent_by_phone(phone_input)
        
        if error:
            st.error(error)
        else:
            st.session_state.current_agent = agent_data
            display_agent_info(agent_data, show_blacklist=False)
            
            st.markdown("### Select New Plan")
            
            # Plan selection with better UI
            col1, col2, col3 = st.columns(3)
            
            plans = [
                {"name": "premium", "title": "Premium", "credits": 100, "validity": "1 Year", "icon": "🏆"},
                {"name": "trial", "title": "Trial", "credits": 100, "validity": "30 Days", "icon": "⏱️"},
                {"name": "basic", "title": "Basic", "credits": 5, "validity": "30 Days", "icon": "📦"}
            ]
            
            for i, plan in enumerate(plans):
                with [col1, col2, col3][i]:
                    # Create a nice plan display
                    st.markdown(f"""
                    **{plan['icon']} {plan['title']}**
                    - Credits: {plan['credits']}
                    - Validity: {plan['validity']}
                    """)
                    if st.button(f"Select {plan['title']}", key=f"select_{plan['name']}", use_container_width=True):
                        st.session_state.selected_plan = plan['name']
            
            if st.session_state.selected_plan:
                selected_plan = next(p for p in plans if p['name'] == st.session_state.selected_plan)
                
                st.divider()
                st.markdown("### Confirm Plan Change")
                st.info(f"You are about to change the plan to **{selected_plan['title'].upper()}** ({selected_plan['credits']} credits, {selected_plan['validity']} validity)")
                
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button("✅ Confirm Update", type="primary", use_container_width=True):
                        success, message = update_user_plan(phone_input, st.session_state.selected_plan, agent_data)
                        if success:
                            st.success(message)
                            st.session_state.selected_plan = None
                            st.rerun()
                        else:
                            st.error(message)
                
                with col2:
                    if st.button("❌ Cancel", use_container_width=True):
                        st.session_state.selected_plan = None
                        st.rerun()

with tab2:
    st.markdown("## 💰 Credit Management")
    
    phone_input_credits = st.text_input(
        "Enter Agent's Phone Number",
        placeholder="e.g., 8118823650 or +918118823650",
        key="credits_phone",
        help="Enter 10-digit Indian mobile number"
    )
    
    if phone_input_credits:
        agent_data, error = get_agent_by_phone(phone_input_credits)
        
        if error:
            st.error(error)
        else:
            display_agent_info(agent_data, show_blacklist=False)
            
            st.markdown("### Add Credits")
            
            col1, col2 = st.columns([2, 1])
            with col1:
                credits_to_add = st.number_input(
                    "Credits to Add",
                    min_value=1,
                    max_value=1000,
                    value=10,
                    help="Enter number of credits to add (1-1000)"
                )
            
            with col2:
                st.markdown("<br>", unsafe_allow_html=True)  # Add some spacing
                if st.button("💰 Add Credits", use_container_width=True):
                    success, message = add_manual_credits(phone_input_credits, credits_to_add, agent_data)
                    if success:
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)

with tab3:
    st.markdown("## 🚫 Blacklist Management")
    
    phone_input_blacklist = st.text_input(
        "Enter Agent's Phone Number",
        placeholder="e.g., 8118823650 or +918118823650",
        key="blacklist_phone",
        help="Enter 10-digit Indian mobile number"
    )
    
    if phone_input_blacklist:
        agent_data, error = get_agent_by_phone(phone_input_blacklist)
        
        if error:
            st.error(error)
        else:
            display_agent_info(agent_data, show_credits=False)
            
            current_status = agent_data.get('blacklisted', False)
            action_text = "Remove from Blacklist" if current_status else "Add to Blacklist"
            action_icon = "✅" if current_status else "🚫"
            
            st.markdown("### Blacklist Action")
            
            if current_status:
                st.warning("⚠️ This agent is currently **BLACKLISTED**")
            else:
                st.info("ℹ️ This agent is currently **ACTIVE**")
            
            if st.button(f"{action_icon} {action_text}", use_container_width=True):
                success, message = toggle_blacklist(phone_input_blacklist, agent_data)
                if success:
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)

# Footer
st.markdown("---")
st.markdown("""
    <div style="text-align: center; margin-top: 30px; padding: 20px; color: rgba(255, 255, 255, 0.6);">
        <p>Agent Management Dashboard • Built with Streamlit</p>
    </div>
""", unsafe_allow_html=True)