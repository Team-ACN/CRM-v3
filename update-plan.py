import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import os
import json

# Load environment variables
load_dotenv()

# Initialize Firebase Admin SDK using environment variables
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

# Get reference to Firestore database
db = firestore.client()

def standardize_phone_number(phone):
    # Remove any spaces or special characters
    phone = ''.join(filter(str.isdigit, phone))
    
    # If number starts with 91 and is 12 digits, add + prefix
    if phone.startswith('91') and len(phone) == 12:
        return '+' + phone
    
    # If number is 10 digits, add +91 prefix
    if len(phone) == 10:
        return '+91' + phone
    
    # If number already has +91 prefix, return as is
    if phone.startswith('+91'):
        return phone
    
    # If none of the above conditions match, return original with +91
    return '+91' + phone

def get_user_input():
    # Take phone number input
    phone_number = input("Enter the phone number of the agent: ")
    
    # Standardize the phone number format
    phone_number = standardize_phone_number(phone_number)

    # Query the database using the phonenumber field
    agents_ref = db.collection('agents')
    query = agents_ref.where('phonenumber', '==', phone_number).limit(1)
    results = query.get()

    if results:
        agent_data = results[0]
        current_data = agent_data.to_dict()
        # Display current plan and expiry details
        print("\nCurrent Plan Details:")
        print(f"Name: {current_data.get('name', 'Not Available')}")
        print(f"Phone Number: {current_data.get('phonenumber', 'Not Available')}")
        print(f"Current Plan: {current_data.get('userType', 'Not Available')}")
        print(f"Plan Expiry: {datetime.fromtimestamp(current_data.get('planExpiry', 0)).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Monthly Credits: {current_data.get('monthlyCredits', 0)}")
        print(f"Trial Used: {current_data.get('trialUsed', False)}")
    else:
        print(f"No agent found with the phone number {phone_number}. Please try again.")
        return None, None, None, None

    # Dropdown menu for plan selection
    print("\nSelect the new plan for the agent:")
    print("1. Premium")
    print("2. Trial")
    print("3. Basic")
    plan_choice = input("Enter the plan number (1/2/3): ")

    # Map plan choice to actual plan names
    if plan_choice == "1":
        plan = "premium"
    elif plan_choice == "2":
        plan = "trial"
    elif plan_choice == "3":
        plan = "basic"
    else:
        print("Invalid choice! Please select a valid plan.")
        return None, None, None, None

    # Check if the previous plan was 'basic' and we are changing to another plan
    previous_plan = current_data.get('userType', None)
    return phone_number, plan, previous_plan, current_data

def update_user_plan(phone_number, plan, previous_plan, current_data):
    # Get the current date
    current_date = datetime.now(pytz.timezone('Asia/Kolkata'))
    current_timestamp = int(current_date.timestamp())

    # Calculate next renewal date (30 days from now)
    next_renewal = current_date + timedelta(days=30)
    next_renewal_timestamp = int(next_renewal.timestamp())

    # Determine plan expiry, monthly credits, and user type based on selected plan
    if plan == "premium":
        plan_expiry = current_date + timedelta(days=365)  # 1 year for premium
        monthly_credits = 100
        user_type = "premium"
    elif plan == "trial":
        plan_expiry = current_date + timedelta(days=30)  # 30 days for trial
        monthly_credits = 50  # Assuming trial gives 50 credits
        user_type = "trial"
    else:  # For basic plan
        plan_expiry = current_date + timedelta(days=30)  # 30 days for basic as well
        monthly_credits = 25  # Assuming basic gives 25 credits
        user_type = "basic"

    # Prepare the update data while preserving existing fields
    update_data = {
        'nextRenewal': next_renewal_timestamp,
        'userType': user_type,
        'planExpiry': int(plan_expiry.timestamp()),
        'monthlyCredits': monthly_credits,
        'updatedAt': current_timestamp,
        'lastModified': current_timestamp
    }

    # If the previous plan was basic and we are switching to another plan, set trialUsed to True
    if previous_plan == "basic" and plan != "basic":
        update_data['trialUsed'] = True
        update_data['trialStartedAt'] = current_timestamp

    # Get the document reference using the query result
    agent_ref = db.collection('agents').document(current_data['id'])

    # Update the agent's document in Firebase
    try:
        agent_ref.update(update_data)
        print(f"\nSuccessfully updated the plan for phone number {phone_number} to {plan}.")
        print(f"New plan expiry: {plan_expiry.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"New monthly credits: {monthly_credits}")
    except Exception as e:
        print(f"Failed to update the plan: {e}")

def main():
    phone_number, plan, previous_plan, current_data = get_user_input()
    if all([phone_number, plan, previous_plan, current_data]):
        update_user_plan(phone_number, plan, previous_plan, current_data)
    else:
        print("Operation cancelled.")

if __name__ == "__main__":
    main()
