import os
import json
import time
import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone

# ------------------- FIREBASE INITIALIZATION -------------------
if "FIREBASE_SERVICE_ACCOUNT" not in os.environ:
    raise ValueError("❌ FIREBASE_SERVICE_ACCOUNT environment variable not found!")

service_account_json = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])

# Fix for escaped newlines in private_key
if "private_key" in service_account_json:
    service_account_json["private_key"] = service_account_json["private_key"].replace("\\n", "\n")

# Initialize Firebase
cred = credentials.Certificate(service_account_json)
firebase_admin.initialize_app(cred)
db = firestore.client()

# Firebase Cloud Messaging setup
SCOPES = ["https://www.googleapis.com/auth/firebase.messaging"]
credentials_fc = service_account.Credentials.from_service_account_info(
    service_account_json, scopes=SCOPES
)
project_id = service_account_json["project_id"]

# ------------------- TIMESTAMP TRACKING -------------------
LAST_FILE = "last_timestamp.txt"

def get_last_timestamp():
    """Get last processed timestamp."""
    if os.path.exists(LAST_FILE):
        with open(LAST_FILE, "r") as f:
            ts = f.read().strip()
            if ts:
                return datetime.fromisoformat(ts)
    # First time: ignore old data
    return datetime.now(timezone.utc)

def set_last_timestamp(ts: datetime):
    """Save last processed timestamp."""
    with open(LAST_FILE, "w") as f:
        f.write(ts.isoformat())

# ------------------- FIRESTORE & FCM NOTIFIER -------------------
def check_firestore_and_send_notifications():
    global credentials_fc

    # Refresh FCM token if expired
    if not credentials_fc.valid or credentials_fc.expired:
        credentials_fc.refresh(Request())
    access_token = credentials_fc.token

    priority_order = ["news", "events", "files"]
    last_timestamp = get_last_timestamp()
    max_timestamp = last_timestamp

    for collection in priority_order:
        # Fetch only new documents
        docs = (
            db.collection(collection)
            .where("timestamp", ">", last_timestamp)
            .order_by("timestamp", direction=firestore.Query.ASCENDING)
            .stream()
        )

        for doc in docs:
            data = doc.to_dict()
            doc_ts = data.get("timestamp")
            if not doc_ts:
                continue

            # Convert Firestore timestamp to datetime
            if hasattr(doc_ts, "to_datetime"):
                doc_dt = doc_ts.to_datetime().replace(tzinfo=timezone.utc)
            else:
                doc_dt = doc_ts

            title = data.get("title", "Something New - Tap to Read")

            # Prepare FCM message
            message = {
                "message": {
                    "topic": "allUsers",
                    "notification": {
                        "title": "📢 Campus Update",
                        "body": title,
                    },
                    "android": {
                        "priority": "HIGH",
                        "notification": {
                            "channel_id": "high_importance_channel",
                            "default_sound": True,
                            "default_vibrate_timings": True,
                            "sound": "default",
                        },
                    },
                    "data": {
                        "click_action": "FLUTTER_NOTIFICATION_CLICK",
                        "collection": collection,
                        "doc_id": doc.id,
                    },
                }
            }

            # Send notification
            url = f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; UTF-8",
            }

            response = requests.post(url, headers=headers, data=json.dumps(message))
            if response.status_code == 200:
                print(f"✅ Sent: {collection} → {title}")
            else:
                print(f"❌ Failed: {collection} → {title} | {response.status_code}")
                print(response.text)

            # Track latest timestamp
            if doc_dt > max_timestamp:
                max_timestamp = doc_dt

    # Save progress
    set_last_timestamp(max_timestamp)

# ------------------- MAIN LOOP -------------------
CHECK_INTERVAL = 60  # seconds

print("🚀 Firestore Notification Watcher Started...")

while True:
    try:
        check_firestore_and_send_notifications()
    except Exception as e:
        print(f"⚠️ Error: {e}")
    time.sleep(CHECK_INTERVAL)
