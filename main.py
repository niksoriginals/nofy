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

# Fix for private_key newlines
if "private_key" in service_account_json:
    service_account_json["private_key"] = service_account_json["private_key"].replace("\\n", "\n")

cred = credentials.Certificate(service_account_json)
firebase_admin.initialize_app(cred)
db = firestore.client()

SCOPES = ["https://www.googleapis.com/auth/firebase.messaging"]
credentials_fc = service_account.Credentials.from_service_account_info(
    service_account_json, scopes=SCOPES
)
credentials_fc.refresh(Request())
access_token = credentials_fc.token
project_id = service_account_json["project_id"]

# ------------------- TRACK LAST TIMESTAMP -------------------
LAST_FILE = "last_timestamp.txt"
def get_last_timestamp():
    if os.path.exists(LAST_FILE):
        with open(LAST_FILE, "r") as f:
            ts = f.read().strip()
            if ts:
                return datetime.fromisoformat(ts)
    return datetime(2000, 1, 1, tzinfo=timezone.utc)

def set_last_timestamp(ts: datetime):
    with open(LAST_FILE, "w") as f:
        f.write(ts.isoformat())

# ------------------- FIRESTORE CHECK & NOTIFICATION -------------------
def check_firestore_and_send_notifications():
    priority_order = ["news", "events", "files"]
    last_timestamp = get_last_timestamp()
    max_timestamp = last_timestamp

    for collection in priority_order:
        docs = db.collection(collection)\
            .order_by("timestamp", direction=firestore.Query.ASCENDING)\
            .stream()

        for doc in docs:
            data = doc.to_dict()
            doc_ts = data.get("timestamp")
            if not doc_ts:
                continue
            # Firestore timestamp to datetime
            if hasattr(doc_ts, "to_datetime"):
                doc_dt = doc_ts.to_datetime().replace(tzinfo=timezone.utc)
            else:
                doc_dt = doc_ts  # already datetime

            if doc_dt <= last_timestamp:
                continue  # already notified

            # Send notification
            title = data.get("title", "Something New - Tap to Read")
            message = {
                "message": {
                    "topic": "allUsers",
                    "notification": {
                        "title": "📢 Campus Update",
                        "body": title
                    },
                    "android": {
                        "priority": "HIGH",
                        "notification": {
                            "channel_id": "high_importance_channel",
                            "default_sound": True,
                            "default_vibrate_timings": True,
                            "sound": "default"
                        }
                    },
                    "data": {
                        "click_action": "FLUTTER_NOTIFICATION_CLICK",
                        "collection": collection,
                        "doc_id": doc.id
                    }
                }
            }

            url = f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; UTF-8",
            }

            response = requests.post(url, headers=headers, data=json.dumps(message))
            if response.status_code == 200:
                print(f"✅ Notification sent for {collection}: {title}")
            else:
                print(f"❌ Failed for {collection}: {title} | Status: {response.status_code}")
                print(response.text)

            # Update max_timestamp
            if doc_dt > max_timestamp:
                max_timestamp = doc_dt

    # Save last notified timestamp
    set_last_timestamp(max_timestamp)

# ------------------- MAIN LOOP -------------------
CHECK_INTERVAL = 60  # seconds
print("🚀 Starting Firestore notification watcher...")

while True:
    try:
        check_firestore_and_send_notifications()
    except Exception as e:
        print("⚠️ Error:", e)
    time.sleep(CHECK_INTERVAL)
