import requests
import json

class MessageAPIClient:
    def __init__(self, base_url="http://127.0.0.1:5000/api"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session() # Use a session for potential future optimizations (e.g., cookies, headers)

    def _handle_response(self, response):
        """Helper function to handle API responses."""
        try:
            response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
            if response.status_code == 204: # No Content
                return None
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e}")
            print(f"Response content: {response.text}")
            return None
        except json.JSONDecodeError:
            print("Error: Could not decode JSON response.")
            print(f"Response content: {response.text}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request Exception: {e}")
            return None

    def register_application(self, name):
        """Registers a new application."""
        url = f"{self.base_url}/applications/register"
        payload = {"name": name}
        try:
            response = self.session.post(url, json=payload)
            return self._handle_response(response)
        except requests.exceptions.ConnectionError:
            print(f"Error: Could not connect to the server at {url}.")
            return None


    def record_heartbeat(self, app_id):
        """Records a heartbeat for a registered application."""
        url = f"{self.base_url}/applications/{app_id}/heartbeat"
        try:
            response = self.session.post(url)
            return self._handle_response(response)
        except requests.exceptions.ConnectionError:
            print(f"Error: Could not connect to the server at {url}.")
            return None

    def list_applications(self):
        """Lists all registered applications."""
        url = f"{self.base_url}/applications"
        try:
            response = self.session.get(url)
            return self._handle_response(response)
        except requests.exceptions.ConnectionError:
            print(f"Error: Could not connect to the server at {url}.")
            return None

    def get_application(self, app_id):
        """Gets details for a specific application."""
        url = f"{self.base_url}/applications/{app_id}"
        try:
            response = self.session.get(url)
            return self._handle_response(response)
        except requests.exceptions.ConnectionError:
            print(f"Error: Could not connect to the server at {url}.")
            return None

    def send_message(self, sender_id, receiver_id, content):
        """Sends a message from one registered application to another."""
        url = f"{self.base_url}/messages/send"
        payload = {
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "content": content
        }
        try:
            response = self.session.post(url, json=payload)
            return self._handle_response(response)
        except requests.exceptions.ConnectionError:
            print(f"Error: Could not connect to the server at {url}.")
            return None

    def receive_messages(self, receiver_app_id, unread_only=False):
        """Retrieves messages for a specific application."""
        url = f"{self.base_url}/messages/receive/{receiver_app_id}"
        params = {}
        if unread_only:
            params['unread'] = 'true'
        try:
            response = self.session.get(url, params=params)
            return self._handle_response(response)
        except requests.exceptions.ConnectionError:
            print(f"Error: Could not connect to the server at {url}.")
            return None

    def mark_message_as_read(self, message_id):
        """Marks a specific message as read."""
        url = f"{self.base_url}/messages/{message_id}/read"
        try:
            response = self.session.post(url)
            return self._handle_response(response)
        except requests.exceptions.ConnectionError:
            print(f"Error: Could not connect to the server at {url}.")
            return None

# --- Example Usage ---
if __name__ == "__main__":
    # Ensure the server.py is running before executing this client.
    client = MessageAPIClient() # Assumes server is running on http://127.0.0.1:5000

    print("--- Testing Application Registration ---")
    app1_name = "DataCollectorApp"
    app2_name = "DataProcessorApp"

    app1_data = client.register_application(app1_name)
    if app1_data and 'app_id' in app1_data:
        app1_id = app1_data['app_id']
        print(f"Registered {app1_name} with ID: {app1_id}")
    else:
        print(f"Failed to register {app1_name} or already registered.")
        # Try to fetch if already registered
        apps = client.list_applications()
        if apps:
            for app in apps.get('applications', []):
                if app['name'] == app1_name:
                    app1_id = app['id']
                    print(f"Found existing {app1_name} with ID: {app1_id}")
                    break
            else: # If loop completes without break
                print(f"Could not determine ID for {app1_name}. Exiting example.")
                exit()
        else:
            exit()

    app2_data = client.register_application(app2_name)
    if app2_data and 'app_id' in app2_data:
        app2_id = app2_data['app_id']
        print(f"Registered {app2_name} with ID: {app2_id}")
    else:
        print(f"Failed to register {app2_name} or already registered.")
        apps = client.list_applications()
        if apps:
            for app in apps.get('applications', []):
                if app['name'] == app2_name:
                    app2_id = app['id']
                    print(f"Found existing {app2_name} with ID: {app2_id}")
                    break
            else:
                print(f"Could not determine ID for {app2_name}. Exiting example.")
                exit()
        else:
            exit()

    print("\n--- Testing List Applications ---")
    apps_list = client.list_applications()
    if apps_list:
        print("Current applications:", json.dumps(apps_list, indent=2))

    print(f"\n--- Testing Get Application Details (App ID: {app1_id}) ---")
    app_details = client.get_application(app1_id)
    if app_details:
        print(f"Details for App ID {app1_id}:", json.dumps(app_details, indent=2))

    print(f"\n--- Testing Record Heartbeat (App ID: {app1_id}) ---")
    heartbeat_response = client.record_heartbeat(app1_id)
    if heartbeat_response:
        print(heartbeat_response.get('message'))

    print(f"\n--- After Heartbeat: Get Application Details (App ID: {app1_id}) ---")
    app_details_after_hb = client.get_application(app1_id)
    if app_details_after_hb:
        print(f"Details for App ID {app1_id}:", json.dumps(app_details_after_hb, indent=2))


    print(f"\n--- Testing Send Message (From App {app1_id} to App {app2_id}) ---")
    message_content = f"Hello {app2_name}, this is {app1_name} sending some important data at {datetime.now().isoformat()}!"
    send_response = client.send_message(sender_id=app1_id, receiver_id=app2_id, content=message_content)
    message_id = None
    if send_response:
        print(send_response.get('message'))
        message_id = send_response.get('message_id')
        print(f"Message ID: {message_id}")

    # Send another message
    send_response_2 = client.send_message(sender_id=app1_id, receiver_id=app2_id, content="This is a second message.")
    if send_response_2:
        print(send_response_2.get('message'))


    print(f"\n--- Testing Receive Messages (For App ID: {app2_id}, all) ---")
    received_messages = client.receive_messages(receiver_app_id=app2_id)
    if received_messages and received_messages.get('messages'):
        print(f"Messages for App ID {app2_id}:")
        for msg in received_messages['messages']:
            print(f"  ID: {msg['id']}, From: {msg['sender_name']} (ID: {msg['sender_id']}), Content: '{msg['content']}', Read: {msg['is_read']}")
