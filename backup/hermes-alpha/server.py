from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timezone

# Initialize Flask app and SQLAlchemy
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///message_system.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --- Database Models ---
class Application(db.Model):
    __tablename__ = 'application'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), unique=True, nullable=False)
    last_heartbeat = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    # Potentially add more fields like IP address, version, etc.

    def __repr__(self):
        return f'<Application {self.name}>'

class Message(db.Model):
    __tablename__ = 'message'
    id = db.Column(db.Integer, primary_key=True)
    sender_id = db.Column(db.Integer, db.ForeignKey('application.id'), nullable=False)
    receiver_id = db.Column(db.Integer, db.ForeignKey('application.id'), nullable=False)
    content = db.Column(db.Text, nullable=False)
    timestamp = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    is_read = db.Column(db.Boolean, default=False)

    sender = db.relationship('Application', foreign_keys=[sender_id], backref=db.backref('sent_messages', lazy=True))
    receiver = db.relationship('Application', foreign_keys=[receiver_id], backref=db.backref('received_messages', lazy=True))

    def __repr__(self):
        return f'<Message {self.id} from {self.sender.name} to {self.receiver.name}>'

# --- API Endpoints ---

# Application Management
@app.route('/api/applications/register', methods=['POST'])
def register_application():
    """Registers a new application."""
    data = request.get_json()
    if not data or 'name' not in data:
        return jsonify({'error': 'Application name is required'}), 400

    app_name = data['name']
    if Application.query.filter_by(name=app_name).first():
        return jsonify({'error': f'Application "{app_name}" already registered'}), 409

    new_app = Application(name=app_name)
    db.session.add(new_app)
    db.session.commit()
    return jsonify({'message': f'Application "{app_name}" registered successfully', 'app_id': new_app.id}), 201

@app.route('/api/applications/<int:app_id>/heartbeat', methods=['POST'])
def record_heartbeat(app_id):
    """Records a heartbeat for a registered application."""
    app_instance = db.session.get(Application, app_id)
    if not app_instance:
        return jsonify({'error': 'Application not found'}), 404

    app_instance.last_heartbeat = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({'message': f'Heartbeat recorded for application "{app_instance.name}"'}), 200

@app.route('/api/applications', methods=['GET'])
def list_applications():
    """Lists all registered applications."""
    apps = Application.query.all()
    output = []
    for app_instance in apps:
        output.append({
            'id': app_instance.id,
            'name': app_instance.name,
            'last_heartbeat': app_instance.last_heartbeat.isoformat() if app_instance.last_heartbeat else None
        })
    return jsonify({'applications': output}), 200

@app.route('/api/applications/<int:app_id>', methods=['GET'])
def get_application(app_id):
    """Gets details for a specific application."""
    app_instance = db.session.get(Application, app_id)
    if not app_instance:
        return jsonify({'error': 'Application not found'}), 404
    return jsonify({
        'id': app_instance.id,
        'name': app_instance.name,
        'last_heartbeat': app_instance.last_heartbeat.isoformat() if app_instance.last_heartbeat else None
    }), 200

# Message Handling
@app.route('/api/messages/send', methods=['POST'])
def send_message():
    """Sends a message from one registered application to another."""
    data = request.get_json()
    if not data or not all(k in data for k in ('sender_id', 'receiver_id', 'content')):
        return jsonify({'error': 'Missing sender_id, receiver_id, or content'}), 400

    sender_id = data['sender_id']
    receiver_id = data['receiver_id']
    content = data['content']

    sender_app = db.session.get(Application, sender_id)
    receiver_app = db.session.get(Application, receiver_id)

    if not sender_app:
        return jsonify({'error': f'Sender application with id {sender_id} not found'}), 404
    if not receiver_app:
        return jsonify({'error': f'Receiver application with id {receiver_id} not found'}), 404

    if sender_id == receiver_id:
        return jsonify({'error': 'Sender and receiver cannot be the same application'}), 400

    new_message = Message(sender_id=sender_id, receiver_id=receiver_id, content=content)
    db.session.add(new_message)
    db.session.commit()
    return jsonify({'message': 'Message sent successfully', 'message_id': new_message.id}), 201

@app.route('/api/messages/receive/<int:receiver_app_id>', methods=['GET'])
def receive_messages(receiver_app_id):
    """Retrieves all unread messages for a specific application."""
    receiver_app = db.session.get(Application, receiver_app_id)
    if not receiver_app:
        return jsonify({'error': f'Application with id {receiver_app_id} not found'}), 404

    # Option 1: Get only unread messages and mark them as read
    # messages = Message.query.filter_by(receiver_id=receiver_app_id, is_read=False).order_by(Message.timestamp.asc()).all()
    # output = []
    # for msg in messages:
    #     output.append({
    #         'id': msg.id,
    #         'sender_id': msg.sender_id,
    #         'sender_name': msg.sender.name,
    #         'content': msg.content,
    #         'timestamp': msg.timestamp.isoformat()
    #     })
    #     msg.is_read = True
    # db.session.commit()

    # Option 2: Get all messages (read and unread) for the receiver.
    # Client application can then decide how to handle them (e.g., filter by 'is_read' or use a separate 'mark_as_read' endpoint).
    # For simplicity, this example returns all messages for the receiver.
    # You might want to add pagination for large numbers of messages.
    query = Message.query.filter_by(receiver_id=receiver_app_id).order_by(Message.timestamp.desc())

    # Optional: filter by read status via query parameter
    only_unread = request.args.get('unread', 'false').lower() == 'true'
    if only_unread:
        query = query.filter_by(is_read=False)

    messages = query.all()
    output = []
    for msg in messages:
        output.append({
            'id': msg.id,
            'sender_id': msg.sender_id,
            'sender_name': msg.sender.name, # Assumes sender relationship is loaded
            'content': msg.content,
            'timestamp': msg.timestamp.isoformat(),
            'is_read': msg.is_read
        })
    return jsonify({'messages': output}), 200

@app.route('/api/messages/<int:message_id>/read', methods=['POST'])
def mark_message_as_read(message_id):
    """Marks a specific message as read."""
    message = db.session.get(Message, message_id)
    if not message:
        return jsonify({'error': 'Message not found'}), 404

    # Optional: You might want to verify that the request is coming from the intended receiver.
    # For example, by requiring receiver_app_id in the POST body or path and checking it.
    # if message.receiver_id != current_user_app_id: # Pseudo-code for auth
    #     return jsonify({'error': 'Unauthorized to mark this message as read'}), 403

    message.is_read = True
    db.session.commit()
    return jsonify({'message': f'Message {message_id} marked as read'}), 200


# --- Utility for Database Initialization ---
def init_db():
    with app.app_context():
        db.create_all()
    print("Database initialized!")

if __name__ == '__main__':
    # Initialize the database if it doesn't exist
    # In a production environment, you might use Flask-Migrate for schema migrations
    with app.app_context():
        db.create_all()
    app.run(debug=True, host='0.0.0.0', port=5000) # Listen on all interfaces
