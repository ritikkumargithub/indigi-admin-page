
from flask import Flask, jsonify, request
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
from flask_cors import CORS
from email_service import send_email
from notification_service import consume_notifications, send_notification

app = Flask(__name__)

CORS(app)

app.config["MONGO_URI"] = "mongodb://localhost:27017/myindigodb"
mongo = PyMongo(app)
db = mongo.db.flights  

@app.route('/')
def index():
    return "Welcome to the Flask MongoDB API!"

@app.route('/add-flight', methods=['POST'])
def add_flight():
    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    flight = {
        "flightNo": data.get("flightNo", 'KOL564'),
        "status": data.get("status", 'On Time'),
        "gateNo": data.get("gateNo", '2'),
        "from": data.get("from", 'Delhi'),
        "to": data.get("to", 'Mumbai'),
        "duration": data.get("duration", '2 hours'),
        "price": data.get("price", '₹4,500'),
        "passenger": data.get("passenger", [])  # List of passenger
    }
    
    flight_id = db.insert_one(flight).inserted_id
    return jsonify({"message": "Flight added", "id": str(flight_id)}), 201

@app.route('/get-flights', methods=['GET'])
def get_flights():
    flights = list(db.find())
    for flight in flights:
        flight["_id"] = str(flight["_id"])
    return jsonify(flights), 200

@app.route('/update-flight-status/<id>', methods=['PUT'])
def update_flight_status(id):
    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400

    current_flight = db.find_one({"_id": ObjectId(id)})
    if not current_flight:
        return jsonify({"error": "Flight not found"}), 404

    isStatusChange = 0
    isGateNoChange = 0

    if 'status' in data and data['status'] != current_flight.get('status'):
        isStatusChange = 1
    if 'gateNo' in data and data['gateNo'] != current_flight.get('gateNo'):
        isGateNoChange = 1
   
    db.update_one({"_id": ObjectId(id)}, {"$set": data})

    # subject = ''
    # body = ''
    # if isStatusChange == 1 and isGateNoChange == 1:
    #     subject = f"Flight Status Update"
    #     body = f"The status of your flight with ID {data['flightNo']} has been updated to {data['status']} and new gate no is: {data['gateNo']}."
    # elif isGateNoChange == 1:
    #     subject = f"Flight Gate No. Update: {data['gateNo']}"
    #     body = f"The status of your flight with ID {data['flightNo']} has been updated to gate no: {data['gateNo']}."
    # elif isStatusChange == 1:
    #     subject = f"Flight Status Update: {data['status']}"
    #     body = f"The status of your flight with ID {data['flightNo']} has been updated to {data['status']}."

    # for user in current_flight['passenger']:
    #     to_email = user.get('email')
    #     send_email(to_email, subject, body)

    if isStatusChange or isGateNoChange:
        notification_message = {
            'isStatusChange':isStatusChange,
            'isGateNoChange':isGateNoChange,
            'flightNo':data['flightNo'],
            'gateNo':data['gateNo'],
            'status':data['status'],
            'allPassenger':current_flight['passenger']
        }
        print(notification_message)
        send_notification(notification_message)
        consume_notifications()
    
    # print(notification_message)
    
    return jsonify({"message": "Flight updated"}), 200

@app.route('/add-user/<flight_id>', methods=['POST'])
def add_user(flight_id):
    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    user = {
        "username": data.get("username"),
        "email": data.get("email")
    }
    
    updated = db.update_one({"_id": ObjectId(flight_id)}, {"$push": {"passenger": user}})
    if updated.matched_count == 0:
        return jsonify({"error": "Flight not found"}), 404
    
    return jsonify({"message": "User added to flight"}), 200

if __name__ == '__main__':
    app.run(debug=True)
