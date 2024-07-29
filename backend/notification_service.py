from confluent_kafka import Producer, Consumer, KafkaException
import json
from email_service import send_email

KAFKA_TOPIC = 'flight_status'
KAFKA_SERVER = 'localhost:9092'

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def send_notification(message):
    p = Producer({'bootstrap.servers': KAFKA_SERVER})
    p.produce(KAFKA_TOPIC, key='flight_status', value=json.dumps(message), callback=delivery_report)
    p.flush()

def consume_notifications():
    c = Consumer({
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': 'flight_status_group',
        'auto.offset.reset': 'earliest'
    })

    c.subscribe([KAFKA_TOPIC])

    try:
        boolean = True
        while boolean:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'End of partition reached {msg.partition()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                message = json.loads(msg.value().decode("utf-8"))
                print(message)
                flightNo = message.get('flightNo')
                isStatusChange = message.get('isStatusChange')
                isGateNoChange = message.get('isGateNoChange')
                allPassenger = message.get('allPassenger')
                gateNo = message.get('gateNo')
                status = message.get('status')
                print(allPassenger)
                print(f'Received message: Flight ID {flightNo}, Status {status}')

                subject = ''
                body = ''
                if isStatusChange == 1 and isGateNoChange == 1:
                    subject = f"Flight Status Update"
                    body = f"The status of your flight with ID {flightNo} has been updated to {status} and new gate no is: {gateNo}."
                elif isGateNoChange == 1:
                    subject = f"Flight Gate No. Update: {gateNo}"
                    body = f"The status of your flight with ID {flightNo} has been updated to gate no: {gateNo}."
                elif isStatusChange == 1:
                    subject = f"Flight Status Update: {status}"
                    body = f"The status of your flight with ID {flightNo} has been updated to {status}."

                for user in allPassenger:
                    to_email = user.get('email')
                    send_email(to_email, subject, body)
            boolean = False   
                
    finally:
        c.close()
