import datetime
from flask import Flask, Response
from kafka import KafkaConsumer

# Fire up the Kafka Consumer
topic = "HoangPhuc-distributed-video1"
kafka_server="kafka:9092"

consumer = KafkaConsumer(
    topic, 
    bootstrap_servers=kafka_server)

# Set the consumer in a Flask App
app = Flask(__name__)

@app.route('/health')
def checkHealth():
    return "OK", 200

@app.route('/', methods=['GET'])
def video():
    """
    This is the heart of our video display. Notice we set the mimetype to 
    multipart/x-mixed-replace. This tells Flask to replace any old images with 
    new values streaming through the pipeline.
    """
    return Response(
        get_video_stream(), 
        mimetype='multipart/x-mixed-replace; boundary=frame')

def get_video_stream():
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer:
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
    print("This code is set up by Le Nguyen Hoang Phuc - 23521198")