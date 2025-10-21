import time
import cv2
from kafka import KafkaProducer

topic = "HoangPhuc-distributed-video1"
kafka_server = "kafka:9092"


def publish_video(video_file):
    """
    Publish given video file to a specified Kafka topic. 
    Kafka Server is expected to be running on the localhost. Not partitioned.
    
    :param video_file: path to video file <string>
    """
    # Start up producer
    producer = KafkaProducer(bootstrap_servers=kafka_server)

    # Open file
    video = cv2.VideoCapture(video_file)
    
    print('publishing video...')

    while(video.isOpened()):
        success, frame = video.read()

        # Ensure file was read successfully
        if not success:
            print("bad read!")
            break
        
        # Convert image to png
        ret, buffer = cv2.imencode('.jpg', frame)

        # Convert to bytes and send to kafka
        producer.send(topic, buffer.tobytes())

        time.sleep(0.2)
    video.release()
    print('publish complete')
try:
    video_path="video.MOV"
    publish_video(video_path)
except KeyboardInterrupt:
    print("Break\n")
    print("Live ended...")