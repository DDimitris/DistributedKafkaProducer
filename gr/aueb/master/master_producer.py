import threading, cv2, time
from kafka import KafkaProducer


class Producer(threading.Thread):
    daemon = True
    config = {}
    topic = ""
    file = ""
    thread_id = None

    def __init__(self, config, topic, file, thread_id):
        threading.Thread.__init__(self)
        self.config = config
        self.topic = topic
        self.file = file
        self.thread_id = thread_id

    def run(self):
        video = cv2.VideoCapture(self.file)
        producer = KafkaProducer(**self.config)
        producer.send(self.topic, key="Start".encode(), value="Start".encode())
        while (video.isOpened):
            success, image = video.read()
            if not success:
                break
            ret, jpeg = cv2.imencode('.png', image)
            producer.send(self.topic, jpeg.tobytes())
            time.sleep(0.2)
        producer.send(self.topic, key="Done".encode(), value="Done".encode())
        video.release()
