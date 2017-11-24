import threading, cv2, time
from kafka import KafkaProducer


class Producer(threading.Thread):
    daemon = True
    __config = {}
    __topic = ""
    __file = ""

    def __init__(self, config, topic, file):
        threading.Thread.__init__(self)
        self.__config = config
        self.__topic = topic
        self.__file = file

    def run(self):
        video = cv2.VideoCapture(self.__file)
        producer = KafkaProducer(**self.__config)
        print("Start Emitting...")
        while video.isOpened:
            success, image = video.read()
            if not success:
                break
            ret, jpeg = cv2.imencode('.png', image)
            producer.send(self.__topic, jpeg.tobytes())
            time.sleep(0.2)
        print("Done Emitting!")
        producer.send(self.__topic, key="Done".encode(), value="Done".encode())
        video.release()
