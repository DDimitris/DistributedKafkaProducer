import multiprocessing
from kafka import KafkaConsumer
from gr.aueb.utils.colors import bcolors
from gr.aueb.utils.kafka_configurations import agent_producer_configs as prod_conf
from gr.aueb.utils.kafka_configurations import consumer_configs as cons_conf
from gr.aueb.utils.video_producer import Producer


class Consumer(multiprocessing.Process):
    daemon = True
    __sync_topic = ""

    def __init__(self, sync_topic):
        multiprocessing.Process.__init__(self)
        self.__sync_topic = sync_topic

    def run(self):
        consumer = KafkaConsumer(**cons_conf)
        consumer.subscribe([self.__sync_topic])
        for msg in consumer:
            print("Message received from master producer \"" + bcolors.BOLD + msg.value +
                  bcolors.ENDC + "\"")
            if msg.key == "Start":
                str_value = msg.value.split()
                total_producers = str_value[2]
                print("Total threads starting in agent producer are " + str_value[2])
                print("Sending video file \"" +
                      bcolors.BOLD + str_value[1] + bcolors.ENDC + "\" to topic \"" +
                      bcolors.BOLD + str_value[0] + bcolors.ENDC + "\"")
                for i in range(0, int(total_producers)):
                    p = Producer(prod_conf, str_value[0], str_value[1])
                    p.start()
