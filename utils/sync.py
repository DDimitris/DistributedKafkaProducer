from gr.aueb.utils.colors import bcolors
from kafka import KafkaProducer

from utils.kafka_configurations import master_producer_configs as prod_conf


class Sync:
    __topic = ""
    __file = ""
    __no_producers = ""
    __machines = ""
    __internal_sync_topic = ""
    __brokers = ""

    def __init__(self, topic, internal_sync_topic, file, no_producers, machines, brokers):
        self.__topic = topic
        self.__internal_sync_topic = internal_sync_topic
        self.__file = file
        self.__no_producers = no_producers
        self.__machines = machines
        self.__brokers = brokers

    def synchronize(self):
        remote_producers = int(self.__no_producers) / int(self.__machines)
        local_producers = int(self.__no_producers) - (int(remote_producers) * (int(self.__machines) - 1))
        print(bcolors.BOLD + "Local producers " + str(int(local_producers)) +
              " Remote producers " + str(int(remote_producers)) + bcolors.ENDC)
        producer = KafkaProducer(**prod_conf)
        message = self.__topic + " " + self.__file + " " + str(int(remote_producers)) + " " + str(
            self.__no_producers) + " " + self.__brokers
        print(bcolors.BOLD + "Sending to topic \"" + self.__internal_sync_topic +
              "\" the message \"" + message + "\"" + bcolors.ENDC)
        producer.send(self.__internal_sync_topic, key="Start".encode(), value=message.encode())
        print("Sync in progress...")
        return int(remote_producers), int(local_producers)
