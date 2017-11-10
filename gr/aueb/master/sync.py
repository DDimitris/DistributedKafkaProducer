from gr.aueb.utils.kafka_configurations import master_producer_configs as prod_conf
from kafka import KafkaProducer
from gr.aueb.utils.colors import bcolors


class Sync():
    topic = ""
    file = ""
    no_producers = ""
    machines = ""
    internal_sync_topic = ""
    brokers = ""
    trans_id = ""

    def __init__(self, topic, internal_sync_topic, file, no_producers, machines, brokers, trans_id):
        self.topic = topic
        self.internal_sync_topic = internal_sync_topic
        self.file = file
        self.no_producers = no_producers
        self.machines = machines
        self.brokers = brokers
        self.trans_id = trans_id

    def Syncronize(self):
        remote_producers = int(self.no_producers) / int(self.machines)
        local_producers = int(self.no_producers) - (int(remote_producers) * (int(self.machines) - 1))
        print(bcolors.BOLD + "Local producers " + str(int(local_producers)) +
              " Remote producers " + str(int(remote_producers)) + bcolors.ENDC)
        producer = KafkaProducer(**prod_conf)
        message = self.topic + " " + self.file + " " + str(int(remote_producers)) + " " + str(
            self.no_producers) + " " + self.brokers + " " + str(self.trans_id)
        print(bcolors.BOLD + "Sending to topic \"" + self.internal_sync_topic +
              "\" the message \"" + message + "\"" + bcolors.ENDC)
        producer.send(self.internal_sync_topic, key="Start".encode(), value=message.encode())
        print("Sync in progress...")
        time.sleep(0.5)
        return (int(remote_producers), int(local_producers))
