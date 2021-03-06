import getopt
import signal
import sys
import time

from agent.agent_consumer import Consumer
from utils.sync import Sync
from utils.video_producer import Producer as VideoProducer

from utils.kafka_configurations import internal_sync_topic
from utils.kafka_configurations import master_producer_configs as master_prod_conf


def main(argv):
    topic = ""
    no_producers = ""
    vfile = ""
    machines = ""
    brokers = ""
    type = ""

    try:
        opts, args = getopt.getopt(argv, "ht:p:f:m:b:a:",
                                   ["topic=", "producers=", "file=", "machines=", "brokers=", "type="])
    except getopt.GetoptError:
        print("python video_producer.py -t video_streaming -p 2 -f video-10-fps.mp4 -m 2 -b 2")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print("python video_producer.py -t video_streaming -p 2 -f video-10-fps.mp4 -m 2 -b 8 -a master")
            sys.exit()
        elif opt in ("-t", "--topic"):
            topic = arg
        elif opt in ("-p", "--producers"):
            no_producers = arg
        elif opt in ("-f", "--file"):
            vfile = arg
        elif opt in ("-m", "--machines"):
            machines = arg
        elif opt in ("-b", "--brokers"):
            brokers = arg
        elif opt in ("-a", "--type"):
            type = arg

    if type.lower() == "master":
        start_master_producer(topic, vfile, no_producers, machines, brokers)
    else:
        start_agent_producer()


def start_agent_producer():
    c = Consumer(internal_sync_topic)
    c.start()
    signal.signal(signal.SIGINT, signal_handler)
    signal.pause()
    while True:
        time.sleep(1)


def start_master_producer(topic, vfile, no_producers, machines, brokers):
    signal.signal(signal.SIGINT, signal_handler)
    signal.pause()
    s = Sync(topic, internal_sync_topic, vfile, no_producers, machines, brokers)
    remote_producers, local_producers = s.synchronize()
    print("Total threads starting in master producer are " + str(int(local_producers)))
    producer_list = []
    for i in range(0, int(local_producers)):
        p = VideoProducer(master_prod_conf, topic, vfile)
        p.start()
        producer_list.append(p)
    for prod_thread in producer_list:
        prod_thread.join()


def signal_handler(signal, frame):
    sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
