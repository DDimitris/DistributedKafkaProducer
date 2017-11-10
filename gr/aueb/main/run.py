import random, sys, getopt, time, signal
from gr.aueb.master.sync import Sync
from gr.aueb.master.master_producer import Producer as MasterProducer
from gr.aueb.utils.kafka_configurations import master_producer_configs as master_prod_conf
from gr.aueb.utils.kafka_configurations import internal_sync_topic
from gr.aueb.agent import agent_consumer as Consumer


def main(argv):
    trans_id = random.randint(1, 100000)
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
        print("python master_producer.py -t video_streaming -p 2 -f video-10-fps.mp4 -m 2 -b 2")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print("python master_producer.py -t video_streaming -p 2 -f video-10-fps.mp4 -m 2 -b 8 -a master")
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

    if (type.lower() == "master"):
        s = Sync(topic, internal_sync_topic, vfile, no_producers, machines, brokers, trans_id)
        remote_producers, local_producers = s.Syncronize()
        time.sleep(1)

        # Start producer threads for video streaming
        print("Total threads starting in master producer are " + str(int(local_producers)))
        for i in range(0, int(local_producers)):
            print("Starting producer " + str(i + 1) + "...")
            p = MasterProducer(master_prod_conf, topic, vfile, i + 1)
            p.start()
        while True:
            if producer_counter == local_producers:
                sys.exit(0)
    else:
        c = Consumer()
        c.setTopic(internal_sync_topic)
        c.start()
        signal.signal(signal.SIGINT, signal_handler)
        signal.pause()
        while True:
            time.sleep(1)


def signal_handler(signal, frame):
    sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
