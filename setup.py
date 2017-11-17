from distutils.core import setup

setup(
    name='KafkaDistributedClients',
    version='0.1',
    packages=['gr', 'gr.aueb', 'gr.aueb.main', 'gr.aueb.sync', 'gr.aueb.agent', 'gr.aueb.utils', 'gr.aueb.master'],
    url='',
    license='',
    author='Dimitris Dedousis',
    author_email='dimitris.dedousis@gmail.com',
    description='A distributed producer for use with Apache Kafka', requires=["kafka-python", "cv2"]
)
