from setuptools import setup

setup(
    name='KafkaDistributedClients',
    version='0.1',
    packages=['agent', 'utils'],
    url='',
    license='',
    author='Dimitris Dedousis',
    author_email='dimitris.dedousis@gmail.com',
    description='A distributed producer for use with Apache Kafka',
    install_requires=["kafka-python", "cv2"]
)
