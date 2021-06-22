from kafka import KafkaProducer
bootstrap_servers = ['localhost:9092']
topicName = 'myTopic'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers,api_version=(0,11,5))
producer = KafkaProducer()
ack = producer.send(topicName, b'jojo mast hai 14')
metadata = ack.get()
print(metadata.topic)
print(metadata.partition)

