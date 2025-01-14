from RabbitMQ.message_queue import Producer

producer = Producer('消息队列名称')

urls = []
for url in urls:
    print(url)
    producer.send_message(url)