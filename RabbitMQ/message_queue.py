import pika


class RabbitMQ(object):
    def __init__(self, host='192.168.1.149', port=5672, username='user_157', password='go_data_2333'):
        credentials = pika.PlainCredentials(username, password)
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host, port, heartbeat=0, credentials=credentials))
        self._channel = self._connection.channel()

    def __enter__(self):
        return self

    def __exit__(self, type_, value, trace):
        self._channel.close()
        self._connection.close()

    def close(self):
        self._channel.close()
        self._connection.close()


class Producer(RabbitMQ):
    def __init__(self, queue_name):
        """
        这是消息队列中生产者， 根据queue_name的不同发送信息到不同队列中
        样例：
        with Producer('XXXXX') as aaa:
            aaa.send_message('message')
        """
        super(Producer, self).__init__()
        self._queue_name = queue_name
        self._channel.queue_declare(queue=queue_name, durable=True)

    def send_message(self, message):
        self._channel.basic_publish(exchange='', routing_key=self._queue_name, body=message,
                                    properties=pika.BasicProperties(delivery_mode=2))


class Consumer(RabbitMQ):
    def __init__(self, queue_name):
        """
        这是消息队列中消费者， 根据queue_name的不同接收信息
        样例：
        with Consumer('XXXXX') as aaa:
            for message in aaa.read_message():
                print(message)
        """
        super(Consumer, self).__init__()
        self._queue_name = queue_name
        self._channel.queue_declare(queue=self._queue_name, durable=True)

    def read_messages(self):
        while True:
            method_frame, header_frame, body = self._channel.basic_get(self._queue_name, auto_ack=True)
            if method_frame:
                yield body
            else:
                break

    def read_message(self, callback):
        # 设置每次只从队列中取一个消息 进行处理
        self._channel.basic_qos(prefetch_count=1)
        # 指定回调函数，处理收到的消息
        self._channel.basic_consume(queue=self._queue_name, on_message_callback=callback)
        self._channel.start_consuming()

    def query_counts(self):
        queue = self._channel.queue_declare(queue=self._queue_name, passive=True)
        return queue.method.message_count

    def clear_queue(self):
        self._channel.queue_purge(queue=self._queue_name)
        print(f"队列 {self._queue_name} 所有消息已被清空.")