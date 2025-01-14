from curl_cffi import requests

from RabbitMQ.message_queue import Consumer

def callback(ch, method, properties, body):
    url = body.decode('utf-8')
    print("Received URL:", url)

    response = requests.get(url,impersonate='chrome101')
    if response.status_code == 200:
        print(response.status_code)
        # 如果请求成功，确认消息已经被处理，并且从队列中移除
        ch.basic_ack(delivery_tag=method.delivery_tag)


channel = Consumer('消息队列名称').read_message(callback)
# 开始接收消息
channel.start_consuming()
