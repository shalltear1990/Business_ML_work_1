import pika
import numpy as np
import json
from sklearn.datasets import load_diabetes
from datetime import datetime
import time

#HOST = 'localhost'
HOST = 'rabbitmq'

Y_TRUE_QUEUE_NAME = 'y_true'
X_QUEUE_NAME = 'features'

try:

    X, y = load_diabetes(return_X_y=True)

    while True:

        current_time = datetime.now()
        message_id = datetime.timestamp(current_time)
        random_row = np.random.randint(0, X.shape[0]-1)

        message_y_true = {
            'id': message_id,
            'body': y[random_row]
            }

        message_X = {
            'id': message_id,
            'body': list(X[random_row])
            }

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(HOST)
            )
        channel = connection.channel()

        channel.queue_declare(queue=Y_TRUE_QUEUE_NAME)
        channel.queue_declare(queue=X_QUEUE_NAME)

        channel.basic_publish(exchange='',
                              routing_key=Y_TRUE_QUEUE_NAME,
                              body=json.dumps(message_y_true))
        channel.basic_publish(exchange='',
                              routing_key=X_QUEUE_NAME,
                              body=json.dumps(message_X))
        print('features.py: успешная отправка сообщения ('
              + str(current_time)[:-7]
              + ')')

        connection.close()

        time.sleep(10)

except Exception:
    print('Не удалось подключиться к очереди')
