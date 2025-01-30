import pika
import pickle
import numpy as np
import json
from datetime import datetime

MODEL_PATH = './myfile.pkl'

#HOST = 'localhost'
HOST = 'rabbitmq'

Y_TRUE_QUEUE_NAME = 'y_true'
Y_PRED_QUEUE_NAME = 'y_pred'
X_QUEUE_NAME = 'features'

with open(MODEL_PATH, 'rb') as pkl_file:
    regressor = pickle.load(pkl_file)

try:

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(HOST)
        )
    channel = connection.channel()

    channel.queue_declare(queue=X_QUEUE_NAME)
    channel.queue_declare(queue=Y_PRED_QUEUE_NAME)

    def callback(ch, method, properties, body):

        current_time = datetime.now()

        message = json.loads(body)
        features = np.array(message['body'])
        pred = regressor.predict(features.reshape(1, -1))[0]
        answer = {
            'id': message['id'],
            'body': pred
        }
        channel.basic_publish(exchange='',
                              routing_key=Y_PRED_QUEUE_NAME,
                              body=json.dumps(answer))

        print('model.py: успешное предсказание ('
              + str(current_time)[:-7]
              + ')')

    channel.basic_consume(
        queue=X_QUEUE_NAME,
        on_message_callback=callback,
        auto_ack=True
    )

    channel.start_consuming()

except Exception:
    print('Не удалось подключиться к очереди')
