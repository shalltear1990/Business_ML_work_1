import pika
import json
import pandas as pd

#LOGS_PATH = './../logs/metric_log.csv'
LOGS_PATH = './logs/metric_log.csv'

#HOST = 'localhost'
HOST = 'rabbitmq'

Y_TRUE_QUEUE_NAME = 'y_true'
Y_PRED_QUEUE_NAME = 'y_pred'
PLOT_QUEUE_NAME = 'plot'

data = {}

try:

    # Каждый новый запуск приложения запись будет вестись с нуля
    pd.DataFrame(
        columns=['id',
                 'y_true',
                 'y_pred',
                 'absolute_error']
                ).to_csv(LOGS_PATH, index=False)

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(HOST)
        )
    channel = connection.channel()

    channel.queue_declare(queue=Y_TRUE_QUEUE_NAME)
    channel.queue_declare(queue=Y_PRED_QUEUE_NAME)
    channel.queue_declare(queue=PLOT_QUEUE_NAME)

    def callback(ch, method, properties, body):
        message = json.loads(body)
        id = message['id']
        y = message['body']

        print(f'Из очереди {method.routing_key} получено значение {message}')

        if id in data.keys():
            if method.routing_key == Y_TRUE_QUEUE_NAME:
                abs_err = abs(y - data[id][Y_PRED_QUEUE_NAME])
                pd.DataFrame({
                    'id': [id],
                    'y_true': [y],
                    'y_pred': [data[id][Y_PRED_QUEUE_NAME]],
                    'absolute_error': [abs_err]
                }).to_csv(
                    LOGS_PATH,
                    mode='a',
                    index=False,
                    header=False
                    )
            else:
                abs_err = abs(data[id][Y_TRUE_QUEUE_NAME] - y)
                pd.DataFrame({
                    'id': [id],
                    'y_true': [data[id][Y_TRUE_QUEUE_NAME]],
                    'y_pred': [y],
                    'absolute_error': [abs_err]
                }).to_csv(
                    LOGS_PATH,
                    mode='a',
                    index=False,
                    header=False
                    )
            channel.basic_publish(exchange='',
                                  routing_key=PLOT_QUEUE_NAME,
                                  body=json.dumps('plot_triger'))
        else:
            if method.routing_key == Y_TRUE_QUEUE_NAME:
                data[id] = {
                    Y_TRUE_QUEUE_NAME: y,
                    Y_PRED_QUEUE_NAME: ''
                }
            else:
                data[id] = {
                    Y_TRUE_QUEUE_NAME: '',
                    Y_PRED_QUEUE_NAME: y
                }

    channel.basic_consume(
        queue=Y_TRUE_QUEUE_NAME,
        on_message_callback=callback,
        auto_ack=True
    )
    channel.basic_consume(
        queue=Y_PRED_QUEUE_NAME,
        on_message_callback=callback,
        auto_ack=True
    )

    channel.start_consuming()

except Exception:
    print('Не удалось подключиться к очереди')
