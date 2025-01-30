import pika
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

#LOGS_PATH = './../logs/metric_log.csv'
#PLOT_IMAGE_PATH = './../logs/error_distribution.png'
LOGS_PATH = './logs/metric_log.csv'
PLOT_IMAGE_PATH = './logs/error_distribution.png'

#HOST = 'localhost'
HOST = 'rabbitmq'

PLOT_QUEUE_NAME = 'plot'

try:

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(HOST)
        )
    channel = connection.channel()

    channel.queue_declare(queue=PLOT_QUEUE_NAME)

    def callback(ch, method, properties, body):
        df = pd.read_csv(LOGS_PATH)
        abs_err = df['absolute_error']
        plt.style.use('bmh')
        plt.figure(figsize=(10, 6))
        plt.hist(abs_err, bins=30)
        plt.xlabel('absolute_error')
        plt.ylabel('count')
        plt.savefig(PLOT_IMAGE_PATH)
        print('plot.py: график распределения был обновлён ('
              + str(datetime.now())[:-7]
              + ')')

    channel.basic_consume(
        queue=PLOT_QUEUE_NAME,
        on_message_callback=callback,
        auto_ack=True
    )

    channel.start_consuming()

except Exception:
    print('Не удалось подключиться к очереди')
