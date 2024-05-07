import time
import random
import json
from kafka import KafkaConsumer, KafkaProducer #Como se ejecutará por Colab, no es necesario instalar kafka

class Producer:
    def __init__(self, topic, freq):
        self.dato = (time.time(), round(random.uniform(10,40), 4))
        self.freq = freq #frecuencia del envio de datos
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    def enviar_dato(self):
        #self.producer.send(self.topic, value = (dict_data["timestamp"], dict_data["temperaturas"]))
        time_end = time.time() + 60 # el producer estará durante 1 minuto enviando datos al topic
        while time.time() <= time_end:
            self.producer.send(self.topic, value = self.dato)
            self.dato = (time.time(), round(random.uniform(10,40), 4))
            time.sleep(self.freq)

if __name__ == "__main__":
    producer = Producer("temperaturas", 5)
    producer.enviar_dato()