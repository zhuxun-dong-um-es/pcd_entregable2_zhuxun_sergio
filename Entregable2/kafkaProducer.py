import time
import random
import json
from kafka import KafkaConsumer, KafkaProducer #Como se ejecutará por Colab, no es necesario instalar kafka

class Producer:
    """
    Clase Producer, encargado de generar datos aleatorios y mandarlos al tópico correspondiente a cierta frecuencia.
    """
    def __init__(self, topic, freq):
        self.dato = (time.time(), round(random.uniform(10,40), 4)) #dato generado aleatoriamente
        self.freq = freq #frecuencia del envio de datos, en segundos
        self.topic = topic #tópico donde se enviarán los datos
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    def enviar_dato(self, n):
        time_end = time.time() + (n+1) # el producer estará durante n segundos enviando datos al topic.
                                        # (+1) por posibles retardos de ciertas instrucciones.
        while time.time() <= time_end:
            self.producer.send(self.topic, value = self.dato) #Enviamos al tópico el dato generado aleatoriamente
            self.dato = (time.time(), round(random.uniform(10,40), 4)) #Dato generado aleatoriamente
            time.sleep(self.freq) #La frecuencia de envío, en segundos

if __name__ == "__main__":
    producer = Producer("temperaturas", 5) #Frecuencia de envío establecido a 5 segundos.
    producer.enviar_dato(60) #Tiempo de envío establecido a 60 segundos.