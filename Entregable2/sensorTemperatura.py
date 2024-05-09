from abc import ABC, abstractmethod
import numpy as np
from functools import reduce
import time
import random


#R1. Singleton
"""
Se ha decidido usar el patrón Singleton por el hecho de que únicamente nos interesa tener una sola instancia
del sistema gestor.
"""
class SistemaGestor:
    """
    La implementación del patrón es idéntica a la que aparece en las diapositivas de teoría, cambiando, obviamente
    los atributos y métodos que necesitamos para conseguir el funcionamiento requerido.
    """
    _unicaInstancia = None #Se usará posteriomente para controlar la existencia de una única instancia de esta clase

    def __init__(self):
        self.datos = [] #Lista de datos, con un máximo de 12 (60 segundos)
        self.estadisticos = {} #Diccionario de estadísticos
        self.umbral = np.inf #Valor del umbral inicializado a infinito, que posteriormente se podrá cambiar
        self.supera_umbral = False #Almacena una lista de datos (timestamp, temperatura) donde temperatura supere el umbral establecido
        self.sobrecrecimiento = False #Almacena el primer par de datos (timestamp, temperatura) de los últimos 30 segundos (últimos 6 elementos)
                                    # en que la diferencia de temperaturas es mayor que 10.
        self.ops = ["estadisticos", "umbral", "sobrecrecimiento"] #Operaciones del patrón Chain of Responsibilities



    @classmethod
    def get_instance(cls):
        """
        Método para obtener la única instancia de la clase SistemaGestor.
        Si se invoca cuando ya existe una instancia, devuelve esta misma.
        """
        if cls._unicaInstancia == None:
            cls._unicaInstancia = cls()
        return cls._unicaInstancia
    
    def set_umbral(self, umbral):
        """
        Método para establecer el umbral de temperatura.
        """
        self.umbral = umbral

    def actualizar(self, dato):
        """
        Actualiza los datos almacenados al recibir un nuevo dato, manteniendo
        únicamente 12 datos (60 segundos). Si se supera esta cantidad, se elimina 
        la más antigua para almacenar el último dato.
        """

        #Cuando se quiere añadir un dato cuando hemos alcanzado los 12 datos,
        # eliminamos el primero y desplazamos todos los restantes una posición
        # a la izda, y colocamos en la última posición el nuevo dato. 
        if len(self.datos) == 12:
            i = 1
            while i < 11:
                aux = self.datos[i]
                self.datos[i-1] = aux
                i = i + 1
            self.datos[-1] = dato
        else: #Si no se ha alcanzado el límite, simplemente lo añadimos a la lista
            self.datos.append(dato)
        
        self.procesar() #Una vez añadido el nuevo dato, realizamos las operaciones sobre la lista de datos.
    
    
    # Hace uso del patrón Chain of Responsibilities
    # Estadistico -> Umbral -> Sobrecrecimiento
    def procesar(self):
        """
        Se lleva a cabo los 3 pasos requeridos usando el patrón
        Chain of Responsibilities.
        """
        #Instanciamos los manejadores
        op1 = Sobrecrecimiento()
        op2 = Umbral(successor=op1)
        op3 = Estadistico(successor=op2)

        for op in self.ops: #Se ha separado del resto la operacion Estadistico ya que
                            # se ha utilizado el patrón Strategy para la implementación de esta.
            if op == "estadisticos":
                e1 = MeanSV() #Instanciamos las distintas estrategias
                e2 = MaxMin()
                e3 = Quantile()
                estrategias = [e1, e2, e3]
                for e in estrategias: #Calculamos los estadísticos de cada estrategia
                    op3.set_estrategia(e)
                    op3.realizar_operacion(op, self.estadisticos, list(zip(*self.datos))[1])
                    #El último parámetro lo que hace es: separar los pares de elementos de cada tupla (timestamp, temp) que forma
                    # parte de la lista de datos, almacenar cada una de ellas en una tupla distinta, consiguiendo asi
                    # otra lista cuyo primer elemento es una tupla de todos los timestamps, y la segunda, otra tupla de 
                    # todas las temperaturas. De esta nueva lista, únicamente necesitamos la tupla de temperaturas para
                    # calcular los estadísticos.

            else:
                op2.realizar_operacion(gestor= self, op = op, l = self.datos, umbral=self.umbral)
    
    def mostrar_info(self):
        """
        Imprime algunos atributos de interés del sistema gestor.
        """
        print("Estadisticos: ", self.estadisticos)
        print("Sobrecrecimiento detectado en los ultimos 30 seg (el primero): ", self.sobrecrecimiento)
        print("Temperaturas que superan el umbral: ", self.supera_umbral)
            
            
    

#R2. Observer
"""
Para llevar a cabo el requerimiento de un sistema de notificación, y la recepción de esta por parte
de los clientes, se ha usado el patrón Observer.
"""
class Observable:
    """
    Clase Observable, el encargado de publicar los datos y de notificar a los clientes.
    En nuestro caso, únicamente tenemos 1 cliente, el sistema gestor.
    """
    def __init__(self):
        self.cliente = None

    def activar(self, gestor:SistemaGestor):
        """
        Establecer "conexión" con el cliente pasado como parámetro.
        """
        self.cliente = gestor

    def desactivar(self):
        """
        "Desconectar" con el cliente actual. Luego no podrá recibir más notificaciones.
        """
        self.cliente = None #En este caso, simplemente ponemos el atributo cliente a None,
                            # luego si tratamos de desactivar sin antes haber activado, no ocurriría ningún error.

    def notificar(self, dato):
        """
        Notifica al cliente con el dato proporcionado como parámetro.
        Si no existe cliente activado o no es el sistema gestor, salta error.
        """
        if isinstance(self.cliente, SistemaGestor):
            self.cliente.actualizar(dato) #El sistema utiliza este nuevo dato para realizar todas las operaciones.
        
        else:
            raise Exception("Cliente no activado o no es SistemaGestor!")

class Sensor(Observable):
    """
    Clase Sensor, representa el sensor de temperatura que manda los datos de temperatura al sistema.
    """
    def __init__(self):
        """
        Almacena el último dato recolectado en un atributo
        """
        self.dato = None

    def enviar_dato(self, dato):
        """
        Notifica al cliente con el dato proporcionado como parámetro, haciendo uso
        del método notificar de la clse Observable.
        """
        self.dato = dato
        self.notificar(self.dato)
    
    def enviar_dato_tiempo_real(self, duracion):
        """
        Simula el envío de datos (aleatorios) al sistema en tiempo real durante un tiempo indicado mediante un parámetro.
        """
        time_end = time.time() + duracion #Tiempo de finalización de envío de datos
        while time.time() <= time_end:
            dato = (time.time(), round(random.uniform(20,40),4)) #Los datos son generados aleatoriamente entre 20,40
            self.enviar_dato(dato)
            time.sleep(5) #Tras el envío de cada dato, esperamos 5 segundos
"""
Nota: No se ha implementado la clase abstracta (o interfaz) Observer debido a que en este caso se establece
explícita la existencia de una única instancia SistemaGestor(el observador, o nuestro cliente), por lo que 
nos ha parecido redundante crear la clase Observer del que va a heredar únicamente un observador concreto.
"""



#R3. Chain of responsibilities
"""
Para llevar a cabo los 3 pasos encadenados de la tarea, hemos optado por el patrón Chain of responsibilities
por la posibilidad de tener objetos distintos que cubran pasos distintos desacoplando el emisor de una petición al
receptor.
"""
class Manejador(ABC):
    """
    Clase abstracta Manejador, del que heredarán todas las clases concretas que llevaran a cabo los 3 pasos.
    """
    def __init__(self, successor = None):
        self.successor = successor
    
    def realizar_operacion(self):
        pass


#R4. Strategy (dentro del R3)
"""
Para el primer paso de los 3, pide explícitamente la implementación de varias estrategias para la computación,
luego está claro que vamos a usar el patrón Strategy.
"""
class Strategy:
    def estrategia(self):
        pass

class MeanSV(Strategy): #Estrategia concreta encargada de calcular la media y la desviación típica
    def estrategia(self, d, l): #Como los diccionarios y las listas se pasan por referencia, podemos usarlos como parámetros directamente
                                #y modificarlas.
        """
        Calcula la media y la desviación típica de la lista de datos, y serán almacenadas en el diccionario de estadísticos
        pasado como parámetro.
        """
        n = len(l)
        mean = round(reduce(lambda x, y: x+y, l) / n, 4) #media
        sv = round(np.sqrt(sum(map(lambda x: (x-mean)**2, l))/(n-1)), 4) if (n-1) != 0 else 0 #desviación típica
        #para el cálculo de la desviación, como depende del tamaño de la lista de datos, en caso
        # de que esta sea 1, tendremos un error de división nula, luego si el denominador
        # es nulo, la desviación lo ponemos a 0.
        
        #Actualizamos el diccionario de estadísticos
        d["media"] = mean
        d["Desviacion Tipica"] = sv

class Quantile(Strategy): #Estrategia encargada de calcular los cuantiles (cuartiles solo)
    def estrategia(self, d, l):
        """
        Calcula los cuartiles de la lista de datos, y serán almacenados en el diccionario de estadísticos
        pasado como parámetro.
        """
        n = len(l)
        l_ordenado = sorted(l) #Los cuantiles se calculan con la lista ordenada.
        #dependiento de si la longitud de la lista de datos sea par o impar, el cálculo se difiere:
        q25 = round(list(map(lambda x: x[(n+1)//4 - 1] if (n+1)%4 == 0 else ((x[(n+1)//4 - 1] + x[(n+1)//4])/2), [l_ordenado]))[0], 4)
        median = round(list(map(lambda x: x[(n-1)//2] if n%2 == 1 else ((x[(n-1)//2] + x[n//2])/2), [l_ordenado]))[0], 4)
        q75 = round(list(map(lambda x: x[(3*(n+1))//4 - 1] if (n+1)%4 == 0 else ((x[(3*(n+1))//4 - 1] + x[((3*(n+1))//4)%n])/2), [l_ordenado]))[0], 4)
        #Sólo por poder usar map en el cómputo, se ha pasado a dicha función el valor [lista_ordenada], así el map
        # recibe una lista de 1 único elemento, que es la lista de datos ordenados, y realiza el cálculo correspondiente
        # devolviendo un generador, que lo pasamos a lista y este contendrá únicamente el cuartil buscado (lo extraemos con el índice 0).

        #Actualizamos el diccionario de estadísticos
        d["mediana"] = median
        d["Q1"] = q25
        d["Q3"] = q75

class MaxMin(Strategy): #Estrategia encargada de calcular el máximo y el mínimo.
    def estrategia(self, d, l):
        """
        Calcula el máximo y el mínimo de la lista de datos, y serán almacenados en el diccionario de estadísticos
        pasado como parámetro.
        """
        #poco que decir...
        maximo = reduce(lambda x,y: x if x>y else y, l)
        minimo = reduce(lambda x,y: x if x<y else y, l)

        d["max"] = maximo
        d["min"] = minimo

class Estadistico(Manejador):
    """
    Esta clase funciona como el contexto del patrón Strategy, y también como otro manejador de
    la cadena de responsabilidades.
    """
    def __init__(self, successor = None):
        Manejador.__init__(self, successor = successor) #Como manejador, tendrá (o no) un sucesor
        self.estrategia = None #Y como contexto, tendrá asignada una estrategia concreta
    
    def set_estrategia(self, estrategia:Strategy):
        """
        Establece la estrategia que se va a usar.
        """
        self.estrategia = estrategia

    def realizar_operacion(self, op, d, l):
        """
        Realiza la operación si le corresponde. Sino, lo pasa a su sucesor.
        """
        if op == "estadisticos":
            self.estrategia.estrategia(d, l) #La operacion es realizada mediante una de las estrategias antes definidas.
        
        elif self.successor:
            self.successor.realizar_operacion(op)


class Umbral(Manejador):
    """
    Clase manejador encargado de comprobar si hay alguna temperatura que supera el umbral establecido.
    """
    def realizar_operacion(self, **kwargs):
        """
        Realiza la operación si le corresponde. Sino, lo pasa a su sucesor.
        """
        #Aquí recalcar el uso del parámetro **kwargs: el principal motivo de esto es ahorrarnos comprobaciones innecesarias
        # a la hora de llamar a estos métodos desde el sistema gestor. Como para cada paso u operación, los parámetros
        # requeridos se difieren, usamos **kwargs para poder pasarles todos los parámetros que queramos y extraer
        # únicamente los que vamos a usar, eliminando así la necesidad de comprobar qué operación vamos a hacer
        # para decidir los parámetros que le vamos a pasar.
        op = kwargs["op"]

        if op == "umbral":
            umbral = kwargs["umbral"]
            l = kwargs["l"]
            gestor = kwargs["gestor"] #El hecho de que exista o no temperaturas que superen el umbral está controlado
                                    # por un atributo del sistema gestor, luego debemos pasarle la propia instancia
                                    # del sistema gestor y modificar el atributo correspondiente.

            overheat = list(filter(lambda x: x[1] > umbral, l)) #Filtramos la lista de temperaturas

            if len(overheat) == 0: #Si la lista filtrada está vacía, es que no hay ninguna temperatura superior al umbral.
                gestor.supera_umbral = False

            else: #Si no, guardamos la lista de temperaturas que superan el umbral en el atributo correspondiente.
                gestor.supera_umbral = overheat
        
        elif self.successor:
            self.successor.realizar_operacion(**kwargs)


class Sobrecrecimiento(Manejador):
    """
    Clase manejador encargado de comprobar si ha habido un crecimiento de temperatura superior a 10 grados
    en los últimos 30 segundos.
    """
    def realizar_operacion(self, **kwargs):
        """
        Realiza la operación si le corresponde. Sino, lo pasa a su sucesor.
        """
        #Aquí, de nuevo, se ha usado **kwargs por las mismas razones mencionadas anteriormente.
        op = kwargs["op"]

        if op == "sobrecrecimiento":
            l = kwargs["l"]
            gestor = kwargs["gestor"] #Necesitamos el propio gestor para poder modificar el atributo correspondiente, que 
                                    # no se se pasa por referencia.

            if len(l) <= 1: #Si sólo hay 1 dato, no ninguno, no podemos comparar, luego no hay crecimiento.
                gestor.sobrecrecimiento = False
                return 0 #para terminar el procesamiento
            
            elif len(l) <= 6: #Si hay menos de 6 datos (han pasado menos de 30 segundos), seleccionamos todos los datos
                                # para comprobar si hay crecimiento de más de 10 grados con cada pareja.
                l30s = l
            
            else: #Si hay más de 6 datos (han pasado más de 30 segundos), cogemos los últimos 6 datos (últimos 30 segundos) 
                    #para hacer la búsqueda
                l30s = l[-6:]

            #La búsqueda en sí lo haremos usando doble puntero (o índice).
            #La idea es simple: empezamos con el dato más antiguo (el de hace 30 segundos) fijando un puntero sobre él (i), y
            # con el otro puntero apuntado al siguiente dato a este (j), comparamos las temperaturas haciendo la diferencia
            # de temp[j] - temp[i]. Si esta diferencia resulta ser superior a 10, significa que ha habido un crecimiento mayor
            # que 10 grados. Sino, avanzamos el puntero (j). Si (j) llega hasta el final, y la diferencia no ha sido mayor
            # que 10 en ningún caso, avanzamos (i) y establecemos (j) como el siguiente elemento de (i). Así sucesivamente,
            # hasta encontrar una pareja cuya diferencia de temperaturas supere a los 10 grados, o que (i) llegue hasta el último
            # elemento.
            #Cabe recalcar que la diferencia se hace con el elemento más "nuevo" menos el elemento más antiguo, si este
            # resulta ser positivo, estamos hablando de un crecimiento. 
            i = 0
            j = 1

            while i < len(l30s):
                while j < len(l30s):
                    if l30s[j][1] - l30s[i][1] >= 10:

                        gestor.sobrecrecimiento = [l30s[i], l30s[j]] #Devolvemos la primera pareja de temperaturas cuya diferencia supera los 10º
                        return 0
                    
                    j = j + 1

                i = i + 1
                j = i + 1

            gestor.sobrecrecimiento = False #Si hemos terminado el bucle y en ningún momento se ha interrumpido
                                            # por haber encontrado una pareja cuya diferencia sea superior a 10,
                                            # significa que durante los últimos 30 segundos, no ha habido un crecimiento
                                            # de ese nivel.

        elif self.successor:
            self.successor.realizar_operacion(**kwargs)

#### PRUEBAS ####
if __name__ == "__main__":
    gestor = SistemaGestor.get_instance() #Obtenemos una instancia del sistema gestor
    sensor = Sensor() #Obtenemos una instancia del sensor, que será quién envíe los datos (el observable)
    sensor.activar(gestor) # "Conectamos" el gestor con el sensor.

    gestor.set_umbral(30) #Establecemos el umbral de temperatura

    #Simulación de envío de datos cada 5 segundos durante n segundos:
    n = 15
    time_end = time.time() + (n+1) #Tiempo de terminación. (+1) por el retardo que genera ciertas instrucciones.
    while time.time() <= time_end: #Mientras que el tiempo de actual sea menor a tiempo de terminación
        sensor.enviar_dato((time.time(), round(random.uniform(20,40), 4))) #Enviamos un dato aleatorio al cliente
        gestor.mostrar_info() #Mostramos cierta información de interés cada vez que enviamos un dato
                                # para observar cómo se va actualizando
        time.sleep(5)
        print("\n"*3)

    """
    Nota: El código anterior es equivalente a usar el método "enviar_dato_tiempo_real" implementado en la clase
    Sensor, pero como queremos ver cómo se van actualizando los estadísticos y tampoco queremos meter
    ningún print al método, pues lo hemos hecho así.
    """