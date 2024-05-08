from abc import ABC, abstractmethod
import numpy as np
from functools import reduce

"""
Versión 8/5/2024:
    Se ha modificado el cálculo de los cuartiles de la primera de las estrategias. Además se ha suprimido un cálculo extra
    de la mediana.
    
    Cambio del orden de definición de las clases de las estrategias concretas (Quantile y MeanSV) acorde al orden descrito
    en el enunciado del entregable.
    
"""


#R1. Singleton
class SistemaGestor:

    _unicaInstancia = None

    def __init__(self):
        self.datos = [] #Lista de datos, que en principio almacenaremos 12
        self.estadisticos = {} #Diccionario de estadísticos
        self.umbral = np.inf
        self.supera_umbral = False
        self.sobrecrecimiento = False
        self.ops = ["estadisticos", "umbral", "sobrecrecimiento"]



    @classmethod
    def get_instance(cls):
        if cls._unicaInstancia == None:
            cls._unicaInstancia = cls()
        return cls._unicaInstancia
    
    def set_umbral(self, umbral):
        self.umbral = umbral

    def actualizar(self, dato):
        if len(self.datos) == 12:

            i = 1
            while i < 11:
                aux = self.datos[i]
                self.datos[i-1] = aux
                i = i + 1
            self.datos[-1] = dato
        
        else:
            self.datos.append(dato)
    
    
    # Estadistico -> Umbral -> Sobrecrecimiento
    # Hace uso del patrón Chain of Responsibilities
    def procesar(self):
        
        op1 = Sobrecrecimiento()
        op2 = Umbral(successor=op1)
        op3 = Estadistico(successor=op2)

        for op in self.ops:
            if op == "estadisticos":
                e1 = MeanSV()
                e2 = MaxMin()
                e3 = Quantile()
                estrategias = [e1, e2, e3]
                for e in estrategias:
                    op3.set_estrategia(e)
                    op3.realizar_operacion(op, self.estadisticos, list(zip(*self.datos))[1])

            else:
                op2.realizar_operacion(gestor= self, op = op, l = self.datos, umbral=self.umbral)
                #print(resultado)
            
            
    

#R2. Observer
class Observable:
    def __init__(self):
        self.cliente = None

    def activar(self, observer):
        self.cliente = observer

    def desactivar(self):
        self.cliente = None

    def notificar(self, dato):
        self.cliente.actualizar(dato)

class Sensor(Observable):
    def __init__(self):
        self.dato = None

    def enviar_dato(self, dato):
        self.dato = dato
        self.notificar(self.dato)
    

#R3. Chain of responsibilities
class Manejador(ABC):
    def __init__(self, successor = None):
        self.successor = successor
    
    def realizar_operacion(self):
        pass
    
    def set_manejador(self):
        pass


#R4. Strategy (dentro del R3)
class Strategy:
    def estrategia(self, d, l):
        pass

class MeanSV(Strategy):
    def estrategia(self, d, l):
        n = len(l)
        mean = round(reduce(lambda x, y: x+y, l) / n, 4)
        sv = round(np.sqrt(sum(map(lambda x: (x-mean)**2, l))/(n-1)), 4) if (n-1) != 0 else 0
            
        d["media"] = mean
        d["Desviacion Tipica"] = sv

class Quantile(Strategy):
    def estrategia(self, d, l):
        n = len(l)
        l_ordenado = sorted(l)
        q25 = list(map(lambda x: x[(n+1)//4 - 1] if (n+1)%4 == 0 else ((x[(n+1)//4 - 1] + x[(n+1)//4])/2), [l_ordenado]))[0]
        median = list(map(lambda x: x[(n-1)//2] if n%2 == 1 else ((x[(n-1)//2] + x[n//2])/2), [l_ordenado]))[0]
        q75 = list(map(lambda x: x[(3*(n+1))//4 - 1] if (n+1)%4 == 0 else ((x[(3*(n+1))//4 - 1] + x[(3*(n+1))//4])/2), [l_ordenado]))[0]
        print(q25, median, q75)

        d["mediana"] = median
        d["Q1"] = q25
        d["Q3"] = q75

class MaxMin(Strategy):
    def estrategia(self, d, l):
        maximo = reduce(lambda x,y: x if x>y else y, l)
        minimo = reduce(lambda x,y: x if x<y else y, l)

        d["max"] = maximo
        d["min"] = minimo

class Estadistico(Manejador):
    def __init__(self, successor = None):
        Manejador.__init__(self, successor = successor)
        self.estrategia = None
    
    def set_estrategia(self, estrategia:Strategy):
        self.estrategia = estrategia

    def realizar_operacion(self, op, d, l):
        if op == "estadisticos":
            self.estrategia.estrategia(d,l)
        
        elif self.successor:
            self.successor.realizar_operacion(op)
        
    def set_manejador(self, nuevo_manejador):
        self.successor = nuevo_manejador


class Umbral(Manejador):
    def realizar_operacion(self, **kwargs):
        op = kwargs["op"]

        if op == "umbral":
            umbral = kwargs["umbral"]
            l = kwargs["l"]
            gestor = kwargs["gestor"]

            overheat = list(filter(lambda x: x[1] > umbral, l))

            if len(overheat) == 0:
                gestor.supera_umbral = False
                #return False

            else:
                gestor.supera_umbral = overheat
                #return overheat
        
        elif self.successor:
            self.successor.realizar_operacion(**kwargs)
    
    def set_manejador(self, nuevo_manejador):
        self.successor = nuevo_manejador

class Sobrecrecimiento(Manejador):
    def realizar_operacion(self, **kwargs):
        op = kwargs["op"]

        if op == "sobrecrecimiento":
            gestor = kwargs["gestor"]
            l = kwargs["l"]

            if len(l) <= 1:
                gestor.sobrecrecimiento = False
                return 0 #para terminar el procesamiento
                #return False
            
            elif len(l) <= 6:
                l30s = l
            
            else:
                l30s = l[-6:]  #last 30 sec

            #Vamos a hacerlo por doble puntero xd
            i = 0
            j = 1

            while i < len(l30s):
                while j < len(l30s):
                    if l30s[j][1] - l30s[i][1] >= 10:

                        gestor.sobrecrecimiento = [l30s[i], l30s[j]] #Devuelve la primera pareja de temperaturas cuya diferencia supera los 10º
                        return 0
                    
                    j = j + 1

                i = i + 1
                j = i + 1

            gestor.sobrecrecimiento = False

        elif self.successor:
            self.successor.realizar_operacion(**kwargs)
    
    def set_manejador(self, nuevo_manejador):
        self.successor = nuevo_manejador


if __name__ == "__main__":
    gestor = SistemaGestor.get_instance()
    sensor = Sensor()
    sensor.activar(gestor)

    #Pruebas
    for i in range(12):
        sensor.enviar_dato((i*5, i*i))

    gestor.set_umbral(30)
    print("Datos obtenidos", gestor.datos)
    gestor.procesar()
    print("Estadisticos: ", gestor.estadisticos)
    print("Sobrecrecimiento detectado en los ultimos 30 seg (el primero): ", gestor.sobrecrecimiento)
    print("Temperaturas que superan el umbral: ", gestor.supera_umbral)