from abc import ABC, abstractmethod
import numpy as np
from functools import reduce

"""
Notas: 6/5/2024
    En la versión actual, el método "procesar" de la clase SistemaGestor devuelve los valores que cumplan cierta condición en vez de modificar
    los parámetros correspondientes con el fin de llevar a cabo las pruebas de forma más sencilla (por ejemplo, si detecta una temperatura que supera
    el umbral dado, el parámetro self.supera_umbral pasaría a True(o la tupla de valores correspondientes), pero en la versión actual devuelve 
    las temperaturas que superan el umbral en vez de realizar la modificación).

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
            
            elif op == "umbral":
                resultado = op2.realizar_operacion(op, self.datos, self.umbral)
                print(resultado)

            else:
                resultado = op1.realizar_operacion(op, self.datos)
                print(resultado)
            
    

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
    def estrategia(self):
        pass

class MeanSV(Strategy):
    def estrategia(self, d, l):
        n = len(l)
        mean = round(reduce(lambda x, y: x+y, l) / n, 4)
        median = round(list(map(lambda x: x[(n+1)//2 - 1] if n%2 == 1 else ((x[n//2 - 1] + x[(n//2 - 1)+1])/2), [l]))[0], 4)
        sv = round(np.sqrt(sum(map(lambda x: (x-mean)**2, l))/(n-1)), 4) if (n-1) != 0 else 0
            
        d["media"] = mean
        d["mediana"] = median
        d["Desviacion Tipica"] = sv

class MaxMin(Strategy):
    def estrategia(self, d, l):
        maximo = reduce(lambda x,y: x if x>y else y, l)
        minimo = reduce(lambda x,y: x if x<y else y, l)

        d["max"] = maximo
        d["min"] = minimo

class Quantile(Strategy):
    def estrategia(self, d, l):
        n = len(l)
        l_ordenado = sorted(l)
        median = list(map(lambda x: x[(n+1)//2 - 1] if n%2 == 1 else ((x[n//2 - 1] + x[(n//2 - 1)+1])/2), [l_ordenado]))[0]
        q25 = list(map(lambda x: x[(n+1)//4 - 1], [l_ordenado]))[0]
        q75 = list(map(lambda x: x[(3*(n+1))//4 - 1], [l_ordenado]))[0]

        d["mediana"] = median
        d["Q1"] = q25
        d["Q3"] = q75

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
    def realizar_operacion(self, op, l, umbral):
        if op == "umbral":
            overheat = list(filter(lambda x: x[1] > umbral, l))

            if len(overheat) == 0:
                return False
            
            return overheat
        
        elif self.successor:
            self.successor.realizar_operacion(op)
    
    def set_manejador(self, nuevo_manejador):
        self.successor = nuevo_manejador

class Sobrecrecimiento(Manejador):
    def realizar_operacion(self, op, l):
        if op == "sobrecrecimiento":
            if len(l) <= 1:
                return False
            
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
                        return [l30s[i], l30s[j]]
                    
                    j = j + 1

                i = i + 1
                j = i + 1

            return False

        elif self.successor:
            self.successor.realizar_operacion(op)
    
    def set_manejador(self, nuevo_manejador):
        self.successor = nuevo_manejador


if __name__ == "__main__":
    gestor = SistemaGestor.get_instance()
    sensor = Sensor()
    sensor.activar(gestor)

    #Pruebas
    for i in range(12):
        sensor.enviar_dato((i, i*2 + 10))

    gestor.set_umbral(30)
    print(gestor.datos)
    gestor.procesar()