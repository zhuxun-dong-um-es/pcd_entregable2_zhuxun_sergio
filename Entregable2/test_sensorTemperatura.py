import pytest
from sensorTemperatura import *

def test_Singleton():
    gestor1 = SistemaGestor.get_instance()
    gestor2 = SistemaGestor.get_instance()

    assert((gestor1 == gestor2))

def test_DataLength():
    gestor = SistemaGestor.get_instance()
    sensor = Sensor()
    sensor.activar(gestor)

    for i in range(13):
        sensor._enviar_dato((i*5, i*i))

    assert((len(gestor._datos) == 12))

def test_registro_gestor():
    gestor = SistemaGestor.get_instance()
    sensor = Sensor()
    sensor.activar(gestor)
    
    assert((sensor._cliente == gestor))

if __name__ == "__main__":
    test_Singleton()
    test_DataLength()
    test_registro_gestor()