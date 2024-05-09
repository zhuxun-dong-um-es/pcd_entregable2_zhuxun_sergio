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
        sensor.enviar_dato((i*5, i*i))

    assert((len(gestor.datos) == 12))


if __name__ == "__main__":
    test_Singleton()
    test_DataLength()