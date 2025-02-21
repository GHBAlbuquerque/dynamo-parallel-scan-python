from abc import abstractmethod, ABC

class MessageProducer(ABC):
    @abstractmethod
    def produce(self, message):
        pass