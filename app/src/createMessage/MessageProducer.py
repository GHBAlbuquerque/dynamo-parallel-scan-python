from abc import abstractmethod, ABC

class MessageProducer(ABC):
    @abstractmethod
    def send_event(self, message):
        pass