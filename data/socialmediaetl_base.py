import abc

class SocialMediaETL:

    def __init__(self):
        pass

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass

    @abc.abstractmethod
    def execute(self):
        """Retrieve data from the input source and return an object."""
        return 

