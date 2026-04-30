from abc import ABC, abstractmethod

class Pipeline(ABC):
    """ Class to build out data pipelines
    """
    
    @abstractmethod
    def extract(self):
        """ Method for extracting data out of a source system
        """
        
    @abstractmethod
    def transform(self):
        """ Method for transforming data pulled out of source system
        """
    
    @abstractmethod
    def load(self):
        """ Method for loading data into some storage area
        """
    
    def run(self):
        """ Runs the full pipeline
        """
        self.extract()
        self.transform()
        self.load()