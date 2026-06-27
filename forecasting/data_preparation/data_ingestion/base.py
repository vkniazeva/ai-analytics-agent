from abc import ABC, abstractmethod
import pandas as pd

class BaseSource(ABC):

    @abstractmethod
    def fetch(self) -> pd.DataFrame:
        """loads the data and returns a DataFrame """
        pass