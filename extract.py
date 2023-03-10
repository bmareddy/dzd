# For the sake of simplicity all classes are organized in the single file

import os
import pandas as pd

class Extract():
    """
    Contains the properties and methods related to 
    extracting and serializing the data from flat files
    """
    _COLLECTIONS_HEADER = ["sampid", "hid", "isolate", "date_collected"]
    _PHENOTYPE_HEADER = [
        "hid",
        "isolate",
        "received",
        "organism",
        "source",
        "test",
        "antibiotic",
        "value",
        "antibiotic_interpretaion",
        "method"
    ]

    def __init__(self, source, path):
        """
        Inputs:
        * source: `collections` or `phenotype`
        * path: relative path to the file

        Returns:
        Instantiated object
        """
        self.source = source
        self.path = path
        
        # TODO: validate that self.source is with `collections` or `phenotype`

    def _read_data(self):
        """
        Reads data into a pandas dataframe
        and sets the column names to predefined list
        """
        source_path = os.path.join(os.getcwd(),self.path)
        # TODO: validate that file exists at source path
        # TODO: validate that file at source path is a csv

        df_source = pd.read_csv(source_path)
        if self.source == 'collections':
            # TODO: validate that # of columns is same len(_COLLECTIONS_HEADER)
            df_source.columns = self._COLLECTIONS_HEADER

        if self.source == 'phenotype':
            # TODO: validate that # of columns is same len(_COLLECTIONS_HEADER)
            df_source.columns = self._PHENOTYPE_HEADER
        
        return df_source
    
    def get_source_data(self):
        return self._read_data()
