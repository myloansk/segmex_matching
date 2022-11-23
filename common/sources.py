from __future__ import annotations
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from typing import Dict, List, NamedTuple,Optional

import pandas as pd 

class Source(ABC):

    _sourceConfig:NamedTuple

    @abstractmethod
    def readData(self)->DataFrame:pass

    @abstractmethod
    def prepareData(self)->DataFrame:pass

class PoiData(Source):

    _sourceConfig:_externalSrcConfig

    def readData(self)->DataFrame:
        df_from_each_file = (pd.read_csv(f, low_memory=False) for f in [self._externalSrcConfig.poi_filepath[self._cc]])
        poiListingPd   = pd.concat(df_from_each_file, ignore_index=True)

        # Convert to spark dataframe
        return spark.createDataFrame(poiListingPd)

    def prepareData(self) -> DataFrame:
        return super().prepareData()


