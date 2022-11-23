from __future__ import annotations
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from typing import Dict, List, NamedTuple,Optional

import pandas as pd 

class Source(ABC):

    _sourceConfig:NamedTuple
    _sparkDf:DataFrame = None

    @abstractmethod
    def readData(self)->DataFrame:pass

    @abstractmethod
    def prepareData(self)->DataFrame:pass

class PoiData(Source):

    _sourceConfig:_externalSrcConfig
    _sparkDf:DataFrame

    def readData(self)->DataFrame:
        df_from_each_file = (pd.read_csv(f, low_memory=False) for f in [self._externalSrcConfig.poi_filepath[self._cc]])
        poiListingPd   = pd.concat(df_from_each_file, ignore_index=True)

        # Convert to spark dataframe
        self._sparkDf = spark.createDataFrame(poiListingPd)
        return self
    
    def prepareAuxiliaryData(self)->DataFrame:
        # Filter for at least one review in the last 6 months
        exprSumCounts = lambda months : pysum( f.coalesce(f.col(f'reviews_count_m{i}').cast('integer'), f.lit(0)) for i in range(1, months+1))
        self._sparkDf = self._sparkDf.filter(exprSumCounts(6)>0)

        return self._sparkDf

    def prepareData(self)->DataFrame:
        # Filter for at least one review in the last 6 months
        exprSumCounts = lambda months : pysum( f.coalesce(f.col(f'reviews_count_m{i}').cast('integer'), f.lit(0)) for i in range(1, months+1))
        poiListingDf = poiListingDf.filter(exprSumCounts(6)>0)

        self.set_auxiliary_data(poiListingDf)
        # Filter for valid coordinates
        
        poiListingDf = poiListingDf.filter(self._externalSrcConfig.__create_filtering_conditions__())                                                                            
        poiListingDf = poiListingDf.withColumn('POSTAL_CODE',f.lit(None).cast(t.StringType()))
        poiListingDf = poiListingDf.withColumn('CITY',f.coalesce(f.col('location_city'), f.col('location_state')))

        # Finalize table
        mapOfColumnAliases = self._externalSrcConfig.__create_map_of_selected_column_aliases__()
        return (poiListingDf.select([f.col(colName).alias(aliasName) for colName, aliasName in mapOfColumnAliases.items()]))

    def getData(self)->DataFrame:
        self.readData()
        self.prepareAuxiliaryData()
        self.prepareData()



