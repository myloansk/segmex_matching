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
        
        self._sparkD = self._sparkD..filter(self._externalSrcConfig.__create_filtering_conditions__())                                                                            
        self._sparkD = self._sparkD.withColumn('POSTAL_CODE',f.lit(None).cast(t.StringType()))
        self._sparkD = self._sparkD.withColumn('CITY',f.coalesce(f.col('location_city'), f.col('location_state')))

        # Finalize table
        mapOfColumnAliases = self._externalSrcConfig.__create_map_of_selected_column_aliases__()
        return (self._sparkD.select([f.col(colName).alias(aliasName) for colName, aliasName in mapOfColumnAliases.items()]))



class CCHBCData(Source):
    
    _sourceConfig: _internalSrcConfig
    _sparkDf: DataFrame = None

    listOfInputs:Optional[DataFrame]
    

    def readData(self)->None:
        # Load customer MD
        customerDf = spark.read.option("header", "true").option("sep", "|").csv(self._internalSrcConfig.filepaths['customer'].format( CAPS_CC = self._cc.upper()))
        customerDf = customerDf.fillna("NAN", subset=['_BIC_CSUP_CUST'])
        
        # Apply standard filters
        customerDf = customerDf.filter(self._internalSrcConfig.__create_filtering_condition__())
        # Load customer MD PI
        customerPiDf = spark.read.option("header", "true").option("sep", "|").csv(self._internalSrcConfig.filepaths['customer_pi'].format( CAPS_CC = self._cc.upper()))

        # Load country name
        salesOrgDf = spark.read.option("header", "true").csv(self._internalSrcConfig.filepaths['sales_org'])
        salesOrgDf = salesOrgDf.select('SALESORG', 'COUNTRYTXT').distinct()

        self.listOfInputs = [customerDf, customerPiDf, salesOrgDf]



    def prepareData(self) -> DataFrame:
        
        customerDf, customerPiDf, salesOrgDf = self.listOfInputs
        
        self._internalDf = (customerDf.join(customerPiDf, on='CUSTOMER', how='left')
                                .join(salesOrgDf, on='SALESORG', how='left')
                                .select(customerDf['CUSTOMER'].alias('OUTLET_ID'),
                                        f.coalesce(customerPiDf['NAME'], customerPiDf['CUSTOMER_DESC']).alias('OUTLET_NAME_1'),
                                        f.coalesce(customerPiDf['NAME2'], customerPiDf['CUSTOMER_DESC']).alias('OUTLET_NAME_2'),
                                        salesOrgDf['COUNTRYTXT'].alias('COUNTRY'),
                                        customerPiDf['CITY'].alias('CITY'),
                                        customerPiDf['STREET'].alias('ADDRESS'),
                                        customerPiDf['POSTAL_CD'].alias('POSTAL_CODE'),
                                        customerDf['LONGITUDE'].cast('double').alias('LONGITUDE'),
                                        customerDf['LATITUDE'].cast('double').alias('LATITUDE'),
                                    )
        )

        self._sparkDf = self._sparkDf.select(*[f.col(Col).alias(Col+'_INTERNAL') for Col in self._internalSrcConfig.selected_column_list])
        return self._sparkDf 




