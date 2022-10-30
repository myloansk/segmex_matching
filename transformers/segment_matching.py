# Databricks notebook source
# MAGIC %run Repos/konstantinos-michail.mylonas@cchellenic.com/outlet_matching/commons/transform

# COMMAND ----------

# MAGIC %run Repos/konstantinos-michail.mylonas@cchellenic.com/outlet_matching/commons/extract

# COMMAND ----------


spark.conf.set(
  "fs.azure.account.key.cchbcaadls2prod.blob.core.windows.net",
  "J0OGUd0Ac+N8TJbptBLmnQ0okp4yqIpaynmHjBkIYnsJYIiC9mUBL6GrH+QQNSWewS3/7i7CSHckZaWvH3HIog==")
import yaml


consfig_file_path = '/dbfs/mnt/datalake/development/mylonas/outlet_matching_config_v2.yml'

config = yaml.safe_load(open(consfig_file_path))


# Init a InternalSrcConfig  dataclass object that holds configuration for external(adapter layer)
internalSrcConfig = InternalSrcConfig(**config['layer']['adaption']['internal'])

# Init a ExternalSrcConfig  dataclass object that holds configuration for external(adapter layer)
externalSrcConfig = ExternalSrcConfig(**config['layer']['adaption']['external'])

# Init a TrgConfig  dataclass object that holds configuration for application layer
trgConfig = TrgConfig(**config['layer']['application'])                         

extractData = ExtractData(internalSrcConfig, externalSrcConfig)


# COMMAND ----------

import logging
from datetime import datetime
from typing import Dict, List, NamedTuple

import pandas as pd
cc = 'at' 
  

class TrgConfig(NamedTuple):
  output_column_list:List[str]
  output_column_aliases:List[str]
  extenal_column_list:List[str]
  external_column_aliases:List[str]
  join_column_list:List[str]
  join_column_aliases:List[str]  
  drop_column_list:List[str]
  
  def __create_map_of_output_columns_aliases__(self)->Dict[str, str]:
    return dict(zip(self.output_column_list, self.output_column_aliases))

  def __create_map_of_external_columns_aliases__(self)->Dict[str, str]:
    return dict(zip(self.extenal_column_list, self.external_column_aliases))

  def __create_map_of_join_columns_aliases__(self)->Dict[str, str]:
     return dict(zip(self.join_column_list, self.join_column_aliases))  
 
 
class OutletMatching():
  def __init__(self, extractData:ExtractData, transformerContext:TransformerContext,
               trgArgs: TrgConfig, country_code: str = cc)->None:
    
    self._cc = cc
    self._trgArgs = trgArgs
    self._transformerContext = transformerContext
    self._extractData = extractData
    
    # Empty pyspark.DataFrames 
    self._internalDf:DataFrame = None
    self._externalDf:DataFrame = None
    self._poiListingDf:DataFrame = None
    
    self._tempDf:DataFrame = None
    self._outputDf:DataFrame = None
    
  def extract(self):
    self._extractData.get_external_data()
    self._externalDf = self._extractData._externalDf
    
    self._extractData.get_internal_data()
    self._internalDf = self._extractData._internalDf
    
    #TODO get poiListingDf from 
    self._poiListingDf = self._extractData.get_auxiliary_data()

  def transform(self):
    self._transformerContext.set_internal_df(self._internalDf)
    self._transformerContext.set_external_df(self._externalDf)
    self._tempDf = self._transformerContext.letsGo()
    
  def load(self)->Optional[DataFrame]:
    
    mapOfExternalColumns = self._trgArgs.__create_map_of_external_columns_aliases__()
    
    mapOfJoinColumns = self._trgArgs.__create_map_of_join_columns_aliases__()
    
    mapOfFinalColumns = self._trgArgs.__create_map_of_output_columns_aliases__()
    
    self._outputDf = (self._poiListingDf.select([f.col(colName).alias(aliasName) for colName, aliasName in mapOfExternalColumns.items()])
                        .join( self._tempDf, on='OUTLET_ID_EXTERNAL', how='left')
                        .select([f.col(colName).alias(aliasName) for colName, aliasName in mapOfJoinColumns.items()])
            )
    
    self._outputDf =  self._outputDf.withColumn("ADDRESS_EXTERNAL_ALL", f.coalesce(f.col("UNMATCHED_EXTERNAL_ADDRESS"),f.col("ADDRESS_EXTERNAL")))
    self._outputDf =  self._outputDf.withColumn("NAME_EXTERNAL_ALL", f.coalesce(f.col("UNMATCHED_EXTERNAL_NAME"),f.col("OUTLET_NAME_EXTERNAL")))
    self._outputDf =  self._outputDf.withColumn("flag",f.lit(None))
    self._outputDf =  self._outputDf.withColumn("flag", f.when((f.col("OUTLET_NAME_SIMILARITY")> 85) & (f.col("ADDRESS_TEXT_SIMILARITY") > 70), f.lit("High")).otherwise(f.col("flag")))
    self._outputDf =  self._outputDf.withColumn("flag", f.when((f.col("OUTLET_NAME_SIMILARITY")< 85) | (f.col("ADDRESS_TEXT_SIMILARITY") < 70), f.lit("Low")).otherwise(f.col("flag")))

    self._outputDf =  self._outputDf.drop(*self._trgArgs.drop_column_list)
    self._outputDf =  self._outputDf.select([f.col(colName).alias(aliasName) for colName, aliasName in mapOfFinalColumns.items()])
    return self._outputDf
    #if(should_persist):
    #   self._outputDf.toPandas().to_csv(outputFilepath + 'at_segmex_expected_output_pardt={partition_date}.csv'.format(partition_date=datetime.now().strftime("%Y%m%d_%H%M%S")),encoding="utf-16", index = False)
    #return self._outputDf
    
      

  

# COMMAND ----------

transformerConfig = TransformerConfig(['LONGITUDE','LATITUDE'],'OUTLET_ID')
fuzzyMatcherContext = FuzzyMatcherContext()
stringMatcherContext = StringContext()
transformerContext = TransformerContext(stringMatcherContext, fuzzyMatcherContext, transformerConfig)

# COMMAND ----------

finalColumnMap = {
                  'OUTLET_ID_EXTERNAL':'OUTLET_ID_EXTERNAL',
                  'CUSTOMER':'OUTLET_ID_EXTERNAL',
                  'PRICE_RANGE_INDICATOR':'PRICE_RANGE_INDICATOR',
                  'NAME_1':'NAME_1',
                  'NAME_2':'NAME_2',
                  "NAME_EXTERNAL_ALL":"NAME_EXTERNAL",
                  'ADDRESS_EXTERNAL_ALL': "ADDRESS_EXTERNAL",                        
                  'ADDRESS':'ADDRESS',
                  'DISTANCE_METERS':'DISTANCE_METERS',
                  'REVIEWS_COUNT':'REVIEWS_COUNT',
                  'STARS':'STARS',
                  'OUTLET_NAME_SIMILARITY':'OUTLET_NAME_SIMILARITY',
                  'ADDRESS_TEXT_SIMILARITY':'ADDRESS_TEXT_SIMILARITY',
                  'ADDRESS_NUM_SIMILARITY':'ADDRESS_TEXT_SIMILARITY',
                  'POSTAL_CODE_SIMILARITY':'POSTAL_CODE_SIMILARITY',
                  'CITY_TEXT_SIMILARITY':'CITY_TEXT_SIMILARITY',
                  'POSTAL_CODE_SIMILARITY':'CITY_TEXT_SIMILARITY',
                  "SENTIMENT":'SENTIMENT',  
                  "MATCH_RULE":'MATCH_RULE'
                  }




dropColumnLst = [
                 "ADDRESS_EXTERNAL",
                 "UNMATCHED_EXTERNAL_ADDRESS",
                 "UNMATCHED_EXTERNAL_NAME",
                 "OUTLET_NAME_EXTERNAL"
                ]


externalColummnMap = {
                      'poi_id':'OUTLET_ID_EXTERNAL',
                      'price_range_indicator':'price_range_indicator',
                      "location_text":"UNMATCHED_EXTERNAL_ADDRESS",
                      "name":"UNMATCHED_EXTERNAL_NAME",
                      "reviews_count":"REVIEWS_COUNT",
                      "stars":"STARS",
                      "sentiment":"SENTIMENT"
                     }

joinSelectColumnMap = {
                      'OUTLET_ID_EXTERNAL':'OUTLET_ID_EXTERNAL',	
                      'OUTLET_ID_INTERNAL':'CUSTOMER',		
                      'price_range_indicator':'PRICE_RANGE_INDICATOR',		
                      'OUTLET_NAME_1_INTERNAL':'NAME_1',
                      'OUTLET_NAME_2_INTERNAL':'NAME_2',	
                      "OUTLET_NAME_SIMILARITY":"OUTLET_NAME_SIMILARITY",
                      'OUTLET_NAME_EXTERNAL':'OUTLET_NAME_EXTERNAL',
                      "ADDRESS_TEXT_SIMILARITY":'ADDRESS_TEXT_SIMILARITY',
                      "ADDRESS_NUM_SIMILARITY":'ADDRESS_NUM_SIMILARITY',
                      "POSTAL_CODE_SIMILARITY":'POSTAL_CODE_SIMILARITY',
                      "CITY_TEXT_SIMILARITY":'CITY_TEXT_SIMILARITY',
                      "POSTAL_CODE_SIMILARITY":'POSTAL_CODE_SIMILARITY',
                      "UNMATCHED_EXTERNAL_NAME":'UNMATCHED_EXTERNAL_NAME',
                      'ADDRESS_INTERNAL':'ADDRESS',
                      'ADDRESS_EXTERNAL':'ADDRESS_EXTERNAL',
                      "UNMATCHED_EXTERNAL_ADDRESS":"UNMATCHED_EXTERNAL_ADDRESS",
                      'DISTANCE_METERS':'DISTANCE_METERS',
                      "REVIEWS_COUNT":'REVIEWS_COUNT',
                      "STARS":'STARS',
                      "SENTIMENT":'SENTIMENT',
                      "MATCH_RULE":'MATCH_RULE'}

# COMMAND ----------

#trgConfig = TrgConfig(list(finalColumnMap.keys()),list(finalColumnMap.values()),list(externalColummnMap.keys()), list(externalColummnMap.values()),list(joinSelectColumnMap.keys()),list(joinSelectColumnMap.values()),dropColumnLst)
trgConfig = TrgConfig(**config['layer']['application']) 
outletMatching = OutletMatching(extractData, transformerContext,trgConfig)
outletMatching.extract()

# COMMAND ----------

outletMatching.transform()

# COMMAND ----------

df = outletMatching.load()

# COMMAND ----------

df.display()

# COMMAND ----------

outletMatching._tempDf.display()