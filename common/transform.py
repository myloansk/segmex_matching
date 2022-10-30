# Databricks notebook source
# MAGIC %pip install unidecode thefuzz geopandas rtree geopy pyyaml

# COMMAND ----------

# MAGIC %run ./string_processor

# COMMAND ----------

# MAGIC %run ./fuzzy_matcher

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t
import geopandas as gpd
import pandas as pd
import numpy as np
from builtins import sum as pysum
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from typing import Dict, List, NamedTuple,Optional
from functools import reduce
from geopy import distance

class TransformerConfig(NamedTuple):
  COORDINATES_COLUMN_LIST:List[str]
  PRIMARY_KEY:str
  


class TransformerContext:
    def __init__(self, stringProcessor:StringContext,
                 fuzzyMatcher: FuzzyMatcherContext, transformerConfig:TransformerConfig)->None:
      self._stringProcessorContext = stringProcessor
      self._fuzzyMatcherContext = fuzzyMatcherContext

      self._transformerConfig = transformerConfig

      self._internalDf:DataFrame = None
      self._externalDf:DataFrame = None
    
    #@internalDf.setter
    def set_internal_df(self, internalDf:DataFrame)->None:
      self._internalDf = internalDf
    
    @property  
    def get_internal_df(self)->DataFrame:
      return self._internalDf
    
    #@externalDf.setter
    def set_external_df(self, externalDf:DataFrame)->None:
      self._externalDf = externalDf
    
    @property
    def get_external_df(self)->DataFrame:
      return self._externalDf
    
    @staticmethod
    def geospatialJoin(internalDf:DataFrame, externalDf:DataFrame,
                   coordinatesColumnLst:List[str], pk:str, bufferSize:int=500)->Optional[DataFrame]:
  
        internalPd = internalDf.select(*[s + '_INTERNAL' for s in [pk] + coordinatesColumnLst ]).toPandas()
        externalPd = externalDf.select(*[s + '_EXTERNAL'   for s in [pk] +coordinatesColumnLst ]).toPandas()
        #externalPd = externalDf.select([f.col(s).alias(s + '_EXTERNAL') for s in [pk] +coordinatesColumnLst ]).toPandas()

        # Convert to geopandas
        internalPd[coordinatesColumnLst[0]] = internalPd[coordinatesColumnLst[0] + '_INTERNAL'].astype(np.float32) 
        internalPd[coordinatesColumnLst[1]] = internalPd[coordinatesColumnLst[1] + '_INTERNAL'].astype(np.float32)
        internalGpd = gpd.GeoDataFrame(internalPd, geometry=gpd.points_from_xy(internalPd.LONGITUDE, internalPd.LATITUDE), crs = 'epsg:4326')

        externalPd[coordinatesColumnLst[0]] = externalPd[coordinatesColumnLst[0]  + '_EXTERNAL'].astype(np.float32)
        #externalPd[coordinatesColumnLst[0]] = externalPd[coordinatesColumnLst[0]].astype(np.float32)
        #externalPd[coordinatesColumnLst[1]] = externalPd[coordinatesColumnLst[1] ].astype(np.float32)
        externalPd[coordinatesColumnLst[1]] = externalPd[coordinatesColumnLst[1]  + '_EXTERNAL'].astype(np.float32)
        
        externalGpd = gpd.GeoDataFrame(externalPd, geometry=gpd.points_from_xy(externalPd.LONGITUDE, externalPd.LATITUDE), crs = 'epsg:4326')

        internalGpd = internalGpd.to_crs('epsg:3035') # Convert to project that is in meters instead of degrees  
        internalGpd['geometry'] = internalGpd['geometry'].buffer(bufferSize) # It is in meters
        internalGpd = internalGpd.to_crs('epsg:4326') # Convert back to standard projection

        # Join
        spatialMatchGpd = gpd.sjoin(internalGpd, externalGpd, predicate='intersects', how='inner') # predicate='within' would also work here

        # Convert to spark
        spatialMatchDf = spark.createDataFrame(spatialMatchGpd[[pk + s for s in  ['_INTERNAL', '_EXTERNAL'] ]])
        #spatialMatchDf = spark.createDataFrame(spatialMatchGpd)
        return spatialMatchDf
   
    @staticmethod
    def createBaseDataFrame(spatialMatchDf:DataFrame, internalDf:DataFrame,  externalDf:DataFrame, pk:str='OUTLET_ID_INTERNAL')->DataFrame:
       #return  spatialMatchDf.join(internalDf, on='OUTLET_ID_INTERNAL', how='left').join(externalDf, on='OUTLET_ID', how='left')
        return  spatialMatchDf.join(internalDf, on='OUTLET_ID_INTERNAL', how='left').join(externalDf, on='OUTLET_ID_EXTERNAL', how='left')

    @staticmethod
    def applyRules(dFrame:DataFrame, ruleMap:Dict)->Optional[DataFrame]:

        dFrame = dFrame.withColumn('MATCH_RULE', f.array([]))
        dFrame = reduce(lambda df, k: dFrame.withColumn('MATCH_RULE', f.when(ruleMap[k], f.array_union(f.col('MATCH_RULE'), f.array(f.lit(k))) ).otherwise(f.col('MATCH_RULE'))), 
                        list(ruleMap.keys()), dFrame)


        dFrame = dFrame.withColumn('MATCH_RULE', f.when(f.size(f.col('MATCH_RULE'))>0, f.col('MATCH_RULE')).otherwise(f.lit(None)))
        dFrame = dFrame.withColumn('MATCHED', f.when(f.col('MATCH_RULE').isNotNull(), f.lit('YES')).otherwise(f.lit(None)))
        return dFrame
    
    @staticmethod
    def getUniqueMatch(inputDf:DataFrame):
        # Keep matches
        df2 = inputDf.filter(f.col('MATCHED')=='YES')

        # Sort by best match (from internal outlet perspective)
        sortExpr = [f.col('OUTLET_NAME_SIMILARITY').desc_nulls_last(), f.col('ADDRESS_NUM_SIMILARITY').desc_nulls_last(), f.col('DISTANCE_METERS').asc_nulls_last(),f.col('ADDRESS_TEXT_SIMILARITY').desc_nulls_last()]
        w = Window.partitionBy('OUTLET_ID_INTERNAL').orderBy(*sortExpr)

        df2 = (df2.withColumn('MATCH_RANK_INTERNAL', f.row_number().over(w))
                .filter(f.col('MATCH_RANK_INTERNAL')==1)
                .drop('MATCH_RANK_INTERNAL')
            )
        # Keep matches
        df2 = inputDf.filter(f.col('MATCHED')=='YES')

        # Sort by best match (from internal outlet perspective)
        sortExpr = [f.col('OUTLET_NAME_SIMILARITY').desc_nulls_last(), f.col('ADDRESS_NUM_SIMILARITY').desc_nulls_last(), f.col('DISTANCE_METERS').asc_nulls_last(),f.col('ADDRESS_TEXT_SIMILARITY').desc_nulls_last()]
        w = Window.partitionBy('OUTLET_ID_INTERNAL').orderBy(*sortExpr)

        df2 = (df2.withColumn('MATCH_RANK_INTERNAL', f.row_number().over(w))
                .filter(f.col('MATCH_RANK_INTERNAL')==1)
                .drop('MATCH_RANK_INTERNAL')
            )
        return df2
      
    # --- Distance between points ---
    @staticmethod
    @f.udf(t.DoubleType())
    def distance_km(lat1, long1, lat2, long2):
      coords_1 = (lat1, long1)
      coords_2 = (lat2, long2)
      return distance.distance(coords_1, coords_2).km

    def letsGo(self)->DataFrame:
      # Init objects to be used
      addPostalCode = AddPostalCode()
      #removeRepeatedInfo = RemoveRepeatedInfo()
      stringContext = StringContext()
      
      # Setting string operation( add postal code)
      stringContext._stringProcess = addPostalCode
      
      self._externalDf = stringContext.process_string_columns(self._externalDf, 'POSTAL_CODE',['ADDRESS'] )
     
      # Init objects to be used
      removeRepeatedInfo = RemoveRepeatedInfo()
      # Setting string operation( add postal code)
      stringContext._stringProcess = removeRepeatedInfo
    
      # Adding column with postal code to pyspark.DataFrame
      self._externalDf = stringContext.process_string_columns(self._externalDf, 'ADDRESS',['ADDRESS','COUNTRY','COUNTY','STATE','CITY'] )
    
      # Changing operation to remove postal code
      removePostalCode = RemovePostalCode()
      stringContext._stringProcess = removePostalCode
      
      self._externalDf = stringContext.process_string_columns(self._externalDf, 'ADDRESS',['ADDRESS'] )
    
      # Finalize table
      self._externalDf =  self._externalDf.select('OUTLET_ID', 'LONGITUDE', 'LATITUDE', 'OUTLET_NAME', 'CITY', 'ADDRESS', 'POSTAL_CODE')

      # Add suffix to column names
      self._externalDf =  self._externalDf.select(*[f.col(Col).alias(Col+'_EXTERNAL') for Col in  self._externalDf.columns])
      spatialMatchDf = TransformerContext.geospatialJoin(self._internalDf, self._externalDf, 
                                                         self._transformerConfig.COORDINATES_COLUMN_LIST,self._transformerConfig.PRIMARY_KEY)
  
      ##### Create main table from spatial match
      mainDf = TransformerContext.createBaseDataFrame(spatialMatchDf, self._internalDf,
                                                      self._externalDf,self._transformerConfig.PRIMARY_KEY)

      df = mainDf
      ##### Add extra comparison attributes
      cleanAddress = CleanAddress()
      stringContext._stringProcess = cleanAddress
      df = stringContext.process_string_columns(df, 'ADDRESS_TEXT_INTERNAL',['ADDRESS_INTERNAL'] )
      df = stringContext.process_string_columns(df, 'ADDRESS_TEXT_EXTERNAL',['ADDRESS_EXTERNAL'] )
      
      df = stringContext.process_string_columns(df, 'CITY_TEXT_INTERNAL',['CITY_INTERNAL'] )
      df = stringContext.process_string_columns(df, 'CITY_TEXT_EXTERNAL',['CITY_EXTERNAL'] )
     
      getAddressNumber = GetAddressNumber()
      stringContext._stringProcess = getAddressNumber
      
      df = stringContext.process_string_columns(df, 'ADDRESS_NUM_INTERNAL',['ADDRESS_INTERNAL'] )
      df = df.withColumn('ADDRESS_NUM_INTERNAL', f.when(f.col('ADDRESS_NUM_INTERNAL').isNull(), f.lit(None)).otherwise(f.col('ADDRESS_NUM_INTERNAL')))
      
      df = stringContext.process_string_columns(df, 'ADDRESS_NUM_EXTERNAL',['ADDRESS_EXTERNAL'] )
      df = df.withColumn('ADDRESS_NUM_EXTERNAL', f.when(f.col('ADDRESS_NUM_EXTERNAL').isNull(), f.lit(None)).otherwise(f.col('ADDRESS_NUM_EXTERNAL')))
      
      fuzzyMatchTokenSort = FuzzyMatchTokeSort()
      fuzzyMatcherContext = FuzzyMatcherContext()
    
      #Setting fuzzy matching similarity algorithm to fuzzy_match_token_sort
      fuzzyMatcherContext._fuzzyMatcher = fuzzyMatchTokenSort
    
      #Calculating fuzzy matching similarity of outlet name using fuzzy_match_token_sort
      df = fuzzyMatcherContext.calculate_similarity(df, 'OUTLET_NAME_1_SIMILARITY',['OUTLET_NAME_1_INTERNAL', 'OUTLET_NAME_EXTERNAL'] )
      df = fuzzyMatcherContext.calculate_similarity(df, 'OUTLET_NAME_2_SIMILARITY',['OUTLET_NAME_2_INTERNAL', 'OUTLET_NAME_EXTERNAL'] )
 
      df = df.withColumn('OUTLET_NAME_SIMILARITY', f.greatest(f.col('OUTLET_NAME_1_SIMILARITY'), f.col('OUTLET_NAME_2_SIMILARITY')) )
  
      #Setting fuzzy matching similarity algorithm to fuzzy_match_token_set
      fuzzyMatcherContext._fuzzyMatcher = FuzzyMatchTokenSet
    
      #Calculating fuzzy matching similarity of outlet address using fuzzy_match_token_sest
      df = fuzzyMatcherContext.calculate_similarity(df, 'CITY_TEXT_SIMILARITY',['CITY_TEXT_INTERNAL', 'CITY_TEXT_EXTERNAL'] )
      df = fuzzyMatcherContext.calculate_similarity(df, 'ADDRESS_TEXT_SIMILARITY',['ADDRESS_TEXT_INTERNAL', 'ADDRESS_TEXT_EXTERNAL'] )
      
      doesItContainNum = DoesItContainNum()
      stringContext._stringProcess = doesItContainNum
   
      df = stringContext.process_string_columns(df, 'ADDRESS_NUM_SIMILARITY',['ADDRESS_NUM_INTERNAL', 'ADDRESS_NUM_EXTERNAL'] )
      
      arePostalCodeTheSame = ArePostalCodeTheSame()
      stringContext._stringProcess = arePostalCodeTheSame
      df = stringContext.process_string_columns(df, 'POSTAL_CODE_SIMILARITY',['POSTAL_CODE_INTERNAL', 'POSTAL_CODE_EXTERNAL'] )
    
      df = df.withColumn('DISTANCE_METERS', (TransformerContext.distance_km('LATITUDE_INTERNAL', 'LONGITUDE_INTERNAL', 'LATITUDE_EXTERNAL', 'LONGITUDE_EXTERNAL')*1000).cast('integer') )

      # Checkpoint
      #df.coalesce(24).write.mode('overwrite').format("parquet").saveAsTable('harry.temp_df3001') 
      #df = spark.sql('select * from harry.temp_df3001') 

      ruleDict = {'rule1': (f.col('OUTLET_NAME_SIMILARITY')>85)&((f.col('ADDRESS_NUM_SIMILARITY')==100)),
                'rule2': (f.col('ADDRESS_TEXT_SIMILARITY')>70)&(f.col('ADDRESS_NUM_SIMILARITY')==100)&(f.col('OUTLET_NAME_SIMILARITY')>60),
                'rule3': (f.col('ADDRESS_TEXT_SIMILARITY')>90)&(f.col('OUTLET_NAME_SIMILARITY')>85),
                'rule4': (f.col('ADDRESS_TEXT_SIMILARITY')>85)&(f.col('ADDRESS_NUM_SIMILARITY')==100),
                'rule5': (f.col('ADDRESS_TEXT_SIMILARITY')>80)&(f.col('ADDRESS_NUM_SIMILARITY')==100)&(f.col('OUTLET_NAME_SIMILARITY')>50),
               }
      ##### Apply Rules
      df = TransformerContext.applyRules(df, ruleDict)
      ##### Remove multiple matches
      df = TransformerContext.getUniqueMatch(df)
      return df
