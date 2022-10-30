# Databricks notebook source
'''
TODO:

'''

# COMMAND ----------

# MAGIC %pip install geopandas rtree thefuzz[speedup] unidecode geopy

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window
import re
from functools import reduce
from builtins import sum as pysum
import pandas as pd
import geopandas as gpd
from shapely import wkt
import numpy as np
import itertools

from thefuzz import fuzz
from typing import Dict, List, Optional
from pyspark.sql import DataFrame
import re
from unidecode import unidecode
from geopy import distance

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")


# COMMAND ----------

country = 'at'                       

# COMMAND ----------

# MAGIC %md ##### Adapter Layer

# COMMAND ----------

# Adapter Layer

spark.conf.set(
  "fs.azure.account.key.cchbcaadls2prod.blob.core.windows.net",
  "J0OGUd0Ac+N8TJbptBLmnQ0okp4yqIpaynmHjBkIYnsJYIiC9mUBL6GrH+QQNSWewS3/7i7CSHckZaWvH3HIog==")

internalSelectedColumnLst = ['OUTLET_ID', 'LONGITUDE', 'LATITUDE', 'OUTLET_NAME_1', 'OUTLET_NAME_2', 'CITY', 'ADDRESS', 'POSTAL_CODE']
internalFilePaths = {
                     'customer':"abfss://cchbc@cchbcaadls2prod.dfs.core.windows.net/sources/ca/cch_bw/md/raw/CA_0CUS03_Customer/{CAPS_CC}_0CUS03/",
                     'customerPi':"wasbs://cchbc-pi@cchbcaadls2prod.blob.core.windows.net/sources/ca/cch_bw/md/raw/CACUS03PI_Customer_PI/{CAPS_CC}_CUS03PI/",
                     'saleOrg':"abfss://cchbc@cchbcaadls2prod.dfs.core.windows.net/sources/ca/cch_bw/md/raw/CA_SALORG_SalesOrg/CA_SALORG/CA_SALORG.csv"
                    }

globalAccountGroupList = [str(a) for a in range(9000, 9099 + 1)]
globalCustHier2List = ['KA', 'NK']
globalOrderBlockList = ['CW', 'F', 'I', '#', 'C', 'TS', 'PR', 'NR', 'VF','NAN']
globalExlcudeChannelList = ['036', '099', 'ZZZ']
globalExlcudeSubChannelList = ['610', '815', '816']

internalFilteringCondLst = ['(ACCNT_GRP IN ({globalAccountGroupListPch}))',
                            '(CUST_HIE02 IN ({globalCustHier2ListPch}))',
                            '(_BIC_CTRADE_CH NOT IN ({globalExlcudeChannelListPch}))',
                            '(_BIC_CSUB_TRAD NOT IN ({globalExlcudeSubChannelListPch}))',
                            '(CSUB_TRAD_DESC !="DO NOT USE")',
                            "(LONGITUDE != 0)", '(_BIC_CSUP_CUST IN ({globalOrderBlockListPch}))',
                            "(LATITUDE != 0)", '(LONGITUDE IS NOT NULL)', '(LATITUDE IS NOT NULL)']
internalFilteringCond = """{expr}""".format(expr = ' and '.join(internalFilteringCondLst))


internalFilteringCond = internalFilteringCond.format(globalAccountGroupListPch = ','.join([ele for ele in globalAccountGroupList]), 
                                            globalCustHier2ListPch = ','.join(["\"" + ele + "\"" for ele in globalCustHier2List]),
                                            globalOrderBlockListPch = ','.join(["\"" + ele + "\"" if ele!='' else ele for ele in globalOrderBlockList]),
                                            globalExlcudeChannelListPch = ','.join([ "\"" + ele + "\""  for ele in globalExlcudeChannelList]),
                                            globalExlcudeSubChannelListPch = ','.join(["\"" + ele + "\"" for ele in globalExlcudeSubChannelList]))


def getInternalData(internalSelectedColumnLst:List[str],internalFilePaths:Dict[str, str],internalFilteringCond:str, cc:str=country)->DataFrame:
  # Load customer MD
  customerDf = spark.read.option("header", "true").option("sep", "|").csv(internalFilePaths['customer'].format( CAPS_CC = cc.upper()))
  customerDf = customerDf.fillna("NAN", subset=['_BIC_CSUP_CUST'])
  # Apply standard filters
  customerDf = customerDf.where(f.expr(internalFilteringCond))
 

  # Load customer MD PI
  customerPiDf = spark.read.option("header", "true").option("sep", "|").csv(internalFilePaths['customerPi'].format( CAPS_CC = cc.upper()))

  # Load country name
  salesOrgDf = spark.read.option("header", "true").csv(internalFilePaths['saleOrg'])
  salesOrgDf = salesOrgDf.select('SALESORG', 'COUNTRYTXT').distinct()


  # Create columns
  internalDf = (customerDf.join(customerPiDf, on='CUSTOMER', how='left')
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
  
  # Add suffix to column names
  internalDf = internalDf.select(*[f.col(Col).alias(Col+'_INTERNAL') for Col in internalSelectedColumnLst])
  return internalDf 

   
  
selectedColumnMap = {
                      'poi_id':'OUTLET_ID',
                      'name':'OUTLET_NAME',
                      'location_country':'COUNTRY',
                      'CITY':'CITY',
                      'location_county':'COUNTY',
                      'location_state':'STATE',
                      'location_text':'ADDRESS',
                      'POSTAL_CODE':'POSTAL_CODE',
                      'location_lon':'LONGITUDE',
                      'location_lat':'LATITUDE', 
                    }

# File paths
poiListingPaths = {'ro': ['/dbfs/mnt/datalake_gen2/cchbc/sources/ca/carto/raw/ro/sentiment/dataappeal_pointsofinterest_placessentiment_rou_latlon_v1_quart.csv'],
                   'gr': ['/dbfs/mnt/datalake_gen2/cchbc/sources/ca/carto/raw/gr/sentiment/dataappeal_pointsofinterest_placessentiment_grc_latlon_v1_quart.csv'],
                   'cz': ['/dbfs/mnt/datalake_gen2/cchbc/sources/ca/carto/raw/cz/sentiment/dataappeal_pointsofinterest_placessentiment_cze_latlon_v1_quart.csv'],
                   'pl': ['/dbfs/mnt/datalake_gen2/cchbc/sources/ca/carto/raw/pl/sentiment/dataappeal_pointsofinterest_placessentiment_pol_latlon_v1_quart.csv'],
                   'at': ['/dbfs/mnt/datalake_gen2/cchbc/sources/ca/carto/raw/at/sentiment/dataappeal_pointsofinterest_placessentiment_aut_latlon_v1_quart_v2.csv'],
                  }
                                                                            
#externalFilteringCond = (f.col('location_lon').cast('double')!=0) & (f.col('location_lat').cast('double')!=0) & (f.col('location_lon').isNotNull()) & (f.col('location_lat').isNotNull())                                      
                                                                            
externalFilteringCond = """{expr}""".format(expr = ' and '.join(["(CAST(location_lon AS DOUBLE)!=0)","(CAST(location_lat AS DOUBLE)!=0)","(location_lon IS NOT NULL)", "(location_lat IS NOT NULL)"]))

def getExternalData(selectedColumnMap:Dict[str, str],srcPathMap:Dict[str, str],externalFilteringCond:str, cc:str=country)->DataFrame:
  # Load external outlet data
  df_from_each_file = (pd.read_csv(f, low_memory=False) for f in poiListingPaths[country])
  poiListingPd   = pd.concat(df_from_each_file, ignore_index=True)

  # Convert to spark dataframe
  poiListingDf = spark.createDataFrame(poiListingPd)

  # Filter for at least one review in the last 6 months
  exprSumCounts = lambda months : pysum( f.coalesce(f.col(f'reviews_count_m{i}').cast('integer'), f.lit(0)) for i in range(1, months+1))
  poiListingDf = poiListingDf.filter(exprSumCounts(6)>0)

  # Filter for valid coordinates
  #poiListingDf = poiListingDf.filter((f.col('location_lon').cast('double')!=0) & (f.col('location_lat').cast('double')!=0) & (f.col('location_lon').isNotNull()) & (f.col('location_lat').isNotNull()))
  print(externalFilteringCond)
  poiListingDf = poiListingDf.filter(externalFilteringCond)                                                                            
  poiListingDf = poiListingDf.withColumn('POSTAL_CODE',f.lit(None).cast(t.StringType()))
  poiListingDf = poiListingDf.withColumn('CITY',f.coalesce(f.col('location_city'), f.col('location_state')))

  # Finalize table
  externalDf = (poiListingDf.select([f.col(colName).alias(aliasName + '_EXTERNAL') for colName, aliasName in selectedColumnMap.items()]))
             
  return externalDf




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




def getOutputDataFrame(result:DataFrame, poiListingDf:DataFrame,
                       externalColummnMap:Dict[str,str], finalColumnMap:Dict[str, str], 
                       outputFilePath:str, should_persist:bool=True)->Optional[DataFrame]:
    priceDf = (poiListingDf.select([f.col(colName).alias(aliasName) for colName, aliasName in externalColummnMap.items()])
                        .join(resultDf, on='OUTLET_ID_EXTERNAL', how='left')
                        .select([f.col(colName).alias(aliasName) for colName, aliasName in externalColummnMap.items()])
            )


    priceDf = priceDf.withColumn("ADDRESS_EXTERNAL_ALL", f.coalesce(f.col("UNMATCHED_EXTERNAL_ADDRESS"),f.col("ADDRESS_EXTERNAL")))
    priceDf = priceDf.withColumn("NAME_EXTERNAL_ALL", f.coalesce(f.col("UNMATCHED_EXTERNAL_NAME"),f.col("OUTLET_NAME_EXTERNAL")))
    priceDf = priceDf.withColumn("flag",f.lit(None))
    priceDf = priceDf.withColumn("flag", f.when((f.col("OUTLET_NAME_SIMILARITY")> 85) & (f.col("ADDRESS_TEXT_SIMILARITY") > 70), f.lit("High")).otherwise(f.col("flag")))
    priceDf = priceDf.withColumn("flag", f.when((f.col("OUTLET_NAME_SIMILARITY")< 85) | (f.col("ADDRESS_TEXT_SIMILARITY") < 70), f.lit("Low")).otherwise(f.col("flag")))

    priceDf = priceDf.drop(dropColumnLst)
    priceDf = priceDf.select([f.col(colName).alias(aliasName) for colName, aliasName in finalColumnMap.items()])
    if(should_persist):
      priceDf.toPandas().to_csv(outputFilepath + 'at_segmex_expected_output_pardt={partition_date}.csv'.format(partition_date=datetime.now().strftime("%Y%m%d_%H%M%S")),encoding="utf-16", index = False)
    return priceDf

# COMMAND ----------

internalFuncDf = getInternalData(internalSelectedColumnLst,internalFilePaths, internalFilteringCond)
externalFuncDf = getExternalData(selectedColumnMap,poiListingPaths,externalFilteringCond, poiListingPaths)

# COMMAND ----------

def are_dfs_equal(df1, df2): 
    return (df1.schema == df2.schema) and (df1.collect() == df2.collect())
  
isInternalDataFrameTheSame = are_dfs_equal(internalFuncDf, internalDf)

print(f'The dataset are {isInternalTheSame}')

isExterrnalTheSame = are_dfs_equal(externalFuncDf, externalDf)

# COMMAND ----------

externalFuncDf.display()

# COMMAND ----------

externalDf.display()

# COMMAND ----------

internalDf = spark.read.format("csv").load("dbfs:/mnt/datalake/development/mylonas/segment_matching/internal_data.csv", header = True)\
                  .select(f.col("OUTLET_ID_INTERNAL"),
                          f.col("LONGITUDE_INTERNAL").cast("double"),
                          f.col("LATITUDE_INTERNAL").cast("double"),
                          f.col("OUTLET_NAME_1_INTERNAL"),
                          f.col("OUTLET_NAME_2_INTERNAL"),
                          f.col("CITY_INTERNAL"),
                          f.col("ADDRESS_INTERNAL"),
                          f.col("POSTAL_CODE_INTERNAL"),
                         )
externalDf = spark.read.format("csv").load("dbfs:/mnt/datalake/development/mylonas/segment_matching/external_data.csv",  header = True)\
                  .select(f.col("OUTLET_ID_EXTERNAL"),
                          f.col("LONGITUDE_EXTERNAL").cast("double"),
                          f.col("LATITUDE_EXTERNAL").cast("double"),
                          f.col("OUTLET_NAME_EXTERNAL"),
                          f.col("CITY_EXTERNAL"),
                          f.col("ADDRESS_EXTERNAL"),
                          f.col("POSTAL_CODE_EXTERNAL")
                                           )

# COMMAND ----------

# MAGIC %md ##### Application Layer

# COMMAND ----------

# Application Layer

coordinatesColumnLst = ['LONGITUDE','LATITUDE']
pk = 'OUTLET_ID'

def geospatialJoin(internalDf:DataFrame, externalDf:DataFrame,
                   coordinatesColumnLst:List[str] = coordinatesColumnLst, pk:str = pk, bufferSize:int=500)->Optional[DataFrame]:
  
  internalPd = internalDf.select(*[s + '_INTERNAL' for s in [pk] +coordinatesColumnLst ]).toPandas()
  externalPd = externalDf.select([s + '_EXTERNAL' for s in [pk] +coordinatesColumnLst ]).toPandas()

  # Convert to geopandas
  internalPd[coordinatesColumnLst[0]] = internalPd[coordinatesColumnLst[0] + '_INTERNAL'].astype(np.float32) 
  internalPd[coordinatesColumnLst[1]] = internalPd[coordinatesColumnLst[1] + '_INTERNAL'].astype(np.float32)
  internalGpd = gpd.GeoDataFrame(internalPd, geometry=gpd.points_from_xy(internalPd.LONGITUDE, internalPd.LATITUDE), crs = 'epsg:4326')

  externalPd[coordinatesColumnLst[0]] = externalPd[coordinatesColumnLst[0]  + '_EXTERNAL'].astype(np.float32) 
  externalPd[coordinatesColumnLst[1]] = externalPd[coordinatesColumnLst[1]  + '_EXTERNAL'].astype(np.float32)
  externalGpd = gpd.GeoDataFrame(externalPd, geometry=gpd.points_from_xy(externalPd.LONGITUDE, externalPd.LATITUDE), crs = 'epsg:4326')

  internalGpd = internalGpd.to_crs('epsg:3035') # Convert to project that is in meters instead of degrees  
  internalGpd['geometry'] = internalGpd['geometry'].buffer(bufferSize) # It is in meters
  internalGpd = internalGpd.to_crs('epsg:4326') # Convert back to standard projection

  # Join
  spatialMatchGpd = gpd.sjoin(internalGpd, externalGpd, predicate='intersects', how='inner') # predicate='within' would also work here

  # Convert to spark
  spatialMatchDf = spark.createDataFrame(spatialMatchGpd[[pk + s for s in  ['_INTERNAL', '_EXTERNAL'] ]])
  return spatialMatchDf

def createBaseDataFrame(spatialMatchDf:DataFrame, internalDf:DataFrame,  externalDf:DataFrame, pk:str='OUTLET_ID_INTERNAL')->DataFrame:
  return  spatialMatchDf.join(internalDf, on='OUTLET_ID_INTERNAL', how='left').join(externalDf, on='OUTLET_ID_EXTERNAL', how='left')


def applyRules(dFrame:DataFrame, ruleMap:Dict)->Optional[DataFrame]:
  
  dFrame = dFrame.withColumn('MATCH_RULE', f.array([]))
  dFrame = reduce(lambda df, k: dFrame.withColumn('MATCH_RULE', f.when(ruleMap[k], f.array_union(f.col('MATCH_RULE'), f.array(f.lit(k))) ).otherwise(f.col('MATCH_RULE'))), 
                  list(ruleMap.keys()), dFrame)


  dFrame = dFrame.withColumn('MATCH_RULE', f.when(f.size(f.col('MATCH_RULE'))>0, f.col('MATCH_RULE')).otherwise(f.lit(None)))
  dFrame = dFrame.withColumn('MATCHED', f.when(f.col('MATCH_RULE').isNotNull(), f.lit('YES')).otherwise(f.lit(None)))
  return dFrame

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

# --- Clean values ---

# Add postal code to separate columns
@f.udf(t.StringType())
def add_postal_code(txt):
  if txt:
    result = re.search("\d{3}\s{0,1}\d{2}", txt) # Find postal code
    if result: 
      return str(result.group(0).replace(' ', ''))
    
    result = re.search("\d{4}", txt) # Find 4-digit postal code
    if result: 
      return str(result.group(0).replace(' ', ''))
    
  else: 
    return None



# Remove postal code from address
@f.udf(t.StringType())
def remove_postal_code(txt):
  if txt:
    txt = re.sub("\d{3}\s\d{2}", "",  txt) # Find postal code
#     txt = re.sub("\d{4}", "",  txt) # Find postal code
    txt = re.sub(' +', ' ', txt) # Remove multiple spaces
    return txt
 
  else: 
    return None



# Remove extra words from address
@f.udf(t.StringType())
def remove_extra_words_in_address(txt, country, county, state, city):
  if txt:
    if country: txt = re.compile(rf"\b{country}\b", re.IGNORECASE).sub("", txt) # Remove country from address
    if county: txt = re.compile(rf"\b{county}\b", re.IGNORECASE).sub("", txt) # Remove county from address    
    if state: txt = re.compile(rf"\b{state}\b", re.IGNORECASE).sub("", txt) # Remove state from address
    if city: txt = re.compile(rf"\b{city}\b", re.IGNORECASE).sub("", txt) # Remove city from address
    words = txt.split(); txt = " ".join(sorted(set(words), key=words.index)) # Remove duplicate words
    txt = re.sub('\,+', ' ', txt) # Remove commas
    txt = re.sub('\s+', ' ', txt) # Remove multiple spaces
    txt = txt.strip() # Remove leading or trailing whitespace
    
    if txt:
      return txt
    else:
      return None
  else: 
    return None
  
# --- Clean address & city text ---

@f.udf(t.StringType())
def clean_address_text(txt):
  if txt:
    txt = re.sub('\W+',' ', txt) # Replace special characters with space
    txt = re.sub("[!,*)@#%(&$_?.^']", '', txt) # Remove special characters (a bit redundant)
    txt = re.compile('\s*[0-9]+(?:\s|$)').sub(" ", txt) # Remove numbers  
    txt = txt.replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue').replace('ß', 'ss')
    txt = unidecode(txt) # Convert to unicode characters
    txt = txt.lower() # Convert to lowercase
    
    if country == 'at':
      txt = re.sub('\s+str\s+','strasse ', txt)
      txt = re.sub('\s+strasse\s+','strasse ', txt)
      txt = re.sub('\s+g\s+','gasse ', txt)
      txt = re.sub('\s+gasse\s+','gasse ', txt)
      txt = re.sub('pl\s+','platz ', txt)
      txt = re.sub('\s+platz\s+','platz ', txt)
      
    words = txt.split(); txt = " ".join(sorted(set(words), key=words.index)) # Remove duplicate words
    txt = re.sub('\s+', ' ', txt) # Remove multiple spaces
    txt = txt.strip() # Remove leading or trailing whitespace
    return txt
  else:
    return None
  
@f.udf(t.ArrayType(t.IntegerType()))
def find_address_num(txt):
  if txt:
    txt = re.sub('\W+',' ', txt) # Replace special characters with space
    return [int(s) for s in re.findall(r"\s*[0-9]+(?:\s|$)", txt)] # Put numbers to list
  else:
    return None 
  
@f.udf(t.IntegerType())
def fuzzy_match_token_sort(txt1, txt2):
  
  def clean_text(txt):
    txt = re.sub("[!,*)@#%(&$_?.^']", '', txt)
    txt = re.sub(' +', ' ', txt) # Remove multiple spaces
    txt = unidecode(txt)
    txt = txt.lower()
    return txt
  
  if txt1 and txt2:
    txt1 = clean_text(txt1)
    txt2 = clean_text(txt2)
    similarity_score = int(fuzz.token_sort_ratio(txt1, txt2))
  else:
    similarity_score = None
    
  return similarity_score


# --- Fuzzy token set similarity ---

@f.udf(t.IntegerType())
def fuzzy_match_token_set(txt1, txt2):
  
  if txt1 and txt2:
    
    best_similarity_score = 0
    min_len = min(len(txt1.split()), len(txt2.split()))
    
    # Iterate over combination of words inside the address
    for txtcomb1 in itertools.combinations(txt1.split(), min_len):
      for txtcomb2 in itertools.combinations(txt2.split(), min_len):
        print(txtcomb1, txtcomb2)
        subtxt1 = " ".join(sorted(set(txtcomb1), key=txtcomb1.index))
        subtxt2 = " ".join(sorted(set(txtcomb2), key=txtcomb2.index))
        print(subtxt1, subtxt2, int(fuzz.token_set_ratio(subtxt1, subtxt2)))
        similarity_score = int(fuzz.token_set_ratio(subtxt1, subtxt2))
        if similarity_score>best_similarity_score: 
          best_similarity_score = similarity_score

  else:
    best_similarity_score = None
    
  return best_similarity_score


# --- Address number is same ---

@f.udf(t.IntegerType())
def contains_num(lst1, lst2):
  if lst1 and lst2 and len(set(lst1).intersection(set(lst2))) > 0:
    return 100
  elif lst1 and lst2:
    return 0
  else:
    return None
  
# --- Postal number is same ---

@f.udf(t.IntegerType())
def same_postal_code(po1, po2):
  if po1 and po2 and po1==po2:
    return 100
  elif po1 and po2:
    return 0
  else:
    return None
  

# --- Distance between points ---

@f.udf(t.DoubleType())
def distance_km(lat1, long1, lat2, long2):
  coords_1 = (lat1, long1)
  coords_2 = (lat2, long2)
  return distance.distance(coords_1, coords_2).km
  

# COMMAND ----------

# MAGIC %md ##### Main

# COMMAND ----------

def main():
  # Getting the data
  internalDf = getInternalData(internalSelectedColumnLst,internalFilePaths, internalFilteringCond)
  externalDf = getExternalData(selectedColumnMap,poiListingPaths,externalFilteringCond, poiListingPaths)

  #Starting executing application layer
  
  externalDf = externalDf.withColumn('ADDRESS', remove_postal_code(f.col('ADDRESS')))

  externalDf = externalDf.withColumn('ADDRESS', remove_extra_words_in_address('ADDRESS', 'COUNTRY', 'COUNTY', 'STATE', 'CITY'))
  
  # Finalize table
  externalDf = externalDf.select('OUTLET_ID', 'LONGITUDE', 'LATITUDE', 'OUTLET_NAME', 'CITY', 'ADDRESS', 'POSTAL_CODE')

  # Add suffix to column names
  externalDf = externalDf.select(*[f.col(Col).alias(Col+'_EXTERNAL') for Col in externalDf.columns])
  ##### Geospatial Match
  spatialMatchDf = geospatialJoin(internalDf,externalDf)
  
  ##### Create main table from spatial match
  mainDf = createBaseDataFrame(spatialMatchDf, internalDf,externalDf)
  
  df = mainDf
  ##### Add extra comparison attributes
  df = df.withColumn('ADDRESS_TEXT_INTERNAL', clean_address_text(f.col('ADDRESS_INTERNAL')))
  df = df.withColumn('ADDRESS_TEXT_EXTERNAL', clean_address_text(f.col('ADDRESS_EXTERNAL')))

  df = df.withColumn('CITY_TEXT_INTERNAL', clean_address_text(f.col('CITY_INTERNAL')))
  df = df.withColumn('CITY_TEXT_EXTERNAL', clean_address_text(f.col('CITY_EXTERNAL')))
  
  # --- Find address number ---
  df = df.withColumn('ADDRESS_NUM_INTERNAL', find_address_num(f.col('ADDRESS_INTERNAL')))
  df = df.withColumn('ADDRESS_NUM_INTERNAL', f.when(f.col('ADDRESS_NUM_INTERNAL')[0].isNull(), f.lit(None)).otherwise(f.col('ADDRESS_NUM_INTERNAL')))

  df = df.withColumn('ADDRESS_NUM_EXTERNAL', find_address_num(f.col('ADDRESS_EXTERNAL')))
  df = df.withColumn('ADDRESS_NUM_EXTERNAL', f.when(f.col('ADDRESS_NUM_EXTERNAL')[0].isNull(), f.lit(None)).otherwise(f.col('ADDRESS_NUM_EXTERNAL')))
  
  # --- Fuzzy token sort similarity ---
  df = df.withColumn('OUTLET_NAME_1_SIMILARITY', fuzzy_match_token_sort('OUTLET_NAME_1_INTERNAL', 'OUTLET_NAME_EXTERNAL') )
  df = df.withColumn('OUTLET_NAME_2_SIMILARITY', fuzzy_match_token_sort('OUTLET_NAME_2_INTERNAL', 'OUTLET_NAME_EXTERNAL') )
  df = df.withColumn('OUTLET_NAME_SIMILARITY', f.greatest(f.col('OUTLET_NAME_1_SIMILARITY'), f.col('OUTLET_NAME_2_SIMILARITY')) )
  
  df = df.withColumn('CITY_TEXT_SIMILARITY', fuzzy_match_token_set('CITY_TEXT_INTERNAL', 'CITY_TEXT_EXTERNAL') )
  df = df.withColumn('ADDRESS_TEXT_SIMILARITY', fuzzy_match_token_set('ADDRESS_TEXT_INTERNAL', 'ADDRESS_TEXT_EXTERNAL') )
  
  df = df.withColumn('ADDRESS_NUM_SIMILARITY', contains_num('ADDRESS_NUM_INTERNAL', 'ADDRESS_NUM_EXTERNAL') )
  
  df = df.withColumn('POSTAL_CODE_SIMILARITY', same_postal_code('POSTAL_CODE_INTERNAL', 'POSTAL_CODE_EXTERNAL') )
  df = df.withColumn('DISTANCE_METERS', (distance_km('LATITUDE_INTERNAL', 'LONGITUDE_INTERNAL', 'LATITUDE_EXTERNAL', 'LONGITUDE_EXTERNAL')*1000).cast('integer') )
  
  # Checkpoint
  df.coalesce(24).write.mode('overwrite').format("parquet").saveAsTable('harry.temp_df2') 
  df = spark.sql('select * from harry.temp_df2') 
  
  ruleDict = {
             'rule1': (f.col('OUTLET_NAME_SIMILARITY')>85)  & ((f.col('ADDRESS_NUM_SIMILARITY')==100)),
             'rule2': (f.col('ADDRESS_TEXT_SIMILARITY')>70) & (f.col('ADDRESS_NUM_SIMILARITY')==100) & (f.col('OUTLET_NAME_SIMILARITY')>60),
             'rule3': (f.col('ADDRESS_TEXT_SIMILARITY')>90) & (f.col('OUTLET_NAME_SIMILARITY')>85),
             'rule4': (f.col('ADDRESS_TEXT_SIMILARITY')>85) & (f.col('ADDRESS_NUM_SIMILARITY')==100),
             'rule5': (f.col('ADDRESS_TEXT_SIMILARITY')>80) & (f.col('ADDRESS_NUM_SIMILARITY')==100) & (f.col('OUTLET_NAME_SIMILARITY')>50),
           }
  ##### Apply Rules
  df = applyRules(df, ruleDict)
  ##### Remove multiple matches
  df2 = getUniqueMatch(df)
  return df2
  



  


# COMMAND ----------

# Run main
justDf = main()

justDf.display()

# COMMAND ----------

# MAGIC %md ##### Save results

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
                      "UNMATCHED_EXTERNAL_ADDRESS",
                      'DISTANCE_METERS':'DISTANCE_METERS',
                      "REVIEWS_COUNT":'REVIEWS_COUNT',
                      "STARS":'STARS',
                      "SENTIMENT":'SENTIMENT',
                      "MATCH_RULE":'MATCH_RULE'}




def getOutputDataFrame(result:DataFrame, poiListingDf:DataFrame,
                       externalColummnMap:Dict[str,str], finalColumnMap:Dict[str, str], 
                       outputFilePath:str, should_persist:bool=True)->Optional[DataFrame]:
    priceDf = (poiListingDf.select([f.col(colName).alias(aliasName) for colName, aliasName in externalColummnMap.items()])
                        .join(resultDf, on='OUTLET_ID_EXTERNAL', how='left')
                        .select([f.col(colName).alias(aliasName) for colName, aliasName in externalColummnMap.items()])
            )


    priceDf = priceDf.withColumn("ADDRESS_EXTERNAL_ALL", f.coalesce(f.col("UNMATCHED_EXTERNAL_ADDRESS"),f.col("ADDRESS_EXTERNAL")))
    priceDf = priceDf.withColumn("NAME_EXTERNAL_ALL", f.coalesce(f.col("UNMATCHED_EXTERNAL_NAME"),f.col("OUTLET_NAME_EXTERNAL")))
    priceDf = priceDf.withColumn("flag",f.lit(None))
    priceDf = priceDf.withColumn("flag", f.when((f.col("OUTLET_NAME_SIMILARITY")> 85) & (f.col("ADDRESS_TEXT_SIMILARITY") > 70), f.lit("High")).otherwise(f.col("flag")))
    priceDf = priceDf.withColumn("flag", f.when((f.col("OUTLET_NAME_SIMILARITY")< 85) | (f.col("ADDRESS_TEXT_SIMILARITY") < 70), f.lit("Low")).otherwise(f.col("flag")))

    priceDf = priceDf.drop(dropColumnLst)
    priceDf = priceDf.select([f.col(colName).alias(aliasName) for colName, aliasName in finalColumnMap.items()])
    if(should_persist):
      priceDf.toPandas().to_csv(outputFilepath + 'at_segmex_expected_output_pardt={partition_date}.csv'.format(partition_date=datetime.now().strftime("%Y%m%d_%H%M%S")),encoding="utf-16", index = False)
    return priceDf

# COMMAND ----------

priceDf = (poiListingDf.select(f.col('poi_id').alias('OUTLET_ID_EXTERNAL'),
                                f.col('price_range_indicator').alias('price_range_indicator'),
                                f.col("location_text").alias("UNMATCHED_EXTERNAL_ADDRESS"),
                                f.col("name").alias("UNMATCHED_EXTERNAL_NAME"),
                                f.col("reviews_count").alias("REVIEWS_COUNT"),
                                f.col("stars").alias("STARS"),
                               f.col("sentiment").alias("SENTIMENT"))
                      .join(resultDf, on='OUTLET_ID_EXTERNAL', how='left')
                      .select(f.col('OUTLET_ID_EXTERNAL').alias('OUTLET_ID_EXTERNAL'),	
                              f.col('OUTLET_ID_INTERNAL').alias('CUSTOMER'),		
                              f.col('price_range_indicator').alias('PRICE_RANGE_INDICATOR'),		
                              f.col('OUTLET_NAME_1_INTERNAL').alias('NAME_1'),
                              f.col('OUTLET_NAME_2_INTERNAL').alias('NAME_2'),	
                              f.col("OUTLET_NAME_SIMILARITY"),
                              f.col('OUTLET_NAME_EXTERNAL').alias('OUTLET_NAME_EXTERNAL'),
                              f.col("ADDRESS_TEXT_SIMILARITY"),
                              f.col("ADDRESS_NUM_SIMILARITY"),
                              f.col("POSTAL_CODE_SIMILARITY"),
                              f.col("CITY_TEXT_SIMILARITY"),
                              f.col("POSTAL_CODE_SIMILARITY"),
                              f.col("UNMATCHED_EXTERNAL_NAME"),
                              f.col('ADDRESS_INTERNAL').alias('ADDRESS'),
                              f.col('ADDRESS_EXTERNAL').alias('ADDRESS_EXTERNAL'),
                              f.col("UNMATCHED_EXTERNAL_ADDRESS"),
                              f.col('DISTANCE_METERS').alias('DISTANCE_METERS'),
                              f.col("REVIEWS_COUNT"),
                              f.col("STARS"),
                              f.col("SENTIMENT"),
                              f.col("MATCH_RULE")
                      )
         )


priceDf = priceDf.withColumn("ADDRESS_EXTERNAL_ALL", f.coalesce(f.col("UNMATCHED_EXTERNAL_ADDRESS"),f.col("ADDRESS_EXTERNAL")))
priceDf = priceDf.withColumn("NAME_EXTERNAL_ALL", f.coalesce(f.col("UNMATCHED_EXTERNAL_NAME"),f.col("OUTLET_NAME_EXTERNAL")))
priceDf = priceDf.withColumn("flag",f.lit(None))
priceDf = priceDf.withColumn("flag", f.when((f.col("OUTLET_NAME_SIMILARITY")> 85) & (f.col("ADDRESS_TEXT_SIMILARITY") > 70), f.lit("High")).otherwise(f.col("flag")))
priceDf = priceDf.withColumn("flag", f.when((f.col("OUTLET_NAME_SIMILARITY")< 85) | (f.col("ADDRESS_TEXT_SIMILARITY") < 70), f.lit("Low")).otherwise(f.col("flag")))

priceDf = priceDf.drop("ADDRESS_EXTERNAL","UNMATCHED_EXTERNAL_ADDRESS","UNMATCHED_EXTERNAL_NAME","OUTLET_NAME_EXTERNAL")


priceDf = priceDf.select(f.col('OUTLET_ID_EXTERNAL'),
                         f.col('CUSTOMER'),
                         f.col('PRICE_RANGE_INDICATOR'),
                         f.col('NAME_1'),
                         f.col('NAME_2'),
                         f.col("NAME_EXTERNAL_ALL").alias("NAME_EXTERNAL"),
                         f.col('ADDRESS_EXTERNAL_ALL').alias("ADDRESS_EXTERNAL"),                        
                         f.col('ADDRESS'),
                         f.col('DISTANCE_METERS'),
                         f.col('REVIEWS_COUNT'),
                         f.col('STARS'),
                         f.col('OUTLET_NAME_SIMILARITY'),
                         f.col('ADDRESS_TEXT_SIMILARITY'),
                         f.col('ADDRESS_NUM_SIMILARITY'),
                         f.col('POSTAL_CODE_SIMILARITY'),
                         f.col('CITY_TEXT_SIMILARITY'),
                         f.col('POSTAL_CODE_SIMILARITY'),
                         f.col("SENTIMENT"),  
                         f.col("MATCH_RULE"))

display(priceDf)

# COMMAND ----------

priceDf.count()

# COMMAND ----------



priceDf.toPandas().to_csv(outputFilepath + 'at_segmex_expected_output_pardt={partition_date}.csv'.format(partition_date=datetime.now().strftime("%Y%m%d_%H%M%S")),encoding="utf-16", index = False)

# COMMAND ----------

