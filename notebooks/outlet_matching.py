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
import re
from unidecode import unidecode
from geopy import distance

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")


# COMMAND ----------

country = 'at'



# COMMAND ----------

# MAGIC %md ##### Load Data (from raw)

# COMMAND ----------

# --- Customer MD Data ---

globalAccountGroupList = [a for a in range(9000, 9099 + 1)]
globalCustHier2List = ['KA', 'NK']
globalOrderBlockList = ['CW', 'F', 'I', '#', 'C', 'TS', 'PR', 'NR', 'VF']
globalExlcudeChannelList = ['036', '099', 'ZZZ']
globalExlcudeSubChannelList = ['610', '815', '816']

spark.conf.set(
  "fs.azure.account.key.cchbcaadls2prod.blob.core.windows.net",
  "J0OGUd0Ac+N8TJbptBLmnQ0okp4yqIpaynmHjBkIYnsJYIiC9mUBL6GrH+QQNSWewS3/7i7CSHckZaWvH3HIog==")

# Load customer MD
customerDf = spark.read.option("header", "true").option("sep", "|").csv(f"abfss://cchbc@cchbcaadls2prod.dfs.core.windows.net/sources/ca/cch_bw/md/raw/CA_0CUS03_Customer/{country.upper()}_0CUS03/")

# Apply standard filters
customerDf = (customerDf.filter(f.col('ACCNT_GRP').isin(globalAccountGroupList))
                        .filter(f.col('CUST_HIE02').isin(globalCustHier2List))
                        .filter((f.col('_BIC_CSUP_CUST').isin(globalOrderBlockList)) | (f.col('_BIC_CSUP_CUST').isNull()))
                        .filter(~(f.col('_BIC_CTRADE_CH').isin(globalExlcudeChannelList)))
                        .filter(~(f.col('_BIC_CSUB_TRAD').isin(globalExlcudeSubChannelList)))
                        .filter(f.col('CSUB_TRAD_DESC')!='DO NOT USE')
                        .filter((f.col('LONGITUDE').cast('double')!=0) & (f.col('LATITUDE').cast('double')!=0) & (f.col('LONGITUDE').isNotNull()) & (f.col('LATITUDE').isNotNull()))
             )

# Load customer MD PI
customerPiDf = spark.read.option("header", "true").option("sep", "|").csv(f"wasbs://cchbc-pi@cchbcaadls2prod.blob.core.windows.net/sources/ca/cch_bw/md/raw/CACUS03PI_Customer_PI/{country.upper()}_CUS03PI/")

# Load country name
salesOrgDf = spark.read.option("header", "true").csv(f"abfss://cchbc@cchbcaadls2prod.dfs.core.windows.net/sources/ca/cch_bw/md/raw/CA_SALORG_SalesOrg/CA_SALORG/CA_SALORG.csv")
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
#                                 customerDf['_BIC_CTRADE_CH'].alias('_BIC_CTRADE_CH'),
#                                 customerDf['CTRADE_CH_DESC'].alias('CTRADE_CH_DESC'),
#                                 customerDf['_BIC_CSUB_TRAD'].alias('SUB_TRADE_CHANNEL'),
#                                 customerDf['CSUB_TRAD_DESC'].alias('SUB_TRADE_CHANNEL_DESC'),
                               )
             )

# Finalize table
internalDf = internalDf.select('OUTLET_ID', 'LONGITUDE', 'LATITUDE', 'OUTLET_NAME_1', 'OUTLET_NAME_2', 'CITY', 'ADDRESS', 'POSTAL_CODE')

# Add suffix to column names
internalDf = internalDf.select(*[f.col(Col).alias(Col+'_INTERNAL') for Col in internalDf.columns])


# COMMAND ----------

internalDf.toPandas().to_csv("/dbfs/mnt/datalake/development/mylonas/segment_matching/internal_data.csv", header = True, index = False)

# COMMAND ----------

# --- Data Appeal Data ---

# File paths
poiListingPaths = {'ro': ['/dbfs/mnt/datalake_gen2/cchbc/sources/ca/carto/raw/ro/sentiment/dataappeal_pointsofinterest_placessentiment_rou_latlon_v1_quart.csv'],
                   'gr': ['/dbfs/mnt/datalake_gen2/cchbc/sources/ca/carto/raw/gr/sentiment/dataappeal_pointsofinterest_placessentiment_grc_latlon_v1_quart.csv'],
                   'cz': ['/dbfs/mnt/datalake_gen2/cchbc/sources/ca/carto/raw/cz/sentiment/dataappeal_pointsofinterest_placessentiment_cze_latlon_v1_quart.csv'],
                   'pl': ['/dbfs/mnt/datalake_gen2/cchbc/sources/ca/carto/raw/pl/sentiment/dataappeal_pointsofinterest_placessentiment_pol_latlon_v1_quart.csv'],
                   'at': ['/dbfs/mnt/datalake_gen2/cchbc/sources/ca/carto/raw/at/sentiment/dataappeal_pointsofinterest_placessentiment_aut_latlon_v1_quart_v2.csv'],
                  }

# Load external outlet data
df_from_each_file = (pd.read_csv(f, low_memory=False) for f in poiListingPaths[country])
poiListingPd   = pd.concat(df_from_each_file, ignore_index=True)

# Convert to spark dataframe
poiListingDf = spark.createDataFrame(poiListingPd)

# Filter for at least one review in the last 6 months
exprSumCounts = lambda months : pysum( f.coalesce(f.col(f'reviews_count_m{i}').cast('integer'), f.lit(0)) for i in range(1, months+1))
poiListingDf = poiListingDf.filter(exprSumCounts(6)>0)

# Filter for valid coordinates
poiListingDf = poiListingDf.filter((f.col('location_lon').cast('double')!=0) & (f.col('location_lat').cast('double')!=0) & (f.col('location_lon').isNotNull()) & (f.col('location_lat').isNotNull()))

# Finalize table
externalDf = (poiListingDf.select(f.col('poi_id').alias('OUTLET_ID'),
                                  f.col('name').alias('OUTLET_NAME'),
                                  f.col('location_country').alias('COUNTRY'),
                                  f.coalesce(f.col('location_city'), f.col('location_state')).alias('CITY'),
                                  f.col('location_county').alias('COUNTY'),
                                  f.col('location_state').alias('STATE'),
                                  f.col('location_text').alias('ADDRESS'),
                                  f.lit(None).cast('string').alias('POSTAL_CODE'),
                                  f.col('location_lon').cast('double').alias('LONGITUDE'),
                                  f.col('location_lat').cast('double').alias('LATITUDE'),

                                 )
             )


# COMMAND ----------

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

externalDf = externalDf.withColumn('POSTAL_CODE', add_postal_code(f.col('ADDRESS')))

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

externalDf = externalDf.withColumn('ADDRESS', remove_postal_code(f.col('ADDRESS')))

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

externalDf = externalDf.withColumn('ADDRESS', remove_extra_words_in_address('ADDRESS', 'COUNTRY', 'COUNTY', 'STATE', 'CITY'))


# COMMAND ----------

externalDf.display()

# COMMAND ----------

# Finalize table
externalDf = externalDf.select('OUTLET_ID', 'LONGITUDE', 'LATITUDE', 'OUTLET_NAME', 'CITY', 'ADDRESS', 'POSTAL_CODE')

# Add suffix to column names
externalDf = externalDf.select(*[f.col(Col).alias(Col+'_EXTERNAL') for Col in externalDf.columns])


# COMMAND ----------

externalDf.toPandas().to_csv("/dbfs/mnt/datalake/development/mylonas/segment_matching/external_data.csv", header = True, index = False)

# COMMAND ----------

# MAGIC %md ##### Load Data (from coffee) TEMP

# COMMAND ----------

# # --- Load data ---

# root_path = 'dbfs:/mnt/datalake_gen2/cchbc/development/COSTA_COFFEE'

# # Load raw data
# inputDf = spark.read.option("header", "true").parquet(f'{root_path}/{country}/intermediate/outlet_base')

# # Filter for correct coordinates
# inputDf = inputDf.filter((f.col('LONGITUDE')!=0) & (f.col('LATITUDE')!=0) & (f.col('LONGITUDE').isNotNull()) & (f.col('LATITUDE').isNotNull()))


# # Split internal & external universe
# internalDf = inputDf.filter(f.col('SOURCE')=='customer_md').select('OUTLET_ID', 'LONGITUDE', 'LATITUDE', 'OUTLET_NAME', 'CITY', 'ADDRESS', 'POSTAL_CODE', 'COFFEE_CHANNEL')
# externalDf = inputDf.filter(f.col('SOURCE')=='data_appeal').select('OUTLET_ID', 'LONGITUDE', 'LATITUDE', 'OUTLET_NAME', 'CITY', 'ADDRESS', 'POSTAL_CODE', 'COFFEE_CHANNEL')

# # Add suffix to column names
# internalDf = internalDf.select(*[f.col(Col).alias(Col+'_INTERNAL') for Col in internalDf.columns])
# externalDf = externalDf.select(*[f.col(Col).alias(Col+'_EXTERNAL') for Col in externalDf.columns])


# COMMAND ----------

# MAGIC %md ##### Geospatial Match

# COMMAND ----------

# --- Create geospatial matching ---

# Convert to pandas
internalPd = internalDf.select('OUTLET_ID_INTERNAL', 'LONGITUDE_INTERNAL', 'LATITUDE_INTERNAL').toPandas()
externalPd = externalDf.select('OUTLET_ID_EXTERNAL', 'LONGITUDE_EXTERNAL', 'LATITUDE_EXTERNAL').toPandas()

# Convert to geopandas
internalPd['LONGITUDE'] = internalPd['LONGITUDE_INTERNAL'].astype(np.float32) 
internalPd['LATITUDE'] = internalPd['LATITUDE_INTERNAL'].astype(np.float32)
internalGpd = gpd.GeoDataFrame(internalPd, geometry=gpd.points_from_xy(internalPd.LONGITUDE, internalPd.LATITUDE), crs = 'epsg:4326')

externalPd['LONGITUDE'] = externalPd['LONGITUDE_EXTERNAL'].astype(np.float32) 
externalPd['LATITUDE'] = externalPd['LATITUDE_EXTERNAL'].astype(np.float32)
externalGpd = gpd.GeoDataFrame(externalPd, geometry=gpd.points_from_xy(externalPd.LONGITUDE, externalPd.LATITUDE), crs = 'epsg:4326')

# Add buffer
bufferSize = 500

internalGpd = internalGpd.to_crs('epsg:3035') # Convert to project that is in meters instead of degrees  
internalGpd['geometry'] = internalGpd['geometry'].buffer(bufferSize) # It is in meters
internalGpd = internalGpd.to_crs('epsg:4326') # Convert back to standard projection

# Join
spatialMatchGpd = gpd.sjoin(internalGpd, externalGpd, predicate='intersects', how='inner') # predicate='within' would also work here

# Convert to spark
spatialMatchDf = spark.createDataFrame(spatialMatchGpd[['OUTLET_ID_INTERNAL', 'OUTLET_ID_EXTERNAL']])


# COMMAND ----------

# MAGIC %md ##### Create main table from spatial match

# COMMAND ----------

# Create main table from spatial match
mainDf = spatialMatchDf.join(internalDf, on='OUTLET_ID_INTERNAL', how='left').join(externalDf, on='OUTLET_ID_EXTERNAL', how='left')


# COMMAND ----------

df = mainDf

# COMMAND ----------

# MAGIC %md ##### Add extra comparison attributes

# COMMAND ----------

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

df = df.withColumn('ADDRESS_TEXT_INTERNAL', clean_address_text(f.col('ADDRESS_INTERNAL')))
df = df.withColumn('ADDRESS_TEXT_EXTERNAL', clean_address_text(f.col('ADDRESS_EXTERNAL')))

df = df.withColumn('CITY_TEXT_INTERNAL', clean_address_text(f.col('CITY_INTERNAL')))
df = df.withColumn('CITY_TEXT_EXTERNAL', clean_address_text(f.col('CITY_EXTERNAL')))


# COMMAND ----------

# --- Find address number ---

@f.udf(t.ArrayType(t.IntegerType()))
def find_address_num(txt):
  if txt:
    txt = re.sub('\W+',' ', txt) # Replace special characters with space
    return [int(s) for s in re.findall(r"\s*[0-9]+(?:\s|$)", txt)] # Put numbers to list
  else:
    return None

df = df.withColumn('ADDRESS_NUM_INTERNAL', find_address_num(f.col('ADDRESS_INTERNAL')))
df = df.withColumn('ADDRESS_NUM_INTERNAL', f.when(f.col('ADDRESS_NUM_INTERNAL')[0].isNull(), f.lit(None)).otherwise(f.col('ADDRESS_NUM_INTERNAL')))

df = df.withColumn('ADDRESS_NUM_EXTERNAL', find_address_num(f.col('ADDRESS_EXTERNAL')))
df = df.withColumn('ADDRESS_NUM_EXTERNAL', f.when(f.col('ADDRESS_NUM_EXTERNAL')[0].isNull(), f.lit(None)).otherwise(f.col('ADDRESS_NUM_EXTERNAL')))


# COMMAND ----------

# --- Fuzzy token sort similarity ---

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
  

df = df.withColumn('OUTLET_NAME_1_SIMILARITY', fuzzy_match_token_sort('OUTLET_NAME_1_INTERNAL', 'OUTLET_NAME_EXTERNAL') )
df = df.withColumn('OUTLET_NAME_2_SIMILARITY', fuzzy_match_token_sort('OUTLET_NAME_2_INTERNAL', 'OUTLET_NAME_EXTERNAL') )
df = df.withColumn('OUTLET_NAME_SIMILARITY', f.greatest(f.col('OUTLET_NAME_1_SIMILARITY'), f.col('OUTLET_NAME_2_SIMILARITY')) )


# COMMAND ----------

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

df = df.withColumn('CITY_TEXT_SIMILARITY', fuzzy_match_token_set('CITY_TEXT_INTERNAL', 'CITY_TEXT_EXTERNAL') )
df = df.withColumn('ADDRESS_TEXT_SIMILARITY', fuzzy_match_token_set('ADDRESS_TEXT_INTERNAL', 'ADDRESS_TEXT_EXTERNAL') )


# COMMAND ----------

df.display()

# COMMAND ----------

# --- Address number is same ---

@f.udf(t.IntegerType())
def contains_num(lst1, lst2):
  if lst1 and lst2 and len(set(lst1).intersection(set(lst2))) > 0:
    return 100
  elif lst1 and lst2:
    return 0
  else:
    return None
  

df = df.withColumn('ADDRESS_NUM_SIMILARITY', contains_num('ADDRESS_NUM_INTERNAL', 'ADDRESS_NUM_EXTERNAL') )



# COMMAND ----------

# --- Postal number is same ---

@f.udf(t.IntegerType())
def same_postal_code(po1, po2):
  if po1 and po2 and po1==po2:
    return 100
  elif po1 and po2:
    return 0
  else:
    return None
  

df = df.withColumn('POSTAL_CODE_SIMILARITY', same_postal_code('POSTAL_CODE_INTERNAL', 'POSTAL_CODE_EXTERNAL') )



# COMMAND ----------

# --- Distance between points ---

@f.udf(t.DoubleType())
def distance_km(lat1, long1, lat2, long2):
  coords_1 = (lat1, long1)
  coords_2 = (lat2, long2)
  return distance.distance(coords_1, coords_2).km

df = df.withColumn('DISTANCE_METERS', (distance_km('LATITUDE_INTERNAL', 'LONGITUDE_INTERNAL', 'LATITUDE_EXTERNAL', 'LONGITUDE_EXTERNAL')*1000).cast('integer') )


# COMMAND ----------

# MAGIC %md ##### Apply Rules

# COMMAND ----------

# Checkpoint
df.coalesce(24).write.mode('overwrite').format("parquet").saveAsTable('harry.temp_df2') 
df = spark.sql('select * from harry.temp_df2') 


# COMMAND ----------

# Apply rules for matching

ruleDict = {'rule1': (f.col('OUTLET_NAME_SIMILARITY')>85)&((f.col('ADDRESS_NUM_SIMILARITY')==100)|(f.col("ADDRESS_NUM_SIMILARITY").isNull())),
            'rule2': (f.col('ADDRESS_TEXT_SIMILARITY')>70)&(f.col('ADDRESS_NUM_SIMILARITY')==100)&(f.col('OUTLET_NAME_SIMILARITY')>60),
            'rule3': (f.col('ADDRESS_TEXT_SIMILARITY')>90)&(f.col('OUTLET_NAME_SIMILARITY')>85),
            'rule4': (f.col('ADDRESS_TEXT_SIMILARITY')>85)&(f.col('ADDRESS_NUM_SIMILARITY')==100),
            'rule5': (f.col('ADDRESS_TEXT_SIMILARITY')>80)&(f.col('ADDRESS_NUM_SIMILARITY')==100)&(f.col('OUTLET_NAME_SIMILARITY')>40),
            'rule6': (f.col('ADDRESS_TEXT_SIMILARITY')>70)&(f.col('ADDRESS_NUM_SIMILARITY')==100)&(f.col('OUTLET_NAME_SIMILARITY')>30),
            'rule7': (f.col('OUTLET_NAME_SIMILARITY')>60)&((f.col('ADDRESS_NUM_SIMILARITY')==100)|(f.col("ADDRESS_NUM_SIMILARITY").isNull())),
            'rule8': (f.col('ADDRESS_TEXT_SIMILARITY')>60)&(f.col('ADDRESS_NUM_SIMILARITY')==100)&(f.col('OUTLET_NAME_SIMILARITY')>50),
           }


df = df.withColumn('MATCH_RULE', f.array([]))
df = reduce(lambda df, k: df.withColumn('MATCH_RULE', f.when(ruleDict[k], f.array_union(f.col('MATCH_RULE'), f.array(f.lit(k))) ).otherwise(f.col('MATCH_RULE'))), list(ruleDict.keys()), df)


df = df.withColumn('MATCH_RULE', f.when(f.size(f.col('MATCH_RULE'))>0, f.col('MATCH_RULE')).otherwise(f.lit(None)))
df = df.withColumn('MATCHED', f.when(f.col('MATCH_RULE').isNotNull(), f.lit('YES')).otherwise(f.lit(None)))


# COMMAND ----------

# MAGIC %md ##### Remove multiple matches

# COMMAND ----------

# Keep matches
df2 = df.filter(f.col('MATCHED')=='YES')

# Sort by best match (from internal outlet perspective)
sortExpr = [f.col('OUTLET_NAME_SIMILARITY').desc_nulls_last(), f.col('ADDRESS_NUM_SIMILARITY').desc_nulls_last(), f.col('DISTANCE_METERS').asc_nulls_last(),f.col('ADDRESS_TEXT_SIMILARITY').desc_nulls_last()]
w = Window.partitionBy('OUTLET_ID_INTERNAL').orderBy(*sortExpr)

df2 = (df2.withColumn('MATCH_RANK_INTERNAL', f.row_number().over(w))
          .filter(f.col('MATCH_RANK_INTERNAL')==1)
          .drop('MATCH_RANK_INTERNAL')
      )

# Sort by best match (from external outlet perspective)
sortExpr = [f.col('OUTLET_NAME_SIMILARITY').desc_nulls_last(), f.col('ADDRESS_NUM_SIMILARITY').desc_nulls_last(), f.col('DISTANCE_METERS').asc_nulls_last(),f.col('ADDRESS_TEXT_SIMILARITY').desc_nulls_last()]
w = Window.partitionBy('OUTLET_ID_EXTERNAL').orderBy(*sortExpr)

df2 = (df2.withColumn('MATCH_RANK_EXTERNAL', f.row_number().over(w))
          .filter(f.col('MATCH_RANK_EXTERNAL')==1)
          .drop('MATCH_RANK_EXTERNAL')
      )

# COMMAND ----------

# MAGIC %md ##### Save results

# COMMAND ----------

columns = [[Col+'_INTERNAL', Col+'_EXTERNAL', Col+'_SIMILARITY'] for Col in ['CITY_TEXT', 'ADDRESS_TEXT', 'ADDRESS_NUM', 'POSTAL_CODE']]
columns = [x for xs in columns for x in xs]+['OUTLET_ID_INTERNAL', 'OUTLET_ID_EXTERNAL', 'OUTLET_NAME_1_INTERNAL', 'OUTLET_NAME_2_INTERNAL', 'OUTLET_NAME_EXTERNAL', 'OUTLET_NAME_SIMILARITY', 
                                             'ADDRESS_INTERNAL', 'ADDRESS_EXTERNAL', 'DISTANCE_METERS', 'MATCHED', 'MATCH_RULE'] # , 'MATCH_RULE'

resultDf = df2.select(*columns).filter(f.col('MATCHED')=='YES')

resultDf.coalesce(24).write.mode('overwrite').format("parquet").saveAsTable('harry.temp_resultDf') 
resultDf = spark.sql('select * from harry.temp_resultDf') 


# COMMAND ----------

def save_to_csv(df, path: str):
  ''' Append dataframe to existing file in saved location and save as a single csv file. '''
  
  # Read paths
  save_location, file_name = path.rsplit('/',1)
  save_location = save_location.replace('/dbfs', 'dbfs:')
  csv_location = save_location + '/temp.folder'
  file_location = save_location + '/' + file_name
  
  # Save to temp location
  df.repartition(1).write.mode("overwrite").csv(path=csv_location, header="true")

  # Move to proper location (as single file)
  file = dbutils.fs.ls(csv_location)[-1].path
  dbutils.fs.cp(file, file_location)
  dbutils.fs.rm(csv_location, recurse=True)
  
  return True


tempDf = (resultDf.withColumn('ADDRESS_NUM_INTERNAL', f.col('ADDRESS_NUM_INTERNAL').cast('string'))
                  .withColumn('ADDRESS_NUM_EXTERNAL', f.col('ADDRESS_NUM_EXTERNAL').cast('string'))
                  .withColumn('MATCH_RULE', f.col('MATCH_RULE').cast('string'))
         )

save_to_csv(tempDf, '/mnt/datalake/development/Harry/at_matching_03_08_2022.csv')


# COMMAND ----------

resultDf.count()

# COMMAND ----------



expr = (f.col('OUTLET_ID_INTERNAL')=='1210010301') # (f.col('OUTLET_ID_EXTERNAL')=='575df8be754ab7ce124c214506416ff0c56e00c0')#&
resultDf.filter(expr).display()

# COMMAND ----------

display(resultDf)

# COMMAND ----------



# COMMAND ----------

# xDf = resultDf.groupBy('OUTLET_ID_EXTERNAL').agg(f.countDistinct('OUTLET_ID_INTERNAL').alias('cnt')).filter(f.col('cnt')>1).select('OUTLET_ID_EXTERNAL').distinct()
# resultDf.join(xDf, on='OUTLET_ID_EXTERNAL', how='inner').display()

# COMMAND ----------

# externalDf.join(resultDf.select('OUTLET_ID_EXTERNAL').withColumnRenamed('OUTLET_ID_EXTERNAL', 'OUTLET_ID').distinct(), on='OUTLET_ID', how='left_anti').display()#$ OUTLET_ID_EXTERNAL

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

priceDf.count()

# COMMAND ----------

