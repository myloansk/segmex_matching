# Databricks notebook source
import pyspark.sql.functions as f
import pyspark.sql.types as t
import pandas as pd

from builtins import sum as pysum
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from typing import ( Dict, List,
                    NamedTuple, Optional
)

class InternalSrcConfig(NamedTuple):
  selected_column_list:List[str]
  filepaths:Dict[str, str]
  gloabal_acount_group:List[str]
  global_cust_hier2_cust:List[str]
  global_order_block_list:List[str]
  global_exclude_channel_list:List[str]
  global_exclude_subchannel_list:List[str]
  global_account_group_list:List[str] = [str(a) for a in range(9000, 9099 + 1)]
  
  
  
  def __getter_filter_template__(self)->str: 
    return """{expr}""".format(expr = ' and '.join(['(ACCNT_GRP IN ({globalAccountGroupListPch}))','(CUST_HIE02 IN ({globalCustHier2ListPch}))',
                                '(_BIC_CTRADE_CH NOT IN ({globalExlcudeChannelListPch}))','(_BIC_CSUB_TRAD NOT IN ({globalExlcudeSubChannelListPch}))',
                                '(CSUB_TRAD_DESC !="DO NOT USE")',"(LONGITUDE != 0)", '(_BIC_CSUP_CUST IN ({globalOrderBlockListPch}))',
                                "(LATITUDE != 0)", '(LONGITUDE IS NOT NULL)', '(LATITUDE IS NOT NULL)']))
    
  
  def __create_filtering_condition__(self)->str:
    return self.__getter_filter_template__().format(globalAccountGroupListPch = ','.join([ele for ele in self.global_account_group_list]), 
                                                    globalCustHier2ListPch = ','.join(["\"" + ele + "\"" for ele in self.global_cust_hier2_cust]),
                                                    globalOrderBlockListPch = ','.join(["\"" + ele + "\"" if ele!='' else ele for ele in self.global_order_block_list]),
                                                    globalExlcudeChannelListPch = ','.join([ "\"" + ele + "\""  for ele in self.global_exclude_channel_list]),
                                                    globalExlcudeSubChannelListPch = ','.join(["\"" + ele + "\"" for ele in self.global_exclude_subchannel_list]))

class ExternalSrcConfig(NamedTuple):
  selected_column_list:List[str]
  selected_column_aliases:List[str]
  poi_filepath:str
  
  def __create_filtering_conditions__(self)->str:
    return """{expr}""".format(expr = ' and '.join(["(CAST(location_lon AS DOUBLE)!=0)","(CAST(location_lat AS DOUBLE)!=0)","(location_lon IS NOT NULL)", "(location_lat IS NOT NULL)"]))
    
  def __create_map_of_selected_column_aliases__(self)->Dict[str, str]:
    return dict(zip(self.selected_column_list, self.selected_column_aliases))
  
  

  
country = 'at'
class ExtractData():
    def __init__(self, srcConfig:NamedTuple = None, cc:str = country )->None:
        #self._internalSrcConfig = internalSrcConfig
        #self._externalSrcConfig = externalSrcConfig
        self._srcConfig = srcConfig
        self._cc = cc
      

        #self._internalDf:DataFrame = None
        #self._externalDf:DataFrame = None
        #self._auxiliaryDf:DataFrame = None
      
    #def get_auxiliary_data(self)->DataFrame:
    #  return self._auxiliaryDf

    #def set_auxiliary_data(self, df:DataFrame)->None:
    #  self._auxiliaryDf = df

    def get_source_config(self)->NamedTuple:
      return self._srcConfig

    def set_source_config(self, srcConfig:NamedTuple)->None:
      self._srcConfig = srcConfig
    
    def extract_cchbc_data(self)->DataFrame:pass

    def extract_poi_data(self)->DataFrame:
      poiData = (PoiData()
                  .setConfig(self._srcConfig))
      
      (poiData.readData()
            .prepareAuxiliaryData()
            .prepareData())

      return poiData._sparkDf
    
    def extract_cchbc_data(self)->DataFrame:
      cchbcData = (CCHBCData.setConfig(self._srcConfig))

      cchbcData.readData().prepareData()

      return cchbcData._spark

    #def get_internal_source_config(self)->InternalSrcConfig:
    #  return self._internalSrcConfig
    
    #def set_internal_source_config(self, internalSrcConfig:InternalSrcConfig)->None:
    #  self._internalSrcConfig = internalSrcConfig
    #def get_external_source_config(self)->ExternalSrcConfig:
    #  return  self._externalSrcConfig
    
    #def set_external_source_config(self)->None:
    #  self._externalSrcConfig = externalSrcConfig

    #def get_internal_data(self)->DataFrame:
    #      # Load customer MD
    #      customerDf = spark.read.option("header", "true").option("sep", "|").csv(self._internalSrcConfig.filepaths['customer'].format( CAPS_CC = self._cc.upper()))
    #      customerDf = customerDf.fillna("NAN", subset=['_BIC_CSUP_CUST'])
    #      # Apply standard filters
    #      customerDf = customerDf.filter(self._internalSrcConfig.__create_filtering_condition__())
    #      # Load customer MD PI
    #      customerPiDf = spark.read.option("header", "true").option("sep", "|").csv(self._internalSrcConfig.filepaths['customer_pi'].format( CAPS_CC = self._cc.upper()))

    #      # Load country name
    #      salesOrgDf = spark.read.option("header", "true").csv(self._internalSrcConfig.filepaths['sales_org'])
    #      salesOrgDf = salesOrgDf.select('SALESORG', 'COUNTRYTXT').distinct()
          
          # Create columns
    #      self._internalDf = (customerDf.join(customerPiDf, on='CUSTOMER', how='left')
    #                              .join(salesOrgDf, on='SALESORG', how='left')
    #                              .select(customerDf['CUSTOMER'].alias('OUTLET_ID'),
    #                                      f.coalesce(customerPiDf['NAME'], customerPiDf['CUSTOMER_DESC']).alias('OUTLET_NAME_1'),
    #                                      f.coalesce(customerPiDf['NAME2'], customerPiDf['CUSTOMER_DESC']).alias('OUTLET_NAME_2'),
    #                                      salesOrgDf['COUNTRYTXT'].alias('COUNTRY'),
    #                                      customerPiDf['CITY'].alias('CITY'),
    #                                      customerPiDf['STREET'].alias('ADDRESS'),
    #                                      customerPiDf['POSTAL_CD'].alias('POSTAL_CODE'),
    #                                      customerDf['LONGITUDE'].cast('double').alias('LONGITUDE'),
    #                                      customerDf['LATITUDE'].cast('double').alias('LATITUDE'),
    #                                  )
    #                  )
    #      # Add suffix to column names
    #     self._internalDf = self._internalDf.select(*[f.col(Col).alias(Col+'_INTERNAL') for Col in self._internalSrcConfig.selected_column_list])
    #      return self._internalDf  

    

    #def get_external_data(self)->DataFrame:
      # Load external outlet data
    #    df_from_each_file = (pd.read_csv(f, low_memory=False) for f in [self._externalSrcConfig.poi_filepath[self._cc]])
    #    poiListingPd   = pd.concat(df_from_each_file, ignore_index=True)

    #    # Convert to spark dataframe
    #    poiListingDf = spark.createDataFrame(poiListingPd)
        
        # Filter for at least one review in the last 6 months
    #    exprSumCounts = lambda months : pysum( f.coalesce(f.col(f'reviews_count_m{i}').cast('integer'), f.lit(0)) for i in range(1, months+1))
    #    poiListingDf = poiListingDf.filter(exprSumCounts(6)>0)

    #    self.set_auxiliary_data(poiListingDf)
    #    # Filter for valid coordinates
        
    #    poiListingDf = poiListingDf.filter(self._externalSrcConfig.__create_filtering_conditions__())                                                                            
    #    poiListingDf = poiListingDf.withColumn('POSTAL_CODE',f.lit(None).cast(t.StringType()))
    #    poiListingDf = poiListingDf.withColumn('CITY',f.coalesce(f.col('location_city'), f.col('location_state')))

    #    # Finalize table
    #    mapOfColumnAliases = self._externalSrcConfig.__create_map_of_selected_column_aliases__()
    #    self._externalDf = (poiListingDf.select([f.col(colName).alias(aliasName) for colName, aliasName in mapOfColumnAliases.items()]))