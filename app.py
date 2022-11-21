# Databricks notebook source
# MAGIC %run ./commons/extract

# COMMAND ----------

import argparse
import logging
import logging.config

import yaml

def main():
    """
    entry point to run the xtra ETL job
    """
    
    
    # Parsing Config YAML file
    #parser = argparse.ArgumentParser(description='Run the Outlet Matching job.')
    #parser.add_argument('config', help='A configuration file in YAML format.')
    #args = parser.parse_args()
    #config = yaml.safe_load(open(args.config)
    
    consfig_file_path = '/dbfs/mnt/datalake/development/mylonas/outlet_matching_config_v2.yml'

    config = yaml.safe_load(open(consfig_file_path))
    
    # Configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    
    # Reading source configuration                         
    logger.info('Outlet Matching ETL job started')
    logger.info('Outlet Matching ETL job Mounting Configs from yaml to DataClasses ...')
                            
    # Init a InternalSrcConfig  dataclass object that holds configuration for external(adapter layer)
    internalSrcConfig = InternalSrcConfig(**config['layer']['adaption']['internal'])

    # Init a ExternalSrcConfig  dataclass object that holds configuration for external(adapter layer)
    externalSrcConfig = ExternalSrcConfig(**config['layer']['adaption']['external'])

    # Init a TrgConfig  dataclass object that holds configuration for application layer
    trgConfig = TrgConfig(**config['layer']['application'])                         
    
    logger.info('Outlet Matching ETL job Init objects dependant to extract stage')
    #extractData = ExtractData(internalSelectedColumnLst,internalFilePaths,internalFilteringCond,selectedColumnMap,poiListingPaths,externalFilteringCond)
    extractData = ExtractData(internalSrcConfig, externalSrcConfig)
  
    transformerConfig = TransformerConfig(['LONGITUDE','LATITUDE'],'OUTLET_ID')
    fuzzyMatcherContext = FuzzyMatcherContext()
    stringMatcherContext = StringContext()
    transformerContext = TransformerContext(stringMatcherContext, fuzzyMatcherContext, transformerConfig)
  
    logger.info('Outlet Matching ETL job Init Object responsible for Outlet Matching Job')
    outletMatching = OutletMatching(extractData, transformerContext,trgConfig)
    logger.info('Outlet Matching ETL job Extract Stage')
    outletMatching.extract()
    logger.info('Outlet Matching ETL job Tranform Stage')
    outletMatching.transform()
    logger.info('Outlet Matching ETL job Load Stage')
    outletMatching.load()

    

if __name__ == '__main__':
    main()
