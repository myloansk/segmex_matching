# Databricks notebook source
from __future__ import annotations
from abc import ABC, abstractmethod
from thefuzz import fuzz
from itertools import combinations
from typing import List
from unidecode import unidecode

import re
import pyspark.sql.functions as f
import pyspark.sql.types as t


class FuzzyMatcher(ABC):
    """
    The FuzzyMatcher interface declares operations common to all supported versions
    of some algorithm.

    The Context uses this interface to call the algorithm defined by Concrete
    Strategies.
    """
    #_func = None
    
    @staticmethod
    @abstractmethod
    def match(*args)->None:
        pass
      
class FuzzyMatchTokeSort(FuzzyMatcher):
  #func = f.udf(FuzzyMatchTokeSort.match, t.IntegerType())
  
  #@f.udf(t.IntegerType())
  @staticmethod
  @f.udf(t.IntegerType())
  def match(txt1:t.StringType(), txt2:t.StringType()):
  
    def clean_text(txt):
      txt = re.sub("[!,*)@#%(&$_?.^']", '', txt)
      txt = re.sub(' +', ' ', txt) # Remove multiple spaces
      txtDecodedT = unidecode(txt)
      txtDecodedT = txtDecodedT.lower()
      return txtDecodedT
  
    if txt1 and txt2:
      txt1 = clean_text(txt1)
      txt2 = clean_text(txt2)
      similarity_score = int(fuzz.token_sort_ratio(txt1, txt2))
    else:
      similarity_score = None

    return similarity_score

class FuzzyMatchTokenSet(FuzzyMatcher):
  @staticmethod
  @f.udf(t.IntegerType())
  def match(txt1:t.StringType(), txt2:t.StringType()):

    if txt1 and txt2:

      best_similarity_score = 0
      min_len = min(len(txt1.split()), len(txt2.split()))

      # Iterate over combination of words inside the address
      for txtcomb1 in combinations(txt1.split(), min_len):
        for txtcomb2 in combinations(txt2.split(), min_len):
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

# COMMAND ----------

class FuzzyMatcherContext():
  def __init__(self):
    self._fuzzyMatcher = None
      
  @property
  def stringProcess(self) -> FuzzyMatcher:
        """
        The Context maintains a reference to one of the Strategy objects. The
        Context does not know the concrete class of a strategy. It should work
        with all strategies via the Strategy interface.
        """

        return self._fuzzyMatcher

  @stringProcess.setter
  def strategy(self, fuzzyMatcher: FuzzyMatcher) -> None:
        """
        Usually, the Context allows replacing a Strategy object at runtime.
        """

        self._fuzzyMatcher = fuzzyMatcher
        
  def calculate_similarity(self, dFrame:DataFrame, nameOfTargetColumn:str, namesOfInputColumns:List[str])->DataFrame:
    return dFrame.withColumn(nameOfTargetColumn ,self._fuzzyMatcher.match(f.col(namesOfInputColumns[0]), f.col(namesOfInputColumns[1])))
  
        
def letsGo(dFrame:DataFrame)->DataFrame:
    # Init objects to be used
    fuzzyMatchTokenSort = FuzzyMatchTokeSort()
    fuzzyMatcherContext = FuzzyMatcherContext()
    
    #Setting fuzzy matching similarity algorithm to fuzzy_match_token_sort
    fuzzyMatcherContext._fuzzyMatcher = fuzzyMatchTokenSort
    
    #Calculating fuzzy matching similarity of outlet name using fuzzy_match_token_sort
    dFrame = fuzzyMatcherContext.calculate_similarity(dFrame, 'OUTLET_NAME_1_SIMILARITY',['OUTLET_NAME_1_INTERNAL', 'OUTLET_NAME_EXTERNAL'] )
    dFrame = fuzzyMatcherContext.calculate_similarity(dFrame, 'OUTLET_NAME_2_SIMILARITY',['OUTLET_NAME_2_INTERNAL', 'OUTLET_NAME_EXTERNAL'] )
 
    dFrame = dFrame.withColumn('OUTLET_NAME_SIMILARITY', f.greatest(f.col('OUTLET_NAME_1_SIMILARITY'), f.col('OUTLET_NAME_2_SIMILARITY')) )
  
    #Setting fuzzy matching similarity algorithm to fuzzy_match_token_set
    fuzzyMatcherContext._fuzzyMatcher = FuzzyMatchTokenSet
    
    #Calculating fuzzy matching similarity of outlet address using fuzzy_match_token_sest
    dFrame = fuzzyMatcherContext.calculate_similarity(dFrame, 'CITY_TEXT_SIMILARITY',['CITY_TEXT_INTERNAL', 'CITY_TEXT_EXTERNAL'] )
    dFrame = fuzzyMatcherContext.calculate_similarity(dFrame, 'ADDRESS_TEXT_SIMILARITY',['ADDRESS_TEXT_INTERNAL', 'ADDRESS_TEXT_EXTERNAL'] )
 
    return dFrame
    
     

# COMMAND ----------

"""
Example
  /* Run this example to learn how to how invoke and use API
  data = [
          {
           'CUSTOMER':'1210014927','OUTLET_NAME_1_INTERNAL':'HOTEL SONNENSPITZE GESMBH&COKG',
           'OUTLET_NAME_EXTERNAL':'baguette','OUTLET_NAME_2_INTERNAL':'FAMILIE PESENDORFER','ADDRESS_TEXT_INTERNAL':'haberlgasse',
           'ADDRESS_TEXT_EXTERNAL':'payergasse vienna','CITY_TEXT_INTERNAL':'wien','CITY_TEXT_EXTERNAL':'ottakring'
           }
         ]

  df = spark.createDataFrame(data)

  data1 = letsGo(df)

  data1.display()
  */
"""
