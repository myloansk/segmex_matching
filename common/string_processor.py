# Databricks notebook source
from __future__ import annotations
from abc import ABC, abstractmethod

from typing import List
from unidecode import unidecode

import re
import pyspark.sql.functions as f
import pyspark.sql.types as t


class StringProcessor(ABC):
    """
    The StringColumnProcess interface declares operations common to all supported versions
    of some algorithm.

    The Context uses this interface to call the algorithm defined by Concrete
    Strategies.
    """
    @staticmethod
    @abstractmethod
    def process(self)->None:
        pass

class AddPostalCode(StringProcessor):
  # Add postal code to separate columns
  @staticmethod
  @f.udf(t.StringType())
  def process(txt:t.StringType()):
      if txt:
        result = re.search("\d{3}\s{0,1}\d{2}", txt) # Find postal code
        if result: 
          return str(result.group(0).replace(' ', ''))

        result = re.search("\d{4}", txt) # Find 4-digit postal code
        if result: 
          return str(result.group(0).replace(' ', ''))

      else: 
        return None

class RemovePostalCode(StringProcessor):
  # Remove postal code from address
  @staticmethod
  @f.udf(t.StringType())
  def process(txt:t.StringType()):
      if txt:
        txt = re.sub("\d{3}\s\d{2}", "",  txt) # Find postal code
    #     txt = re.sub("\d{4}", "",  txt) # Find postal code
        txt = re.sub(' +', ' ', txt) # Remove multiple spaces
        return txt
      else: 
        return None

class RemoveRepeatedInfo(StringProcessor):      
  # Remove extra words from address
  @staticmethod
  @f.udf(t.StringType())
  def process(txt:t.StringType(), country:t.StringType(), county:t.StringType(), state:t.StringType(), city:t.StringType()):
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

class CleanAddress(StringProcessor):
  # --- Clean address & city text ---
  @staticmethod
  @f.udf(t.StringType())
  def process(txt:t.StringType()):
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
class GetAddressNumber(StringProcessor):
  @staticmethod
  @f.udf(t.StringType())
  def process(txt:t.StringType()):
    if txt:
      txt = re.sub('\W+',' ', txt) # Replace special characters with space
      return [int(s) for s in re.findall(r"\s*[0-9]+(?:\s|$)", txt)] # Put numbers to list
    else:
      return None 
  

class DoesItContainNum(StringProcessor):
  @staticmethod
  @f.udf(t.IntegerType())
  def process( lst1:t.StringType(), lst2:t.StringType()):
      if lst1 and lst2 and len(set(lst1).intersection(set(lst2))) > 0:
        return 100
      elif lst1 and lst2:
        return 0
      else:
        return None
  
class ArePostalCodeTheSame(StringProcessor):
      # --- Postal number is same ---
    @staticmethod
    @f.udf(t.IntegerType())
    def process( po1:str, po2:str):
      if po1 and po2 and po1==po2:
        return 100
      elif po1 and po2:
        return 0
      else:
        return None
  

          

# COMMAND ----------

class StringContext():
  def __init__(self)->None:
    self._stringProcess = None
    
  
  @property
  def stringProcess(self) -> StringProcess:
        """
        The Context maintains a reference to one of the Strategy objects. The
        Context does not know the concrete class of a strategy. It should work
        with all strategies via the Strategy interface.
        """

        return self._stringProcess

  @stringProcess.setter
  def strategy(self, stringProcess: StringProcess) -> None:
        """
        Usually, the Context allows replacing a Strategy object at runtime.
        """

        self._stringProcess = stringProcess
        
  def process_string_columns(self, dFrame:DataFrame, nameOfTargetColumn:str, namesOfInputColumns:List[str])->DataFrame:
    
    return dFrame.withColumn(nameOfTargetColumn ,self._stringProcess.process(*namesOfInputColumns))
        
def letsGo(dFrame:DataFrame)->DataFrame:
    # Init objects to be used
    addPostalCode = AddPostalCode()
    stringContext = StringContext()
    
    # Setting string operation( add postal code)
    stringContext._stringProcess = addPostalCode
    
    # Adding column with postal code to pyspark.DataFrame
    dFrame = stringContext.process_string_columns(dFrame, 'POSTAL_CODE',['ADDRESS'] )
    
    # Changing operation to remove postal code
    removePostalCode = RemovePostalCode()
    stringContext._stringProcess = removePostalCode
    
    dFrame = stringContext.process_string_columns(dFrame, 'ADDRESS',['ADDRESS'] )
 
    return dFrame


# COMMAND ----------

"""
data = [
          {
           'OUTLET_ID':'93fea262b48036d49a5a38dc007ebbed8872d204','ADDRESS':'Pistorf 38',
           'COUNTRY':"austria", 'CITY':'gleinstätten','COUNTY':'gleinstätten','STATE':'steiermark'
           }
         ]

df = spark.createDataFrame(data)

data1 = letsGo(df)

data1.display()
"""