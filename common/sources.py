from __future__ import annotations
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from typing import Dict, List, NamedTuple,Optional

import pandas as pd 

class Source(ABC):

    _sourceConfig:NamedTuple

    @abstractmethod
    def readData(self)->DataFrame:pass

    @abstractmethod
    def prepareData(self)->DataFrame:pass

