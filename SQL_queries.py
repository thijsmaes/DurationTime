import pandas as pd
import numpy as np
import functools as ft
import warnings
warnings.filterwarnings('ignore')

iio_raw_res = " SELECT [WBS], [Begin_Date] AS \'Begin\', [End_Date] AS \'End\', [RESDESCR] AS  \'Description\' ,[RES_TK]," \
              "[RES_TOOL] AS \'Tool\', LOWER([USER]) AS \'User_id\', [FACILITY],Replace(TRIM(Replace([RES_ITEM], [RES_TOOL], \'\')), \'-\', \'\') AS \'Module\'" \
              "FROM [fab].[W_STATUS_RESERVATION_RES] WHERE WBS <>\'\' AND WBS IS NOT NULL AND (STATUS = \'approved\' OR STATUS = \'request\') " \
              "AND (FACILITY = \'PLINE200\' OR FACILITY = \'PLINE300\')"
