from Support import connect_to_SQL, connect_to_SQL2, get_SP_drive
import pandas as pd
import numpy as np
import datetime


def get_tools_parents_reservations(cfg):
    cnxn = connect_to_SQL(cfg.sqlCfg)
    query = cfg.sql_queries["tools_parents"]
    tools_parents = pd.read_sql(query, cnxn)
    cnxn.close()
    return tools_parents