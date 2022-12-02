from Support import connect_to_SQL, connect_to_SQL2, get_SP_drive
import pandas as pd
import numpy as np
#from datetime import datetime, timedelta
import datetime



def get_iio_reservations(cfg):
    cnxn = connect_to_SQL(cfg.sqlCfg)
    mindate = datetime.datetime(2022, 9, 30, 0,0,0)
    maxdate = datetime.datetime.now()

    #+ timedelta(days=1)
    mindate = mindate.strftime('%Y-%m-%d %H:%M')
    maxdate = maxdate.strftime('%Y-%m-%d %H:%M')
    query = cfg.sql_queries["iio_raw_res"] % (mindate, maxdate)
    print(query)
    iio_reservations = pd.read_sql(query, cnxn)
    cnxn.close()
    return iio_reservations

def IIO_without_modules(df):
    df = df.reset_index()
    df["Begin"] = pd.to_datetime(df["Begin"], format="%d/%m/%Y %H:%M")
    df["End"] = pd.to_datetime(df["End"], format="%d/%m/%Y %H:%M")
    df = df.sort_values(["WBS", "Tool", "FACILITY", "Module", "Begin"])
    df["End_Down"] = df["End"].shift(1)
    df["Adjacent_Down"] = np.where(df['Begin'] == df['End_Down'], True, False)
    df["IndexCopy"] = np.where(df["Adjacent_Down"] == True, np.nan, df["index"])
    df["IndexCopy2"] = df["IndexCopy"].fillna(method='ffill')
    df["Module"] = df["Module"].fillna("")
    df["Description"] = df["Description"].fillna("")
    df = df[['WBS', 'FACILITY', 'Module', 'Tool', 'Begin', 'Description', 'End', 'User_id', 'IndexCopy2', 'End_Down']]

    params = {
        'Begin': 'min',
        'End': 'max',
        'Description': lambda x: ';'.join(sorted(pd.Series.unique(x))),
        'User_id': lambda x: ';'.join(sorted(pd.Series.unique(x)))
    }
    sub = df[["WBS", "Tool", "FACILITY", "Module", "Begin", "End", "Description", 'User_id', 'IndexCopy2']]
    GroupedRows1 = sub.groupby(["WBS", "Tool", "FACILITY", "Module", 'IndexCopy2']).agg(params).reset_index()

    params = {
        'Module': lambda x: ';'.join(sorted(pd.Series.unique(x))),
        'Description': 'first',
        'User_id': 'first'
    }

    GroupedRows1 = GroupedRows1.groupby(["WBS", "Tool", "FACILITY", "Begin", "End"]).agg(params).reset_index()
    GroupedRows1 = GroupedRows1.sort_values(["FACILITY", "Tool", "WBS", "Begin", "End"])

    Index = range(0, len(GroupedRows1))
    GroupedRows1["IIO_Res_id"] = Index

    GroupedRows1 = GroupedRows1.rename(
        columns={
            "Module": "Modules"
        }
    )
    IIO_without_modules = GroupedRows1.copy()
    print(IIO_without_modules.tail(10))
    return IIO_without_modules

