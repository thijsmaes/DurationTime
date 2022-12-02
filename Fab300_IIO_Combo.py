import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

def Fab300_iio_merger(df, Source_iio, Source_fab):
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    df = df.sort_values(["WBS" ,"FACILITY" ,'Tool' ,"DateTime" ,"FAB300_BeginEnd"])

    df["Fab300_Res_id_UP"] = df["Fab300_Res_id"].fillna(method='bfill')
    df["Fab300_Res_id_DOWN"] = df["Fab300_Res_id"].fillna(method='ffill')
    df["IIO_Res_id_UP"] = df["IIO_Res_id"].fillna(method='bfill')
    df["IIO_Res_id_DOWN"] = df["IIO_Res_id"].fillna(method='ffill')

    fab_iio_Filtered_Rows = df[
        (df["Fab300_Res_id_UP"] == df["Fab300_Res_id_DOWN"])
        & (df["IIO_Res_id_UP"] == df["IIO_Res_id_DOWN"])
        & (df["Fab300_Res_id_UP"].notnull())
        & (df["IIO_Res_id_UP"].notnull())
        ]

    Removed_Other_Columns = fab_iio_Filtered_Rows[["Fab300_Res_id_UP", "IIO_Res_id_UP"]]
    Removed_Duplicates = Removed_Other_Columns.drop_duplicates()
    Renamed_Columns = Removed_Duplicates.rename(
        columns = {
            "Fab300_Res_id_UP" :"Fab300_Res_id",
            "IIO_Res_id_UP" :"IIO_Res_id"
        }
    )

    index = range(0 ,len(Renamed_Columns))
    Renamed_Columns["Index"] = index
    Added_Index = Renamed_Columns.copy()
    State_DOWN_Remove = Added_Index.copy()
    State_DOWN_Remove.loc[-1] = [np.nan ,np.nan ,-1]  # adding a row
    State_DOWN_Insert = State_DOWN_Remove.copy()
    State_DOWN_Insert = State_DOWN_Insert.sort_values(["Index"])
    index2 = range(0 ,len(State_DOWN_Insert))
    State_DOWN_Insert["Index2"] = index2

    State_DOWN_Add_Index = State_DOWN_Insert.copy()
    State_DOWN_Rename = State_DOWN_Add_Index.rename(
        columns = {
            "Fab300_Res_id" :"Fab300_Res_id_DOWN",
            "IIO_Res_id" :"IIO_Res_id_DOWN"
        }
    )

    With_DOWN = Added_Index.merge(
        State_DOWN_Rename,
        left_on=["Index"],
        right_on=["Index2"],
        suffixes=["" ,"_y"],
        how = 'left'
    )

    With_DOWN["Index3"] = np.where(
        (With_DOWN['Fab300_Res_id' ]==With_DOWN['Fab300_Res_id_DOWN'])
        | (With_DOWN['IIO_Res_id' ]==With_DOWN['IIO_Res_id_DOWN']),
        np.nan,
        With_DOWN['Index']
    )

    Replaced_Value = With_DOWN.copy()
    Replaced_Value["Index4"] = Replaced_Value["Index3"].fillna(method='ffill')
    Replaced_Value = Replaced_Value.drop(columns=["Fab300_Res_id_DOWN", "IIO_Res_id_DOWN"])
    Renamed_Columns1 = Replaced_Value.rename(
        columns={
            "Index4" :"Cluster"
        })

    Merged_queries = Renamed_Columns1.merge(
        Source_fab,
        left_on=["Fab300_Res_id"],
        right_on=["Fab300_Res_id"],
        suffixes=["" ,"_y"],
        how = 'left'
    )
    Expanded_Fab300_with_tool_names = Merged_queries.rename(
        columns ={
            "Begin": "Fab300_Begin",
            "End": "Fab300_End",
            "User_id": "Fab300_User_id"
        })
    Merged_queries_1 = Expanded_Fab300_with_tool_names.merge(
        Source_iio,
        left_on=["IIO_Res_id"],
        right_on=["IIO_Res_id"],
        suffixes=["" ,"_y"],
        how = 'left'
    )
    Expanded_IIO_without_modules = Merged_queries_1.rename(
        columns ={
            "Begin": "IIO_Begin",
            "End": "IIO_End",
            "Modules": "Modules",
            "User_id": "IIO_User_id",
            "Description": "Description"
        })
    Expanded_IIO_without_modules = Expanded_IIO_without_modules.drop(
        columns=["FACILITY_y","Tool_y" ,"WBS_y" ,'Index', 'Index_y', 'Index2', 'Index3']
    )
    return Expanded_IIO_without_modules

def getFab300_IIO_Combo(Fab300_with_tool_names, IIO_without_modules):
    Source_fab = Fab300_with_tool_names.copy()
    RC_fab = Source_fab.drop(columns=["ResTk", "User_id"])
    UnPivot_fab = pd.melt(RC_fab, id_vars=["WBS", "FACILITY", "Tool", "Fab300_Res_id"],
                          value_vars=["Begin", "End"])
    UnPivot_fab = UnPivot_fab.rename(
        columns={
            "variable": "FAB300_BeginEnd",
            "value": "DateTime"
        })

    Source_iio = IIO_without_modules.copy()
    RC_iio = Source_iio.drop(columns=["Modules", "User_id", "Description"])
    UnPivot_iio = pd.melt(RC_iio, id_vars=["WBS", "FACILITY", "Tool", "IIO_Res_id"],
                          value_vars=["Begin", "End"])

    UnPivot_iio = UnPivot_iio.rename(
        columns={
            "variable": "IIO_BeginEnd",
            "value": "DateTime"
        })

    fab_iio_together = pd.concat([UnPivot_fab, UnPivot_iio], ignore_index=True)

    columns = [
        'Fab300_Res_id',
        'IIO_Res_id',
        'Cluster',
        'Fab300_Begin',
        'Fab300_End',
        'FACILITY',
        'ResTk',
        'Tool',
        'Fab300_User_id',
        'WBS',
        'IIO_Begin',
        'Description',
        'IIO_End'
    ]

    Fab300_IIO_overlaps_ids = pd.DataFrame(columns=columns)

    WBSs = fab_iio_together["WBS"].unique()

    for wbs in WBSs:
        wbsdata = fab_iio_together[
            (fab_iio_together["WBS"] == wbs)
        ]
        facilities = wbsdata["FACILITY"].unique()
        for facility in facilities:
            facilityData = wbsdata[
                (wbsdata["FACILITY"] == facility)
            ]

            tools = facilityData["Tool"].unique()
            for tool in tools:
                wbstooldata = facilityData[
                    (wbsdata["Tool"] == tool)
                ]
                df = Fab300_iio_merger(wbstooldata, Source_iio, Source_fab)
                Fab300_IIO_overlaps_ids = pd.concat([Fab300_IIO_overlaps_ids, df])

    return Fab300_IIO_overlaps_ids


def Final_Fab300_IIO_reservations(IIO_without_modules, Fab300withtoolnames, Fab300_IIO_overlaps_ids):
    Fab300_IIO_overlaps_ids["Fab300_Begin"] = pd.to_datetime(Fab300_IIO_overlaps_ids["Fab300_Begin"],
                                                             format="%Y-%m-%d %H:%M")
    Fab300_IIO_overlaps_ids["Fab300_End"] = pd.to_datetime(Fab300_IIO_overlaps_ids["Fab300_End"],
                                                           format="%Y-%m-%d %H:%M")
    Fab300_IIO_overlaps_ids["IIO_Begin"] = pd.to_datetime(Fab300_IIO_overlaps_ids["IIO_Begin"], format="%Y-%m-%d %H:%M")
    Fab300_IIO_overlaps_ids["IIO_End"] = pd.to_datetime(Fab300_IIO_overlaps_ids["IIO_End"], format="%Y-%m-%d %H:%M")

    AddedCustom = Fab300_IIO_overlaps_ids.copy()
    AddedCustom['Modules'] = AddedCustom['Modules'].fillna('')
    AddedCustom["Fab300_Duration"] = (AddedCustom["Fab300_End"] - AddedCustom["Fab300_Begin"]) / np.timedelta64(1, 'h')
    AddedCustom1 = AddedCustom.copy()
    AddedCustom1["IIO_Duration"] = (AddedCustom1["IIO_End"] - AddedCustom1["IIO_Begin"]) / np.timedelta64(1, 'h')
    params = {
        'Fab300_Begin': 'min',
        'IIO_Begin': 'min',
        'Fab300_End': 'max',
        'IIO_End': 'max',
        'Fab300_Res_id': 'count',
        'Description': lambda x: ';'.join(sorted(pd.Series.unique(x))),
        'IIO_User_id': lambda x: ';'.join(sorted(pd.Series.unique(x))),
        'Fab300_User_id': lambda x: ';'.join(sorted(pd.Series.unique(x))),
        'Modules': lambda x: ';'.join(sorted(pd.Series.unique(x)))
    }
    sub = AddedCustom1[
        [
            "FACILITY",
            "Tool",
            "WBS",
            "Cluster",
            "Fab300_Begin",
            "IIO_Begin",
            'Fab300_End',
            'IIO_End',
            'Fab300_Res_id',
            'IIO_Res_id',
            'Description',
            'IIO_User_id',
            'Fab300_User_id',
            'Modules'
        ]
    ]
    GroupedRows1 = sub.groupby(["FACILITY", "Tool", "WBS", "Cluster"]).agg(params).reset_index()
    GroupedRows1["Begin"] = np.where(GroupedRows1['Fab300_Begin'] > GroupedRows1['IIO_Begin'],
                                     GroupedRows1['IIO_Begin'], GroupedRows1['Fab300_Begin'])
    GroupedRows1["End"] = np.where(GroupedRows1['Fab300_End'] > GroupedRows1['IIO_End'], GroupedRows1['Fab300_End'],
                                   GroupedRows1['IIO_End'])
    GroupedRows1 = GroupedRows1.rename(
        columns={
            "Fab300_Res_id": "Cnt"
        }).drop(columns=['Fab300_Begin', 'Fab300_End', 'IIO_Begin', 'IIO_End'])

    Duration_FAB = AddedCustom1[
        [
            "FACILITY",
            "Tool",
            "WBS",
            "Cluster",
            "Fab300_Res_id",
            'Fab300_Duration'
        ]
    ].drop_duplicates().reset_index()

    Duration_IIO = AddedCustom1[
        [
            "FACILITY",
            "Tool",
            "WBS",
            "Cluster",
            "IIO_Res_id",
            'IIO_Duration'
        ]
    ].drop_duplicates().reset_index()

    GroupedRows1_FAB = Duration_FAB.groupby(["FACILITY", "Tool", "WBS", "Cluster"]).sum().reset_index()
    GroupedRows1_IIO = Duration_IIO.groupby(["FACILITY", "Tool", "WBS", "Cluster"]).sum().reset_index()

    GroupedRows = pd.concat(
        objs=(iDF.set_index(["FACILITY", "Tool", "WBS", "Cluster"]) for iDF in
              (GroupedRows1, GroupedRows1_FAB, GroupedRows1_IIO)),
        axis=1,
        join='inner'
    ).reset_index()
    GroupedRows['FAB_IIO_Ratio'] = GroupedRows["Fab300_Duration"] / GroupedRows["IIO_Duration"]

    GroupedRows = GroupedRows[
        [
            'FACILITY',
            'Tool',
            'WBS',
            'Cluster',
            'Cnt',
            'Description',
            'Begin',
            'End',
            'IIO_User_id',
            'Fab300_User_id',
            'Modules',
            'Fab300_Duration',
            'IIO_Duration',
            'FAB_IIO_Ratio'
        ]
    ]

    FilteredRows = GroupedRows[
        (GroupedRows["FAB_IIO_Ratio"] < 3.0)
        & (GroupedRows["Cnt"] <= 4)
        ]

    Fab300_IIO_valid_combos = FilteredRows.drop(columns=["Cnt"])

    outer = Fab300_IIO_overlaps_ids.merge(
        Fab300_IIO_valid_combos,
        how='outer',
        left_on=["FACILITY", "Tool", "WBS", "Cluster"],
        right_on=["FACILITY", "Tool", "WBS", "Cluster"],
        indicator=True
    )
    Fab300_IIO_bad_combos = outer[(outer._merge == 'left_only')].drop('_merge', axis=1)

    IIO_ids_from_bad_combos = Fab300_IIO_bad_combos[["IIO_Res_id"]]
    RemovedDuplicates = IIO_ids_from_bad_combos.drop_duplicates()

    MergedQueries = RemovedDuplicates.merge(
        IIO_without_modules,
        how='inner',
        left_on=["IIO_Res_id"],
        right_on=["IIO_Res_id"],
        indicator=True
    )
    RemovedColumns1 = MergedQueries.drop(columns=["IIO_Res_id"])
    IIO_from_bad_combos = RemovedColumns1.rename(
        columns={
            'User_id': 'IIO_User_id'
        }).drop(columns=["_merge"])

    outer_IIO_from_bad_combos = IIO_without_modules.merge(
        Fab300_IIO_overlaps_ids,
        how='outer',
        left_on=["IIO_Res_id"],
        right_on=["IIO_Res_id"],
        indicator=True
    )
    Pure_IIO_Join = outer_IIO_from_bad_combos[(outer_IIO_from_bad_combos._merge == 'left_only')].drop('_merge', axis=1)
    Pure_IIO = Pure_IIO_Join[
        [
            'Begin',
            'Description_x',
            'End',
            'FACILITY_x',
            'IIO_Res_id',
            'Modules_x',
            'Tool_x',
            'User_id',
            'WBS_x'
        ]
    ]
    RenamedColumns = Pure_IIO.rename(
        columns={
            'User_id': 'IIO_User_id',
            'Description_x': 'Description',
            'FACILITY_x': 'FACILITY',
            'Modules_x': 'Modules',
            'Tool_x': 'Tool',
            'WBS_x': 'WBS'
        }
    )
    AppendedQuery = pd.concat([RenamedColumns, IIO_from_bad_combos]).drop(columns=['IIO_Res_id'])
    AppendedQuery["Begin"] = pd.to_datetime(AppendedQuery["Begin"], format="%Y-%m-%d %H:%M")
    AppendedQuery["End"] = pd.to_datetime(AppendedQuery["End"], format="%Y-%m-%d %H:%M")
    AppendedQuery["IIO_Duration"] = (AppendedQuery["End"] - AppendedQuery["Begin"]) / np.timedelta64(1, 'h')
    AddedCustom2 = AppendedQuery.copy()
    AppendedQuery2 = pd.concat([Fab300_IIO_valid_combos, AddedCustom2])

    outer_Fab300withtoolnames = Fab300withtoolnames.merge(
        Fab300_IIO_overlaps_ids,
        how='outer',
        left_on=["Fab300_Res_id"],
        right_on=["Fab300_Res_id"],
        indicator=True
    )

    Pure_Fab300_Join = outer_Fab300withtoolnames[(outer_Fab300withtoolnames._merge == 'left_only')].drop('_merge',
                                                                                                         axis=1)
    Pure_Fab300 = Pure_Fab300_Join[
        [
            'Begin',
            'End',
            'Fab300_Res_id',
            'FACILITY_x',
            'Tool_x',
            'User_id',
            'WBS_x'
        ]
    ].drop(columns=['Fab300_Res_id']).rename(
        columns={
            'FACILITY_x': 'FACILITY',
            'Tool_x': 'Tool',
            'User_id': 'Fab300_User_id',
            'WBS_x': 'WBS'
        })
    RenamedColumns1 = Pure_Fab300.copy()

    RenamedColumns1["Begin"] = pd.to_datetime(RenamedColumns1["Begin"], format="%Y-%m-%d %H:%M")
    RenamedColumns1["End"] = pd.to_datetime(RenamedColumns1["End"], format="%Y-%m-%d %H:%M")

    RenamedColumns1["Fab300_Duration"] = (RenamedColumns1["End"] - RenamedColumns1["Begin"]) / np.timedelta64(1, 'h')
    AddedCustom3 = RenamedColumns1.copy()

    AppendedQuery3 = pd.concat([AppendedQuery2, AddedCustom3]).drop(columns=['FAB_IIO_Ratio'])
    return AppendedQuery3
