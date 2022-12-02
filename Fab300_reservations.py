from Support import connect_to_SQL, connect_to_SQL2, get_SP_drive
import pandas as pd
import numpy as np
import datetime


def get_fab300_reservations(cfg):
    mindate = datetime.date(2022, 9, 30)
    maxdate = datetime.date.today()
    cnxn = connect_to_SQL2(cfg.sqlCfg)
    mindate = mindate.strftime('%Y-%m-%d')
    maxdate = maxdate.strftime('%Y-%m-%d')
    cursor = cnxn.cursor()
    fab300_res = "EXEC GET_FAB300reservations @mindate = ? , @maxdate = ?"
    params = (mindate, maxdate)
    cursor.execute(fab300_res, params)
    cnxn.commit()
    cursor.close()

    fab300_reservations_query = "SELECT * FROM OUTPUT_fab300_raw_reservations"
    fab300_reservations = pd.read_sql(fab300_reservations_query, cnxn)
    cnxn.close()
    return fab300_reservations


def identify_reservations(df):
    try:
        Fab300_raw_reservations = df.copy()
        Fab300_raw_reservations["DATE_TIME_STAMP"] = pd.to_datetime(Fab300_raw_reservations["DATE_TIME_STAMP"])
        SortedRows = Fab300_raw_reservations.sort_values(["EVENT_ROW_ID"])

        index = range(1, len(SortedRows) + 1)
        IndexShift_1 = [i - 1 if i % 2 != 0 else np.nan for i in index]
        IndexShift_2 = [i - 1 if i % 2 == 0 else np.nan for i in index]

        SortedRows["Index"] = index
        SortedRows["IndexShift_1"] = IndexShift_1
        SortedRows["IndexShift_2"] = IndexShift_2
        SortedRows["IndexShift_1_1"] = SortedRows["IndexShift_1"].fillna(method='bfill')
        SortedRows["IndexShift_2_1"] = SortedRows["IndexShift_2"].fillna(method='bfill')
        SortedRows = SortedRows.drop(columns=['IndexShift_1', 'IndexShift_2'])
        SortedRows = SortedRows.rename(columns={"IndexShift_1_1": "IndexShift_1", "IndexShift_2_1": "IndexShift_2"})
        FilledUp2 = SortedRows.copy()

        UnpivotedOnlySelectedColumns = pd.melt(FilledUp2,
                                               id_vars=['FO_ROW_ID', 'Index', 'IndexShift_1', 'IndexShift_2'],
                                               value_vars=["ResWBS", "ResTk", "DATE_TIME_STAMP", "USER_ID",
                                                           "EVENT_ROW_ID"])

        UnpivotedOnlySelectedColumns["ResProperty_1"] = np.where(
            UnpivotedOnlySelectedColumns['Index'] == UnpivotedOnlySelectedColumns['IndexShift_1'],
            UnpivotedOnlySelectedColumns['variable'] + "_Start", UnpivotedOnlySelectedColumns['variable'] + "_End")
        UnpivotedOnlySelectedColumns["ResProperty_2"] = np.where(
            UnpivotedOnlySelectedColumns['Index'] == UnpivotedOnlySelectedColumns['IndexShift_2'],
            UnpivotedOnlySelectedColumns['variable'] + "_Start", UnpivotedOnlySelectedColumns['variable'] + "_End")

        AddedCustom4 = UnpivotedOnlySelectedColumns.copy()
        RemovedColumns1 = AddedCustom4.drop(columns=["Index", "IndexShift_2", "variable", "ResProperty_2"])

        PivotedColumn1 = RemovedColumns1.pivot(index=['FO_ROW_ID', 'IndexShift_1'], columns='ResProperty_1',
                                               values='value').reset_index()

        if "ResWBS_Start" in PivotedColumn1.columns:
            FilteredRows01 = PivotedColumn1[
                (PivotedColumn1["ResWBS_Start"].notnull() & PivotedColumn1['ResWBS_Start'].str.len() > 0)]
        else:
            # empty dataframe
            FilteredRows01 = pd.DataFrame(columns=PivotedColumn1.columns)

        RemovedColumns2 = AddedCustom4.drop(columns=["Index", "IndexShift_1", "variable", "ResProperty_1"])
        PivotedColumn2 = RemovedColumns2.pivot(index=['FO_ROW_ID', 'IndexShift_2'], columns='ResProperty_2',
                                               values='value').reset_index()


        if "ResWBS_Start" in PivotedColumn2.columns:
            FilteredRows02 = PivotedColumn2[
                (PivotedColumn2["ResWBS_Start"].notnull() & PivotedColumn2['ResWBS_Start'].str.len() > 0)]
        else:
            # empty dataframe
            FilteredRows02 = pd.DataFrame(columns=PivotedColumn2.columns)

        columns = [
            'FO_ROW_ID',
            'DATE_TIME_STAMP_End',
            'DATE_TIME_STAMP_Start',
            'EVENT_ROW_ID_End',
            'EVENT_ROW_ID_Start',
            'ResTk_End',
            'ResTk_Start',
            'ResWBS_End',
            'ResWBS_Start',
            'USER_ID_End',
            'USER_ID_Start'
        ]

        if FilteredRows01.shape[0] == 0:
            combined = FilteredRows02[columns]
        elif FilteredRows02.shape[0] == 0:
            combined = FilteredRows01[columns]
        else:
            FilteredRows01 = FilteredRows01[columns]
            FilteredRows02 = FilteredRows02[columns]

        combined = pd.concat([FilteredRows01, FilteredRows02])

        combined = combined.rename(
            columns=
            {
                "ResTk_Start": "ResTk",
                "ResWBS_Start": "WBS",
                "EVENT_ROW_ID_Start": "EVENT_ROW_ID_Begin",
                "DATE_TIME_STAMP_Start": "DATE_TIME_STAMP_Begin"
            }
        )

        columns = [
            "FO_ROW_ID",
            "ResTk",
            "WBS",
            "EVENT_ROW_ID_Begin",
            "EVENT_ROW_ID_End",
            "DATE_TIME_STAMP_Begin",
            "DATE_TIME_STAMP_End",
            "USER_ID_Start",
            "USER_ID_End"
        ]
        Tools_with_resersvations = combined  # [columns]

        final = combined[columns]
        return final
    except:
        return pd.DataFrame()


def processFab300RawReservations(df):
    columns = [
        "FO_ROW_ID",
        "DATE_TIME_STAMP_Begin",
        "DATE_TIME_STAMP_End",
        'EVENT_ROW_ID_Begin',
        'EVENT_ROW_ID_End',
        'ResTk',
        'USER_ID_Start',
        'USER_ID_End',
        'WBS'
    ]

    for_row_ids = df["FO_ROW_ID"].unique()

    Tools_with_reservations = pd.DataFrame(columns=columns)
    for row_id in for_row_ids:
        grpdata = df[df["FO_ROW_ID"] == row_id]

        df_reserv = identify_reservations(grpdata)
        if df.shape[0] > 0:
            Tools_with_reservations = pd.concat([Tools_with_reservations, df_reserv])

    return Tools_with_reservations


def FAB300_with_tool_names(Tools_with_reservations,Tools_Parents):
    Expanded_Tools_parents = pd.merge(
        Tools_with_reservations,
        Tools_Parents,
        left_on=["FO_ROW_ID"],
        right_on=["ROW_ID"],
        how="left",
        suffixes=["","_y"]
    )
    Expanded_Tools_parents = Expanded_Tools_parents.rename(
        columns={
            "ENT_NAME":"Tool",
            "USER_ID_Start":"USER_ID_Begin",
            "USER_ID_End":"USER_ID_End"
        }
    )
    Expanded_Tools_parents_filteredRows = Expanded_Tools_parents[
        Expanded_Tools_parents["USER_ID_Begin"] == Expanded_Tools_parents["USER_ID_End"]
    ]
    Expanded_Tools_parents_filteredRows = Expanded_Tools_parents_filteredRows.rename(
        columns={
            "USER_ID_Begin":"User_id",
            "DATE_TIME_STAMP_Begin":"Begin",
            "DATE_TIME_STAMP_End":"End"
        }
    )
    Expanded_Tools_parents_filteredRows = Expanded_Tools_parents_filteredRows.sort_values(["FO_ROW_ID","EVENT_ROW_ID_Begin"])
    Fab300_Res_id = range(0,len(Expanded_Tools_parents_filteredRows))
    Expanded_Tools_parents_filteredRows["Fab300_Res_id"] = Fab300_Res_id
    Expanded_Tools_parents_filteredRows = Expanded_Tools_parents_filteredRows[
        (Expanded_Tools_parents_filteredRows["Tool"].notnull())
        & (Expanded_Tools_parents_filteredRows['Tool'].str.len() > 0)
    ]
    columns = [
        "FO_ROW_ID",
        "EVENT_ROW_ID_Begin",
        "Begin",
        "End",
        "Fab300_Res_id",
        "FACILITY",
        "ResTk",
        "Tool",
        "User_id",
        "WBS"
    ]
    Expanded_Tools_parents_filteredRows[columns]
    FAB300_with_tool_names = Expanded_Tools_parents_filteredRows[columns]

    return FAB300_with_tool_names
