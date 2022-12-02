
from datetime import datetime

sqlCfg = {
    "SQL_uname": "",
    "SQL_uname2": "",
    "SQL_Connection": "Driver={ODBC Driver 17 for SQL Server};"
                      "Server=sqlclusp1_fab.imec.be;"
                      "Database=DWH_PROD;"
                      "UID=_USER_;"
                      "PWD=_PWD_;",

    "SQL_Connection2": "Driver={ODBC Driver 17 for SQL Server};"
                       "Server=sqlclusp1_fab.imec.be;"
                       "Database=PMOPS;"
                       "UID=_USER_;"
                       "PWD=_PWD_;",
}

sql_queries = {
    "iio_raw_res": " SELECT [WBS], [Begin_Date] AS \'Begin\', [End_Date] AS \'End\', [RESDESCR] AS  \'Description\' ,[RES_TK]," \
              "[RES_TOOL] AS \'Tool\', LOWER([USER]) AS \'User_id\', [FACILITY],Replace(TRIM(Replace([RES_ITEM], [RES_TOOL], \'\')), \'-\', \'\') AS \'Module\'" \
              "\nFROM [fab].[W_STATUS_RESERVATION_RES] \nWHERE WBS <>\'\' AND WBS IS NOT NULL  \nAND [Begin_Date] >= \'%s\' and [Begin_Date] <= \'%s\' \nAND (STATUS = \'approved\' OR STATUS = \'request\') " \
              "\nAND (FACILITY = \'PLINE200\' OR FACILITY = \'PLINE300\')",
    
    "fab300_raw_res": ("SELECT [FO_ROW_ID], [UDA_NAME], [NEW_VALUE], [EVENT_ROW_ID], Replace([USER_ID], \'IMEC\', \'\') AS \'USER_ID\',"
                       "[DATE_TIME_STAMP] INTO #FAB300 FROM [DWH_PROD].[fab].[W_HIST_ATTR_ENTITY] WHERE [UDA_NAME] = \'ResTk\' OR [UDA_NAME] = \'ResWBS\'"
                        "\nSELECT [FO_ROW_ID], MAX([ResTk]) AS \'ResTk\' ,MAX([ResWBS]) AS \'ResWBS\',[EVENT_ROW_ID],[USER_ID],[DATE_TIME_STAMP]"
                       " INTO OUTPUT_fab300_raw_reservations FROM (SELECT [UDA_NAME], [NEW_VALUE], [FO_ROW_ID], [EVENT_ROW_ID], [USER_ID], [DATE_TIME_STAMP] "
                       "\nFROM #FAB300 WHERE   [DATE_TIME_STAMP] >= %s AND  [DATE_TIME_STAMP] < %s) AS SourceTable PIVOT "
                       "( MAX([NEW_VALUE] ) FOR [UDA_NAME] IN ([ResTk], [ResWBS])) AS PivotTable "
                       "GROUP BY [FO_ROW_ID],[EVENT_ROW_ID],[USER_ID],[DATE_TIME_STAMP]"),

    "tools_parents": "SELECT [DEL_FLAG], [FACILITY],[ENT_NAME],[ROW_ID],CONVERT(datetime2(0), REVERSE(PARSENAME(REPLACE(REVERSE([CSIM_TIMESTAMP]), '.', '.'), 1))) AS [CSIM_TIMESTAMP],"
                     "[Area] \nFROM [fab].[W_STATUS_ENTITY] \nWHERE ([FACILITY] = 'PLINE200' or [FACILITY] = 'PLINE300') AND (([PARENT_ENT] Is null) OR ([PARENT_ENT] = '')) AND DEL_FLAG = 'N'"

}


failure_mail_recipients = "maes57@imec.be"

SP_cfg = {
    "SharePoint_BaseSite": r"imecinternational.sharepoint.com",
    "SharePoint_TargetSite": r"/sites/Team-ToolData",
    "SharePoint_DriveName": r"Documents",
    "SharePoint_TrackPath": r"/Litho_Supplier_Dashboard/",

    "SharePoint_AccessAccount": "appm_con@imecinternational.onmicrosoft.com",
    "pwd": "sk6AH3G6x4aLkt2E",

    "AD_App_client_id": r"415650c1-1005-494c-b25d-08dc96807886",
    "AD_App_authority": r"https://login.microsoftonline.com/a72d5a72-25ee-40f0-9bd1-067cb5b770d4",
}

"""
"SharePoint_BaseSite": r"imecinternational.sharepoint.com",
"SharePoint_TargetSite": r"/sites/Team-ToolData",
"SharePoint_DriveName": r"Documents",
"SharePoint_TrackPath": r"/TEL_Tracks/",
"SharePoint_AccessAccount": "appm_con@imecinternational.onmicrosoft.com",
"pwd": "sk6AH3G6x4aLkt2E",
"""
fname_prefix = "OS"

current_moment = datetime.today()
current_year = current_moment.year  # we will use afterwards
current_day = datetime(current_year, current_moment.month, current_moment.day)

#localPath = "C:/Users/maes57/OneDrive - imec/Documents/Work/Python/Github/VM - LTs_to_ToolLogs - SAS to SQL Server/SOS TEST SOS/"

start_date_py = datetime(2022, 1, 1)
end_date_py = current_day
