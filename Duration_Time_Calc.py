import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import config as cfg
import IIO_Reservations as iio_res
import Fab300_reservations as f300_res
import Fab300_IIO_Combo as combo
import Tool_information as tools
from Support import save_file_to_SP_folder


"""
            SHAREPOINT PATH/FILE-NAME.csv
"""
iio_raw_csv = cfg.SP_cfg["SharePoint_TrackPath"] + "IIO_reservations.csv"
iio_without_modules_csv = cfg.SP_cfg["SharePoint_TrackPath"] + "IIO_without_modules.csv"
fab300_raw_csv = cfg.SP_cfg["SharePoint_TrackPath"] + "Fab300_reservations.csv"
fab300_reservations_processed_csv = cfg.SP_cfg["SharePoint_TrackPath"] + "Fab300_reservations_processed.csv"
Fab300_with_tool_names_csv = cfg.SP_cfg["SharePoint_TrackPath"] + "Fab300_with_tool_names.csv"
Fab300_IIO_overlaps_ids_csv = cfg.SP_cfg["SharePoint_TrackPath"] + "Fab300_IIO_overlaps_ids_names.csv"
Final_Fab300_IIO_reservations_csv = cfg.SP_cfg["SharePoint_TrackPath"] + "Final_Fab300_IIO_reservations.csv"

"""
            PROCESS IIO RESERVATIONS
"""
#
# ======= RAW IIO FROM DATABASE AND SAVE FILE TO SHAREPOINT
#
IIO_reservations = iio_res.get_iio_reservations(cfg)
save_file_to_SP_folder(iio_raw_csv, IIO_reservations.to_csv(index=False).encode())

#
# ======= IIO WITHOUT MODULES AND SAVE FILE TO SHAREPOINT
#
IIO_without_modules = iio_res.IIO_without_modules(IIO_reservations)
save_file_to_SP_folder(iio_without_modules_csv, IIO_without_modules.to_csv(index=False).encode())


"""
            PROCESS FAB300 RESERVATIONS
"""
fab300_reservations = f300_res.get_fab300_reservations(cfg)
save_file_to_SP_folder(fab300_raw_csv, fab300_reservations.to_csv(index=False).encode())

fab300_reservations_processed = f300_res.processFab300RawReservations(fab300_reservations)
save_file_to_SP_folder(fab300_reservations_processed_csv, fab300_reservations_processed.to_csv(index=False).encode())

tools_parents = tools.get_tools_parents_reservations(cfg)
tools_parents["CSIM_TIMESTAMP"] = pd.to_datetime(tools_parents["CSIM_TIMESTAMP"])

fab300_with_tool_names = f300_res.FAB300_with_tool_names(fab300_reservations_processed, tools_parents)
save_file_to_SP_folder(Fab300_with_tool_names_csv, fab300_with_tool_names.to_csv(index=False).encode())


"""
            PROCESS COMBO
"""
fab300_IIO_overlaps_ids = combo.getFab300_IIO_Combo(fab300_with_tool_names, IIO_without_modules)
save_file_to_SP_folder(Fab300_IIO_overlaps_ids_csv, fab300_IIO_overlaps_ids.to_csv(index=False).encode())

Final_Fab300_IIO_reservations = combo.Final_Fab300_IIO_reservations(IIO_without_modules, fab300_with_tool_names, fab300_IIO_overlaps_ids)
save_file_to_SP_folder(Final_Fab300_IIO_reservations_csv, Final_Fab300_IIO_reservations.to_csv(index=False).encode())

