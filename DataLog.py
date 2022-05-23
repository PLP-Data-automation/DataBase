"""
Author: Fuentes Juvera, Luis
E-mail: luis.fuju@outlook.com
username: LuisDFJ

DataLog - Hig-Level abstraction for dumping dataframes
into a SQLite3 DataBase.

Classes
-------
DataLog

Functions
---------
log_from_files( path : str, col : str = "LEGEND", name : str = "HistLog", keycol : str = "TimeInt" ) -> None
    Promps a dialog to select .csv files to dump into .db.
get_local_path( file : str = __file__ ) -> str
    Returns local file path.

"""
from DataBase.utils.CSaveForm import Dialog as openDialog
from DataBase.utils.CDataLog import CDataLog
import os
import pandas


class DataLog( CDataLog ):
    """
    CDataLog - Hig-Level abstraction for dumping dataframes
    into a SQLite3 DataBase.
    """
    def __init__(self, df: pandas.DataFrame, path: str, col: str = "LEGEND", name: str = "HistLog.db", keycol: str = "TimeInt"):
        super().__init__(df, path, col, name, keycol)

def log_from_files( path : str, col : str = "LEGEND", name : str = "HistLog", keycol : str = "TimeInt" ) -> None:
    """
    Promps a dialog to select .csv files to dump into .db.

    Parameters
    ----------
    path : str
        Path to .db file.
    name : str
        Filename of .db file.
    tablename : str
        Schema in .db file.
    keycol : str
        Key Index column

    """
    for file in openDialog( mode='file' ):
        basename = os.path.basename( file )
        postfix = ""
        if "full" in basename.lower():
            if "filter" in basename.lower():
                postfix = "full_filter"
            else:
                postfix = "full"
        elif "reduced" in basename.lower():
            if "filter" in basename.lower():
                postfix = "reduced_filter"
            else:
                postfix = "reduced"
        if postfix:
            df = pandas.read_csv( file )
            logger = DataLog( df, path, col, f"{name}_{postfix}.db", keycol )
            logger.log_table()
            
def get_local_path( file : str = __file__ ) -> str:
    """
    Returns local file path.

    Parameters
    ----------
    file : str
        Local file path, where file is __file__ of local script.

    Returns
    -------
    str
        Local Path.

    """
    return os.path.join( os.path.dirname( os.path.realpath( file ) ), "db" )