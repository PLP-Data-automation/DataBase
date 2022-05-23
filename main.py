"""
Author: Fuentes Juvera, Luis
E-mail: luis.fuju@outlook.com
username: LuisDFJ

Example usage: Data logger using SQLite3

Opens a file explorer browser to select valid .csv files.
The function saves the logs into a .db file.

"""
from DataBase.DataLog import get_local_path, log_from_files
import os

LOCAL_PATH = get_local_path( __file__ )

if __name__ == "__main__":
    if not os.path.exists( "db" ):
        os.makedirs( "db" )
    log_from_files( LOCAL_PATH )
