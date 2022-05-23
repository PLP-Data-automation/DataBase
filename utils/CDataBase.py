"""
Author: Fuentes Juvera, Luis
E-mail: luis.fuju@outlook.com
username: LuisDFJ

CDataBase - Hig-Level abstraction for dumping dataframes
into a SQLite3 DataBase.

Classes
-------
CDataBase

"""
import pandas
import os
import sqlite3
import numpy as np

UNNAMED = 'unnammd'

class CDataBase():
    """
    CDataBase - Hig-Level abstraction for dumping dataframes
    into a SQLite3 DataBase.

    Attributes
    ----------
    table : pandas.DataFrame
        Table to dump.
    path : str
        Path to .db file.
    name : str
        Filename of .db file.
    tablename : str
        Schema in .db file.
    keycol : str
        Key Index column

    Methods
    -------
    writeDB() -> None
        Append new data to existing database.
    openDB() -> None
        Create new sqlite3 database and create cursor.
    closeDB() -> None
        Close when finished.
    createTable(name : str = "Data", key_col : str = "TimeInt") -> None
        Create new table with columns on current DataFrame.
    """
    def __init__( self, table : pandas.DataFrame, path : str, name : str = "HistTable.db", tablename : str = "Data", keycol : str = "TimeInt" ):
        """
        CDataBase - Hig-Level abstraction for dumping dataframes
        into a SQLite3 DataBase.

        Parameters
        ----------
        table : pandas.DataFrame
            Table to dump.
        path : str
            Path to .db file.
        name : str
            Filename of .db file.
        tablename : str
            Schema in .db file.
        keycol : str
            Key Index column
        """
        self.table = table
        self.path = os.path.join( path, name )
        self.tablename = tablename
        self.keycol = keycol
    
    def __enter__( self ):
        self.openDB()
        self.createTable( self.tablename, self.keycol )
        return self

    def __exit__( self, exc_type, exc_value , exc_traceback ):
        self.closeDB()

    def writeDB( self ) -> None:
        """
        Append new data to existing database.
        """
        self.table.to_sql( self.tablename, self.db, if_exists='append', index=False )

    def openDB( self ) -> None:
        """
        Create new sqlite3 database and create cursor.
        """
        self.db = sqlite3.connect( self.path )
        self.cursor = self.db.cursor(  )

    def closeDB( self ) -> None:
        """
        Close when finished.
        """
        self.db.close()

    def createTable( self, name : str = "Data", key_col : str = "TimeInt" ) -> None:
        """
        Create new table with columns on current DataFrame.
        """
        self.cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {name}
            ( { self.get_sql_cols( key_col ) } )
            """
        )
        self.db.commit()

    def get_sql_cols( self, key_col : str = "TimeInt" ):
        dtype_dict = dict( self.table.dtypes )
        sql_cols = []
        for key in dtype_dict.keys():
            if UNNAMED not in key.lower():
                type = self.to_sql_type( dtype_dict[ key ] )
                if key_col == key: sql_cols.append( f"\"{key}\" {type} NOT NULL PRIMARY KEY ON CONFLICT REPLACE" )
                else: sql_cols.append( f"\"{key}\" {type}" )
        return ",".join( sql_cols )

    def to_sql_type( self, dtype : np.dtype ):
        if dtype == np.dtype( 'int64' ):
            return "INTEGER"
        elif dtype == np.dtype( 'float64' ):
            return "REAL"
        if dtype == np.dtype( 'O' ):
            return "TEXT"
        else:
            return None

