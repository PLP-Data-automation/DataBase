import pandas
import os
import sqlite3
import numpy as np


class CDataBase():
    def __init__( self, table : pandas.DataFrame, path : str, name : str = "HistTable.db", tablename : str = "Data", keycol : str = "TimeInt" ):
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

    def writeDB( self ):
        self.table.to_sql( self.tablename, self.db, if_exists='append', index=False )

    def openDB( self ):
        self.db = sqlite3.connect( self.path )
        self.cursor = self.db.cursor(  )

    def closeDB( self ):
        self.db.close()

    def createTable( self, name : str = "Data", key_col : str = "TimeInt" ):
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
            if "unnamed" not in key.lower():
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

