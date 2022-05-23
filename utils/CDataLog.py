"""
Author: Fuentes Juvera, Luis
E-mail: luis.fuju@outlook.com
username: LuisDFJ

CDataLog - Hig-Level abstraction for dumping dataframes
into a SQLite3 DataBase.

Classes
-------
CDataLog

"""

from DataBase.utils.CDataBase import CDataBase
import pandas

class CDataLog():
    """
    CDataLog - Hig-Level abstraction for dumping dataframes
    into a SQLite3 DataBase.

    Attributes
    ----------
    df : pandas.DataFrame
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
    split_table() -> dict
        Decomposes the table in a dictionary per LEGEND.
    log_table() -> None
        Dumps dataframe to sqlite database.

    """
    def __init__( self, df : pandas.DataFrame, path : str, col : str = "LEGEND", name : str = "HistLog.db", keycol : str = "TimeInt" ):
        """
        CDataLog - Hig-Level abstraction for dumping dataframes
        into a SQLite3 DataBase.

        Parameters
        ----------
        df : pandas.DataFrame
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
        self.df     = df
        self.path   = path
        self.col    = col
        self.name   = name
        self.keycol = keycol
    
    def _to_valid_name( self, name : str ) -> str:
        res = ''
        for c in name:
            if c.isascii() and c.isalnum(): res += c
        return res

    def split_table( self ) -> dict:
        """
        Decomposes the table in a dictionary per LEGEND.
        """
        df_dict = {}
        for legend in self.df[self.col].unique():
            df_dict[ legend ] = self.df.loc[ self.df[self.col] == legend ]
        return df_dict

    def log_table( self ) -> None:
        """
        Dumps dataframe to sqlite database.
        """
        df_dict = self.split_table( )
        for device in list( df_dict.keys() ):
            vname = self._to_valid_name( device )
            with CDataBase( df_dict[device], self.path, self.name, vname, self.keycol ) as db:
                db.writeDB()




