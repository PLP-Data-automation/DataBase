import pandas
from utils.CDataBase import CDataBase
import os


LOCAL_PATH = os.path.join( os.path.dirname( os.path.realpath( __file__ ) ), "db" )

df = pandas.read_csv( "TORCEDORAS_SIMPLE.csv" )
df_2 = df.loc[ df["LEGEND"] == "TORCEDORA+2" ]
df_4 = df.loc[ df["LEGEND"] == "TORCEDORA+4" ]

with CDataBase( df_2, LOCAL_PATH, tablename="TORCEDORA2" ) as db:
    db.writeDB()

with CDataBase( df_4, LOCAL_PATH, tablename="TORCEDORA4" ) as db:
    db.writeDB()
