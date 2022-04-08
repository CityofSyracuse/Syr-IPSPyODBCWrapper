import pyodbc
import pandas as pd

class SQLQuerier(object):
    def __init__(self, conn):
        self.conn = conn


    def df_from_query(self, q):

        return pd.read_sql(q, self.conn)