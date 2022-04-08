import pyodbc

class SQLConnector(object):
    def __init__(self, host_ip, db, user, pwd):
        """Class that connects to a database using pyodbc.
        Contains a few helper functions to maintain that."""
        self.host_ip = host_ip
        self.db = db
        self.user = user
        self.pwd = pwd
        self._conn = pyodbc.connect(f'driver={{SQL Server}};SERVER={self.host_ip};DATABASE={self.db};UID={self.user};PWD={self.pwd}')


    def get_connection(self):
        return self._conn