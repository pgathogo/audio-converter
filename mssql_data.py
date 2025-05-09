import winreg
import pyodbc

def read_registry()->dict:
    access_reg = winreg.ConnectRegistry(None, winreg.HKEY_LOCAL_MACHINE)
    access_key = winreg.OpenKey(access_reg, "SOFTWARE\Proxima\StudioONE\Data")
    conn_str = winreg.QueryValueEx(access_key, "ConnectionString")
    tokens = conn_str[0].split(";")
    for tok in tokens:
        if tok.strip().lower().startswith("password"):
            password = tok.split("=")[1]
        if tok.strip().lower().startswith("data"):
            server = tok.split("=")[1]
        if tok.strip().lower().startswith("initial"):
            database = tok.split("=")[1]
        if tok.strip().lower().startswith("user"):
            username = tok.split("=")[1]

    conn = {
        "server": server,
        "database": database,
        "username": username,
        "password": password
    } 
    return conn

class MSSQLData:
    def __init__(self, server, database, username, password):
        self._server = server      
        self._database = database  
        self._username = username  
        self._password = password  
        self._sql_driver ="{ODBC Driver 18 for SQL Server}"

        self.conn_str = (f"DRIVER={self._sql_driver};"
                        f"TrustServerCertificate=yes;"
                        f"SERVER={self._server};"
                        f"DATABASE={self._database};"
                        f"UID={self._username};"
                        f"PWD={self._password};"
                        )

        self.conn = None

    def database(self):
        return self._database
    
    def server(self):
        return self._server

    def connect(self):
        try:
            self.conn = pyodbc.connect(self.conn_str)
            return True
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Error connecting to database: {sqlstate}")
            return False

    def disconnect(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute_query(self, query: str):
        if not self.conn:
            if not self.connect():
                return None
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            return rows
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Error executing query: {sqlstate}")
            return None

    def execute_non_query(self, query):
        print(query)
        if not self.conn:
            if not self.connect():
                return False
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            self.conn.commit()
            return True
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"Error executing non-query: {sqlstate}")
            return False
            