from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

class SQLConnection:
    def __init__(
        self,
        db_host,
        db_port,
        db_database,
        db_username,
        db_password,
        dialect="mysql",              # "mysql" | "mariadb" | "mssql" | "postgresql" ...
        driver="pymysql",             # for MSSQL use "pyodbc"
        ssh_tunnel=None,

        # MSSQL / pyodbc
        odbc_driver="ODBC Driver 18 for SQL Server",
        odbc_trust_server_cert=True,
        odbc_encrypt=None,            # None -> driver default, True -> yes, False -> no
        odbc_dsn=None,                # if using a DSN, put its name here
        odbc_params=None,             # dict of extra ODBC key=value pairs (only for odbc_connect mode)
        use_odbc_connect=False,       # build ?odbc_connect=... instead of URL with query=driver

        # general
        extra_query=None,             # dict -> URL query params
        connect_args=None             # dict -> SQLAlchemy create_engine(connect_args=...)
    ):
        self.db_host = db_host
        self.db_port = int(db_port) if db_port else None
        self.db_database = db_database
        self.db_username = db_username
        self.db_password = db_password
        self.dialect = dialect
        self.driver = driver

        self.ssh_tunnel = ssh_tunnel

        self.odbc_driver = odbc_driver
        self.odbc_trust_server_cert = odbc_trust_server_cert
        self.odbc_encrypt = odbc_encrypt
        self.odbc_dsn = odbc_dsn
        self.odbc_params = odbc_params or {}
        self.use_odbc_connect = use_odbc_connect

        self.extra_query = extra_query or {}
        self.connect_args = connect_args or {}

        self.engine = None
        self.session_factory = None

    # ---- internals ---------------------------------------------------------

    def _resolve_host_port(self):
        if self.ssh_tunnel and getattr(self.ssh_tunnel, "is_active", lambda: False)():
            return "127.0.0.1", int(self.ssh_tunnel.local_bind_port)
        return self.db_host, self.db_port

    def _build_mssql_pyodbc_url(self, host, port):
        """
        Build either:
          - URL with query driver (simple), or
          - odbc_connect string (full ODBC; more control).
        """
        drivername = "mssql+pyodbc"

        # Common ODBC options
        enc = None
        if self.odbc_encrypt is True:
            enc = "yes"
        elif self.odbc_encrypt is False:
            enc = "no"

        if self.use_odbc_connect:
            # Build a raw ODBC connection string, then percent-encode it
            import urllib.parse
            parts = []

            if self.odbc_dsn:
                parts.append(f"DSN={self.odbc_dsn}")
            else:
                parts.append(f"DRIVER={{{{}}}}".format(self.odbc_driver))  # {ODBC Driver 18 for SQL Server}
                server = host if not port else f"{host},{port}"
                parts.append(f"SERVER={server}")
                parts.append(f"DATABASE={self.db_database}")

            parts.append(f"UID={self.db_username}")
            parts.append(f"PWD={self.db_password}")

            if self.odbc_trust_server_cert:
                parts.append("TrustServerCertificate=yes")
            if enc is not None:
                parts.append(f"Encrypt={enc}")

            # Extra ODBC params
            for k, v in self.odbc_params.items():
                parts.append(f"{k}={v}")

            odbc_str = ";".join(parts)
            quoted = urllib.parse.quote_plus(odbc_str)
            # mssql+pyodbc:///?odbc_connect=<quoted string>
            return f"{drivername}:///?odbc_connect={quoted}"

        else:
            # Simple URL with ?driver= & flags
            q = dict(self.extra_query)
            q.setdefault("driver", self.odbc_driver)
            if self.odbc_trust_server_cert:
                q.setdefault("TrustServerCertificate", "yes")
            if enc is not None:
                q.setdefault("Encrypt", enc)

            if self.odbc_dsn:
                # connect using DSN
                return URL.create(
                    drivername=drivername,
                    username=self.db_username,
                    password=self.db_password,
                    query={**q, "dsn": self.odbc_dsn},
                )
            else:
                return URL.create(
                    drivername=drivername,
                    username=self.db_username,
                    password=self.db_password,
                    host=host,
                    port=port,
                    database=self.db_database,
                    query=q,
                )

    def _build_url(self, host, port):
        drivername = f"{self.dialect}+{self.driver}" if self.driver else self.dialect

        # MySQL/MariaDB via PyMySQL
        if self.dialect in ("mysql", "mariadb") and "pymysql" in (self.driver or ""):
            q = dict(self.extra_query)
            q.setdefault("charset", "utf8mb4")
            return URL.create(
                drivername=drivername,
                username=self.db_username,
                password=self.db_password,
                host=host,
                port=port,
                database=self.db_database,
                query=q,
            )

        # MSSQL via pyodbc
        if self.dialect == "mssql" and "pyodbc" in (self.driver or ""):
            return self._build_mssql_pyodbc_url(host, port)

        # Default (e.g., Postgres)
        return URL.create(
            drivername=drivername,
            username=self.db_username,
            password=self.db_password,
            host=host,
            port=port,
            database=self.db_database,
            query=self.extra_query,
        )

    # ---- public API ---------------------------------------------------------

    def connect(self, test_query="SELECT 1"):
        """Connect, (optionally) start SSH tunnel, and validate with SELECT 1."""
        # Start SSH tunnel if provided but not active
        if self.ssh_tunnel and not self.ssh_tunnel.is_active():
            self.ssh_tunnel.start()

        host, port = self._resolve_host_port()
        url = self._build_url(host, port)

        # Sensible defaults per dialect/driver
        if self.dialect == "mssql" and "pyodbc" in (self.driver or ""):
            # accelerate bulk inserts/updates
            self.connect_args.setdefault("fast_executemany", True)

        self.engine = create_engine(url, pool_pre_ping=True, connect_args=self.connect_args)

        # validate
        with self.engine.connect() as conn:
            conn.exec_driver_sql(test_query)

        self.session_factory = sessionmaker(bind=self.engine)
        return self

    def dispose(self, stop_ssh=False):
        if self.engine:
            self.engine.dispose(close=True)
            self.engine = None
        if stop_ssh and self.ssh_tunnel:
            try:
                self.ssh_tunnel.stop()
            except Exception:
                pass


    def get_session(self):
        if not self.session_factory:
            raise RuntimeError("Engine no inicializado. Llama primero a connect().")
        return self.session_factory()


    # Core database operations

    def execute_procedure_lastset(self, procedure_name, params=None):
        params = params or {}
        values = list(params.values())

        conn = self.engine.raw_connection()  # no es context manager
        cur = None
        try:
            cur = conn.cursor()
            placeholders = ",".join(["%s"] * len(values))
            sql = f"CALL {procedure_name}({placeholders})" if values else f"CALL {procedure_name}()"
            cur.execute(sql, values)

            last_rows, last_cols = [], None
            while True:
                if cur.description:                     # hay columnas => es un SELECT
                    rows = cur.fetchall()
                    last_rows = rows
                    last_cols = [d[0] for d in cur.description]
                # si no hay description es un set de DML (UPDATE/INSERT) sin columnas; se ignora
                if not cur.nextset():                   # no hay más sets
                    break

            conn.commit()
            return [dict(zip(last_cols, r)) for r in last_rows] if last_cols else []
        finally:
            try:
                if cur is not None:
                    cur.close()
            finally:
                conn.close()


            

    def bulk_upload_to_staging(self, df, table_name='ProductUpdateStaging'):
        """Bulk upload DataFrame to staging table"""
        r = 0
        try:
            with self.engine.begin() as conn:
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists='replace',
                    index=False
                )
            print(f"Data uploaded to staging table '{table_name}' successfully.")
        except Exception as e:
            print(f"Error uploading data to staging table '{table_name}': {e}")
            r = 1
        # Print the contents of the table after upload
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM {table_name}"))
                rows = result.fetchall()
                # print(f"Contents of '{table_name}':")
                # rows_df = pd.DataFrame(rows, columns=result.keys())
                # print(rows_df.columns)
        except Exception as e:
            r = 1
            print(f"Error fetching data from '{table_name}': {e}")
        return r

    # Specific procedure shortcuts
    def upsert_products_from_staging(self):
        """Execute bulk update from staging table"""
        r = self.execute_procedure_lastset('upsertProductsFromStaging')
        print("Products upserted from staging.", len(r))
 
        return r
    
    def upsert_brands_from_staging(self):
        """Execute bulk update from staging table"""
        s = self.execute_procedure_lastset('upsertBrandsFromStaging')
        print("Brands upserted from staging.", len(s))
        
        return s

    def get_products_info(self, active=True):
        """Get product information"""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("CALL getProductsInfo(:active)"),
                {'active': int(active)}
            )
            return result.fetchall()
    def get_table_info(self, table_name='', cols = "*" ,where = "1 = 1" ,test=False):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT {cols} FROM {table_name} WHERE {where}"))
                rows = result.fetchall()
                table_df = pd.DataFrame(rows, columns=result.keys())
                if test:
                    print(f"Contents of '{table_name}':")
                    print(table_df.columns)
                    print(table_df.head(5))
        except Exception as e:
            print(f"Error fetching data from '{table_name}': {e}")
        return table_df

    def upload_dataframe(self, df: pd.DataFrame, table_name, if_exists='replace', index=False):
        """Upload a DataFrame to a specified table"""
        import pandas as pd
        try:
            with self.engine.begin() as conn:
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists=if_exists,
                    index=index
                )
            print(f"Data uploaded to table '{table_name}' successfully.")
        except Exception as e:
            print(f"Error uploading data to table '{table_name}': {e}")
            raise
    def fech_dataframe(self, query):
        """Fetch data from a query into a DataFrame"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                rows = result.fetchall()
                df = pd.DataFrame(rows, columns=result.keys())
                return df
        except Exception as e:
            print(f"Error fetching data: {e}")
            raise
    
    def close(self):
        """Close database resources"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.session_factory = None

    # Context manager support
    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


