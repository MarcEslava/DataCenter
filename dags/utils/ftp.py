"""
FTP connection module.

Supports FTP and SFTP connections via:
1. Direct credentials
2. Airflow connection ID

Usage:
    from utils.ftp import FTPConn

    with FTPConn(host="ftp.example.com", user="admin", password="secret") as ftp:
        ftp.upload("/local/file.csv", "/remote/path/file.csv")
        ftp.upload_bytes(b"content", "/remote/path/file.txt")
        ftp.upload_df(df, "/remote/path/data.csv")
        files = ftp.list("/remote/path/")
        ftp.download("/remote/path/file.csv", "/local/file.csv")

    # From Airflow connection
    with FTPConn.from_airflow("ftp_default") as ftp:
        ftp.upload("/local/file.csv", "/remote/file.csv")
"""

from typing import List, Optional
import pandas as pd
import io



class FTPConn:
    """
    Context manager for FTP/SFTP connections.

    Set protocol='sftp' for SFTP connections (uses paramiko).
    Set protocol='ftp' for plain FTP connections (uses ftplib).
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: Optional[str] = None,
        port: Optional[int] = None,
        protocol: str = "sftp",
        ssh_key: Optional[str] = None,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.port = port or (22 if protocol == "sftp" else 21)
        self.protocol = protocol
        self.ssh_key = ssh_key
        self._conn = None

    @classmethod
    def from_airflow(cls, conn_id: str):
        """Create FTPConn from an Airflow connection ID."""
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(conn_id)
        extra = conn.extra_dejson
        return cls(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            port=conn.port,
            protocol=extra.get("protocol", "sftp"),
            ssh_key=extra.get("key_file"),
        )

    def _connect_sftp(self):
        """Open SFTP connection."""
        import paramiko
        print(f"[FTP DEBUG] Opening SFTP transport to {self.host}:{self.port}")
        transport = paramiko.Transport((self.host, self.port))
        if self.ssh_key:
            print(f"[FTP DEBUG] Authenticating with SSH key: {self.ssh_key}")
            pkey = paramiko.RSAKey.from_private_key_file(self.ssh_key)
            transport.connect(username=self.user, pkey=pkey)
        else:
            print(f"[FTP DEBUG] Authenticating with password for user: {self.user}")
            transport.connect(username=self.user, password=self.password)
        self._transport = transport
        print(f"[FTP DEBUG] SFTP connection established")
        return paramiko.SFTPClient.from_transport(transport)

    def _connect_ftp(self):
        """Open plain FTP connection."""
        from ftplib import FTP
        print(f"[FTP DEBUG] Opening FTP connection to {self.host}:{self.port}")
        ftp = FTP()
        ftp.connect(self.host, self.port)
        print(f"[FTP DEBUG] Logging in as user: {self.user}")
        ftp.login(self.user, self.password)
        print(f"[FTP DEBUG] FTP connection established")
        return ftp

    def __enter__(self):
        print(f"[FTP DEBUG] __enter__ protocol={self.protocol} host={self.host}:{self.port}")
        if self.protocol == "sftp":
            self._conn = self._connect_sftp()
        else:
            self._conn = self._connect_ftp()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"[FTP DEBUG] __exit__ closing connection to {self.host}")
        if exc_type:
            print(f"[FTP DEBUG] Exception during context: {exc_type.__name__}: {exc_val}")
        if self._conn:
            self._conn.close()
        if self.protocol == "sftp" and hasattr(self, '_transport'):
            self._transport.close()
        print(f"[FTP DEBUG] Connection closed")
        return False

    def upload(self, local_path: str, remote_path: str) -> None:
        """Upload a local file to remote path."""
        print(f"[FTP DEBUG] upload() local={local_path} remote={remote_path} protocol={self.protocol}")
        if self.protocol == "sftp":
            self._conn.put(local_path, remote_path)
        else:
            with open(local_path, 'rb') as f:
                self._conn.storbinary(f"STOR {remote_path}", f)
        print(f"[FTP DEBUG] upload() done: {local_path} -> {remote_path}")

    def upload_bytes(self, data: bytes, remote_path: str) -> None:
        """Upload raw bytes to remote path."""
        print(f"[FTP DEBUG] upload_bytes() size={len(data)} bytes remote={remote_path}")
        buf = io.BytesIO(data)
        if self.protocol == "sftp":
            self._conn.putfo(buf, remote_path)
        else:
            self._conn.storbinary(f"STOR {remote_path}", buf)
        print(f"[FTP DEBUG] upload_bytes() done: {len(data)} bytes -> {remote_path}")

    def upload_df(self, df: pd.DataFrame, remote_path: str, sep: str = ",") -> None:
        """Upload DataFrame as CSV to remote path."""
        print(f"[FTP DEBUG] upload_df() rows={len(df)} cols={list(df.columns)} remote={remote_path}")
        csv_bytes = df.to_csv(index=False, sep=sep).encode("utf-8")
        print(f"[FTP DEBUG] upload_df() CSV size={len(csv_bytes)} bytes")
        self.upload_bytes(csv_bytes, remote_path)

    def download(self, remote_path: str, local_path: str) -> None:
        """Download a remote file to local path."""
        print(f"[FTP DEBUG] download() remote={remote_path} local={local_path} protocol={self.protocol}")
        if self.protocol == "sftp":
            self._conn.get(remote_path, local_path)
        else:
            with open(local_path, 'wb') as f:
                self._conn.retrbinary(f"RETR {remote_path}", f.write)
        print(f"[FTP DEBUG] download() done: {remote_path} -> {local_path}")

    def download_bytes(self, remote_path: str) -> bytes:
        """Download a remote file as bytes."""
        print(f"[FTP DEBUG] download_bytes() remote={remote_path}")
        buf = io.BytesIO()
        if self.protocol == "sftp":
            self._conn.getfo(remote_path, buf)
        else:
            self._conn.retrbinary(f"RETR {remote_path}", buf.write)
        data = buf.getvalue()
        print(f"[FTP DEBUG] download_bytes() done: {len(data)} bytes from {remote_path}")
        return data

    def download_df(self, remote_path: str, sep: str = ",", header: int = 0) -> pd.DataFrame:
        """Download a remote file as DataFrame. Supports CSV and Excel (.xlsx/.xls)."""
        is_excel = remote_path.lower().endswith(('.xlsx', '.xls'))
        print(f"[FTP DEBUG] download_df() remote={remote_path} header={header} is_excel={is_excel}")
        data = self.download_bytes(remote_path)
        if is_excel:
            df = pd.read_excel(io.BytesIO(data), header=header)
        else:
            df = pd.read_csv(io.BytesIO(data), header=header, sep=sep)
        print(f"[FTP DEBUG] download_df() done: {len(df)} rows, columns={list(df.columns)}")
        return df

    def download_matching(self, remote_dir: str, prefix: str, sep: str = ",") -> List[tuple]:
        """Download all files in remote_dir starting with prefix. Returns [(filename, DataFrame), ...]."""
        print(f"[FTP DEBUG] download_matching() dir={remote_dir} prefix={prefix}")
        files = self.list(remote_dir)
        print(f"[FTP DEBUG] download_matching() found {len(files)} total files in {remote_dir}")
        # nlst may return full paths or just filenames
        results = []
        for f in files:
            basename = f.rsplit("/", 1)[-1] if "/" in f else f
            if basename.upper().startswith(prefix.upper()):
                path = f"{remote_dir}/{basename}"
                print(f"[FTP DEBUG] download_matching() downloading: {path}")
                df = self.download_df(path, sep=sep)
                results.append((basename, df))
                print(f"[FTP DEBUG] download_matching() {basename}: {len(df)} rows")
            else:
                print(f"[FTP DEBUG] download_matching() skipping: {basename} (no prefix match)")
        print(f"[FTP DEBUG] download_matching() done: {len(results)} files matched")
        return results

    def list(self, remote_path: str = ".") -> List[str]:
        """List files in remote directory."""
        print(f"[FTP DEBUG] list() remote_path={remote_path} protocol={self.protocol}")
        if self.protocol == "sftp":
            files = self._conn.listdir(remote_path)
        else:
            files = self._conn.nlst(remote_path)
        print(f"[FTP DEBUG] list() found {len(files)} items: {files}")
        return files

    def mkdir(self, remote_path: str) -> None:
        """Create remote directory."""
        print(f"[FTP DEBUG] mkdir() remote_path={remote_path}")
        if self.protocol == "sftp":
            self._conn.mkdir(remote_path)
        else:
            self._conn.mkd(remote_path)
        print(f"[FTP DEBUG] mkdir() done: {remote_path}")

    def remove(self, remote_path: str) -> None:
        """Delete a remote file."""
        print(f"[FTP DEBUG] remove() remote_path={remote_path}")
        if self.protocol == "sftp":
            self._conn.remove(remote_path)
        else:
            self._conn.delete(remote_path)
        print(f"[FTP DEBUG] remove() done: {remote_path}")
