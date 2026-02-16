import sshtunnel
import paramiko
from io import StringIO

class SSHTunnel:
    def __init__(self, ssh_host, ssh_port, ssh_username, 
                 ssh_password=None, ssh_private_key=None,
                 ssh_private_key_password=None, remote_host='localhost', remote_port=3306):

        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.ssh_private_key = ssh_private_key
        self.ssh_private_key_password = ssh_private_key_password
        self.tunnel = None
        self.local_bind_port = None
        self.remote_host = remote_host
        self.remote_port = remote_port
        
    def start(self):
        if self.tunnel and self.tunnel.is_active:
            return self.local_bind_port

        self.tunnel = sshtunnel.SSHTunnelForwarder(
            ssh_address_or_host=(self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_username,
            ssh_password=self.ssh_password,
            ssh_pkey=self._load_private_key(),
            remote_bind_address=(self.remote_host, self.remote_port), 
            local_bind_address=('127.0.0.1', 0)              
        )
        self.tunnel.start()
        self.local_bind_port = self.tunnel.local_bind_port
        return self.local_bind_port


    def _load_private_key(self):
        """Parse SSH private key if provided"""
        if not self.ssh_private_key:
            return None
            
        try:
            key_file = StringIO(self.ssh_private_key)
            for key_class in (paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key):
                try:
                    key_file.seek(0)
                    return key_class.from_private_key(
                        key_file, 
                        password=self.ssh_private_key_password
                    )
                except (paramiko.SSHException, paramiko.PasswordRequiredException):
                    continue
            # Try generic if specific classes didn't work
            key_file.seek(0)
            return paramiko.RSAKey.from_private_key(
                key_file,
                password=self.ssh_private_key_password
            )
        except Exception as e:
            raise ValueError(f"SSH key error: {str(e)}")

    def stop(self):
        """Stop the SSH tunnel"""
        if self.tunnel:
            self.tunnel.stop()
            self.tunnel = None
            self.local_bind_port = None

    def is_active(self):
        """Check if tunnel is active"""
        return self.tunnel and self.tunnel.is_active

    # Context manager support
    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()


