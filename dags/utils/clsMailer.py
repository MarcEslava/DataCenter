import os, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

class Mailer:
    def __init__(self, smtp_server, smtp_port, username, password,
                 use_tls=True, use_auth=True, use_ssl=False,
                 force_envelope_from=True, debug=False):
        self.smtp_server = smtp_server
        self.smtp_port = int(smtp_port)
        self.username = username
        self.password = password
        self.use_tls = use_tls        # STARTTLS (usually port 587)
        self.use_ssl = use_ssl        # SSL-on-connect (usually port 465)
        self.use_auth = use_auth
        self.force_envelope_from = force_envelope_from
        self.debug = debug

    def send_mail(self, subject, body, from_addr, to_addrs,
                  html=False, attachments=None, attachments_io=None,
                  cc=None, bcc=None):
        # normalize recipients
        if isinstance(to_addrs, str):
            to_addrs = [to_addrs]
        cc = cc or []
        bcc = bcc or []
        all_rcpts = list(dict.fromkeys(to_addrs + cc + bcc))  # dedupe

        # build message
        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)
        if cc:
            msg["Cc"] = ", ".join(cc)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html" if html else "plain"))

        # file attachments
        if attachments:
            for file_path in attachments:
                if os.path.isfile(file_path):
                    with open(file_path, "rb") as f:
                        part = MIMEApplication(f.read(), Name=os.path.basename(file_path))
                    part["Content-Disposition"] = f'attachment; filename="{os.path.basename(file_path)}"'
                    msg.attach(part)

        # in-memory attachments
        if attachments_io:
            for filename, file_buffer in attachments_io:
                part = MIMEApplication(file_buffer.getvalue(), Name=filename)
                part["Content-Disposition"] = f'attachment; filename="{filename}"'
                msg.attach(part)

        # envelope-from (MAIL FROM) → safer to use the authenticated user
        envelope_from = self.username if (self.force_envelope_from and self.username) else from_addr

        # connect
        if self.use_ssl or self.smtp_port == 465:
            server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, timeout=30,
                                      context=ssl.create_default_context())
        else:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=30)

        try:
            if self.debug:
                server.set_debuglevel(1)

            # proper order: EHLO -> STARTTLS -> EHLO -> LOGIN
            server.ehlo()
            if self.use_tls and not isinstance(server, smtplib.SMTP_SSL):
                server.starttls(context=ssl.create_default_context())
                server.ehlo()

            if self.use_auth and self.username and self.password:
                server.login(self.username, self.password)

            server.sendmail(envelope_from, all_rcpts, msg.as_string())
        finally:
            server.quit()

# SMTP_SERVER = os.getenv("SMTP_SERVER")
# SMTP_PORT = int(os.getenv("SMTP_PORT"))
# SMTP_USERNAME = os.getenv("SMTP_USERNAME")
# SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
# SMTP_ADDRESS = os.getenv("SMTP_ADDRESS")

# mail = Mailer(smtp_server = SMTP_SERVER,
#     smtp_port = SMTP_PORT, username = SMTP_USERNAME, password = SMTP_PASSWORD, use_tls=True, use_auth=True, force_envelope_from=False, debug=False)
# mail.send_mail(
#     subject="Novetats Plataforma",
#     body=f"Hi ha un total de {len(new_prods)} nous productes",
#     from_addr=SSH_USERNAME,
#     to_addrs=[SMTP_ADDRESS],
#     html=False,
#     attachments_io=[(xlsx_name, excel_buffer)]
# )