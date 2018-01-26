import smtplib
import datetime
import os
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from cryptotrading.emailer_account_info import gmail_user, gmail_pwd, toaddres
from cryptotrading.logger_builder import logger

def send_email():
    fromaddr = gmail_user + '@gmail.com'
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo()
    server.starttls()
    server.login(gmail_user, gmail_pwd)

    # Create the enclosing (outer) message
    outer = MIMEMultipart()
    outer['Subject'] = 'Cryptotrader report for ' + datetime.date.today().strftime('%Y-%m-%d')
    outer['To'] = toaddres
    outer['From'] = fromaddr
    outer.preamble = 'You will not see this in a MIME-aware mail reader.\n'

    # Add the attachments to the message
    msg = None
    file = 'h:\\traderBot_log\\' + datetime.date.today().strftime('%Y-%m-%d.log')
    try:
        with open(file, 'rb') as fp:
            msg = MIMEBase('application', "octet-stream")
            msg.set_payload(fp.read())
        encoders.encode_base64(msg)
        msg.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file))
        outer.attach(msg)
    except:
        logger.info('Cannot load attachment.')
    composed = outer.as_string()

    # Send the email
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(gmail_user, gmail_pwd)
            s.sendmail(fromaddr, toaddres, composed)
            s.close()
        logger.info('Email sent!')
    except:
        logger.info('Unable to send email.')

    server.sendmail(fromaddr, toaddres, composed)
    server.quit()
