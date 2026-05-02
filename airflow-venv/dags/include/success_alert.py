import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
import logging
import sys
sys.path.append('/home/taibat/airflow/airflow-venv/dags/include/')
import os

load_dotenv('/home/taibat/airflow/airflow-venv/dags/include/.env')

def success_alert(**context):
    
    host = os.getenv("SMTP_EMAIL")
    port = int(os.getenv("SMTP_PORT"))
    CLIENT_EMAIL = os.getenv("CLIENT_EMAIL")
    RECIEVER_EMAIL = os.getenv("RECIEVER_EMAIL")
    CLIENT_PASS = os.getenv("CLIENT_PASS")
    ti = context["ti"]
    task_id = ti.task_id
    owner = context["dag"].owner
    dag_id = ti.dag_id

    try:
        with smtplib.SMTP(host, port) as server:
            server.starttls()
            server.login(CLIENT_EMAIL, CLIENT_PASS)
            msg = MIMEMultipart('alternative')
            msg['From'] = CLIENT_EMAIL
            msg['To'] = RECIEVER_EMAIL
            msg['Subject'] = f"DAG Successfull. DAG ID - {dag_id}"
            text = f"""
                Hello {owner},

                Your dag with DAG_ID:{dag_id} is completed successfully

                Regards,
                """

            html = f"""
                            <html>
                            <body>
                            <p>Hello <strong>{owner}</strong>,</p>

                            <p> Your dag with DAG_ID: {dag_id} is completed successfully.</p>

                            <p>

                            <p>Regards,<p>
                            </body>
                            </html>
                            """
            text_message = MIMEText(text, 'plain')
            html_message = MIMEText(html, 'html')
            msg.attach(text_message)
            msg.attach(html_message)
            
            server.sendmail(CLIENT_EMAIL, RECIEVER_EMAIL, msg.as_string())
    except Exception as e:
        logging.error(f"Connection failed - {e}")