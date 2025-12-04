"""
### Airflow DAG for MySQL to CSV and Email (Direct Connections)

This DAG demonstrates a simple ETL pipeline:
1.  **Extract:** Fetches data from a MySQL database using a direct connection defined in the code.
2.  **Transform & Load (to CSV):** Saves the fetched data into a CSV file in a temporary location.
3.  **Distribute:** Sends the CSV file as an email attachment using a direct SMTP connection defined in the code.

"""
from __future__ import annotations

import pendulum
import pandas as pd
import os
import mysql.connector

# Imports for sending email manually
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


from airflow.decorators import dag, task
from airflow.models.param import Param



CSV_FILE_PATH = "/mnt/c/airflow/temp/leads_export.csv"

@dag(
    dag_id="new_report",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), 
    catchup=False, # If set to True,  Airflow will "catch up" and run any missed DAG runs from the start date until now.
                   # Since it's False, Airflow will only run the DAG from the current date forward (no backlog runs).
    schedule=None, # Set to None for manual runs, or use "@daily" etc.
    doc_md=__doc__, # This links the DAG documentation to the module-level docstring (i.e., __doc__).
    tags=["mysql", "email", "reporting", "direct-connection"], # Tags are used in the Airflow UI to categorize and filter DAGs for better searchability.
    params={ # This section allows you to pass runtime parameters to the DAG manually when triggering it from the Airflow UI or API.z
        "recipient_emails": Param(
            "your_email@example.com",
            type="string",
            title="Recipient Emails",
            description="Comma-separated list of emails to send the report to.",
        )
    }
)
def mysql_to_csv_and_email_dag():
    """
    ### MySQL to CSV and Email DAG with Direct Connections

    This DAG connects to MySQL and an SMTP server using credentials
    hardcoded in the tasks below.
    """

    @task
    def fetch_mysql_data_and_save_as_csv() -> str:
        """
        #### Fetch MySQL Data Task
        Connects to MySQL directly, executes a query, and saves the result to a CSV file.
        """
        # 1. Define your MySQL connection details here
        db_config = {
            'user': '`kunal`',
            'password': '********',
            'host': '***************************',
            'database': '***************',
            'port': ****
        }
        
        conn = None
        try:
            print("Establishing direct connection to MySQL...")
            conn = mysql.connector.connect(**db_config) # It create connection to a MySQL database.
            print("Connection successful.")

            sql_query = """
                SELECT pl.creator_id, 
                COUNT(CASE WHEN gvd.stage_1_completed_at IS NOT NULL THEN 1 END) AS stage_1_count, 
                COUNT(CASE WHEN gvd.stage_2_completed_at IS NOT NULL THEN 1 END) AS stage_2_count, 
                COUNT(CASE WHEN gvd.stage_3_completed_at IS NOT NULL THEN 1 END) AS stage_3_count 
                FROM process_leads pl LEFT JOIN gpay_visit_details gvd ON gvd.lead_id = pl.id 
                WHERE pl.pipeline_id = 33 AND (DATE(gvd.stage_2_completed_at) = '2025-08-04' OR DATE(gvd.stage_1_completed_at) = '2025-08-04' OR DATE(gvd.stage_3_completed_at) = '2025-08-04') 
                GROUP BY pl.creator_id ORDER BY pl.creator_id;
            """

            print("Executing MySQL query...")
            df = pd.read_sql(sql_query, conn)
            print(f"Successfully fetched {len(df)} rows from the database.")

        except mysql.connector.Error as err:
            print(f"Error connecting to MySQL: {err}")
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()
                print("MySQL connection is closed.")

        print(f"Saving data to CSV at: {CSV_FILE_PATH}")
        if os.path.exists(CSV_FILE_PATH):
            os.remove(CSV_FILE_PATH)
        
        df.to_csv(CSV_FILE_PATH, index=False)
        
        print("CSV file created successfully.")
        return CSV_FILE_PATH

    @task
    def send_email_with_attachment(file_path: str, **kwargs):
        """
        #### Send Email Task
        Connects to a custom SMTP server and sends the generated CSV as an attachment.
        """
        smtp_host = "********************************"
        smtp_port = ***  
        smtp_user = "********"
        smtp_password = "***********"

        dag_run = kwargs["dag_run"]
        recipient_emails = dag_run.conf.get("recipient_emails", "***************")
        
        # 2. Create the email message
        msg = MIMEMultipart()
        msg['From'] = "**********.netambit.co"
        msg['To'] = "******************"
        msg['Subject'] = f"Daily Leads Report - {kwargs['ds']}"

        # Email body
        body = f"""
        <h3>Daily Leads Report</h3>
        <p>
            Please find the attached CSV file with the leads data for {kwargs['ds']}.
        </p>
        <p>
            Regards,<br>
            Your Airflow Bot
        </p>
        """
        msg.attach(MIMEText(body, 'html'))

        # 3. Attach the file
        try:
            with open(file_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {os.path.basename(file_path)}",
            )
            msg.attach(part)
        except FileNotFoundError:
            print(f"Error: Attachment file not found at {file_path}")
            raise

        # 4. Connect to the SMTP server and send the email
        server = None
        try:
            print(f"Connecting to SMTP server at {smtp_host}:{smtp_port}...")
            # Use SMTP_SSL for port 465, or SMTP for port 587
            if smtp_port == 465:
                server = smtplib.SMTP_SSL(smtp_host, smtp_port)
            else:
                server = smtplib.SMTP(smtp_host, smtp_port)
                server.starttls() # Secure the connection
            
            server.login(smtp_user, smtp_password)
            print("SMTP login successful.")
            
            # The recipient list for sendmail should be a list of strings
            recipients_list = [email.strip() for email in recipient_emails.split(',')]
            server.sendmail("***********.netambit.co", recipients_list, msg.as_string())
            print(f"Email sent successfully to: {recipient_emails}")
        except Exception as e:
            print(f"Failed to send email: {e}")
            raise
        finally:
            if server:
                server.quit()
                print("SMTP server connection closed.")


    # Set task dependencies
    csv_file = fetch_mysql_data_and_save_as_csv()
    send_email_with_attachment(file_path=csv_file)


# Instantiate the DAG

mysql_to_csv_and_email_dag()
