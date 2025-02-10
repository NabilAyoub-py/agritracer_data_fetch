import requests
import pyodbc
import time
from datetime import datetime, date
import logging
from dotenv import load_dotenv
import os
import argparse
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    filename='harvest_data_sync.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Database configuration from environment variables
DB_CONFIG = {
    'server': os.getenv('DB_SERVER'),
    'database': os.getenv('DB_NAME'),
    'trusted_connection': os.getenv('DB_TRUSTED_CONNECTION', 'yes'),
    'uid': os.getenv('DB_USERNAME'),  # Will be None if not set
    'pwd': os.getenv('DB_PASSWORD')   # Will be None if not set
}

# API configuration from environment variables
API_BASE_URL = os.getenv('API_BASE_URL', 'https://run-api-bi-neo-23393472851.us-central1.run.app')
API_KEY = os.getenv('API_KEY', 'SN1re5a4#1sd$Q6ARTvPpd<Zop*ObkSN1rPpf')

# Email configuration from environment variables
EMAIL_SENDER = os.getenv('EMAIL_SENDER')
EMAIL_PASSWORD = os.getenv('EMAIL_APP_PASSWORD')  # Gmail App Password
EMAIL_RECIPIENT = os.getenv('EMAIL_RECIPIENT')
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587

def get_database_connection():
    """Establish database connection"""
    try:
        conn_str = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        logging.error(f"Database connection error: {str(e)}")
        raise

def fetch_api_data(start_date, end_date):
    """Fetch data from API for a date range"""
    try:
        print(f"Fetching data from API for period {start_date} to {end_date}...")
        params = {
            'type': 'harvest',
            'dateStart': start_date.strftime('%Y-%m-%d'),
            'dateEnd': end_date.strftime('%Y-%m-%d')
        }
        
        headers = {
            'Authorization': f'Bearer {API_KEY}',
            'x-env': 'prod',
            'Empresa': 'magopco'
        }
        
        url = f"{API_BASE_URL}/api/magopco/get-bi-produccion"
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        print(f"✓ Successfully fetched {len(data)} records from API")
        return data
    except Exception as e:
        logging.error(f"API fetch error: {str(e)}")
        print(f"✗ Error fetching data from API: {str(e)}")
        raise

def insert_data(conn, data):
    """Insert or update data in database"""
    cursor = conn.cursor()
    total_records = len(data)
    try:
        print(f"\nProcessing {total_records} records...")
        for index, record in enumerate(data, 1):
            if index % 10 == 0:  # Show progress every 10 records
                progress = (index / total_records) * 100
                print(f"Progress: {progress:.1f}% ({index}/{total_records} records)", end='\r')
            
            sql = """
            MERGE harvests AS target
            USING (SELECT ? as id, ? as farm, ? as plot, ? as produce, 
                          ? as worker, ? as unit, ? as harvest_date,
                          ? as start_time, ? as end_time, ? as duration,
                          ? as containers, ? as kgs_harvested) AS source
            ON target.id = source.id
            WHEN MATCHED THEN
                UPDATE SET 
                    farm = source.farm,
                    plot = source.plot,
                    produce = source.produce,
                    worker = source.worker,
                    unit = source.unit,
                    harvest_date = source.harvest_date,
                    start_time = source.start_time,
                    end_time = source.end_time,
                    duration = source.duration,
                    containers = source.containers,
                    kgs_harvested = source.kgs_harvested,
                    insert_date = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (id, farm, plot, produce, worker, unit, 
                       harvest_date, start_time, end_time, 
                       duration, containers, kgs_harvested, insert_date)
                VALUES (source.id, source.farm, source.plot, source.produce,
                       source.worker, source.unit, source.harvest_date,
                       source.start_time, source.end_time, source.duration,
                       source.containers, source.kgs_harvested, GETDATE());
            """
            
            # Convert string dates/times to proper format
            harvest_date = datetime.strptime(record['harvest_date'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            start_time = datetime.strptime(record['start_time'], '%H:%M').strftime('%H:%M:%S')
            end_time = datetime.strptime(record['end_time'], '%H:%M').strftime('%H:%M:%S')
            
            cursor.execute(sql, (
                record['id'],
                record['farm'],
                record['plot'],
                record['produce'],
                record['worker'],
                record['unit'],
                harvest_date,
                start_time,
                end_time,
                record['duration'],
                record['containers'],
                record['kgs_harvested']
            ))
        
        print("\nCommitting changes to database...")  # New line to clear the progress line
        conn.commit()
        print("✓ Database update completed successfully")
        logging.info(f"Successfully processed {total_records} records")
    except Exception as e:
        conn.rollback()
        logging.error(f"Data insertion error: {str(e)}")
        raise
    finally:
        cursor.close()

def send_email_notification(success, start_date, end_date, error_message=None, records_processed=0):
    """Send email notification about sync status"""
    if not all([EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENT]):
        logging.warning("Email configuration missing. Skipping email notification.")
        return

    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_SENDER
        msg['To'] = EMAIL_RECIPIENT

        if success:
            msg['Subject'] = f"✅ AGRITRACER -Data Sync Success - {start_date} to {end_date}"
            body = f"""
            Data synchronization completed successfully!
            
            Period: {start_date} to {end_date}
            Records processed: {records_processed}
            
            This is an automated message.
            """
        else:
            msg['Subject'] = f"❌ AGRITRACER - Data Sync Error - {start_date} to {end_date}"
            body = f"""
            Data synchronization failed!
            
            Period: {start_date} to {end_date}
            Error: {error_message}
            
            Please check the logs for more details.
            This is an automated message.
            """

        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)

        logging.info("Email notification sent successfully")
    except Exception as e:
        logging.error(f"Failed to send email notification: {str(e)}")

def sync_data(start_date=None, end_date=None):
    """Main function to sync data with optional date range"""
    records_processed = 0
    try:
        # If no dates provided, use today's date
        if start_date is None:
            start_date = date.today()
        if end_date is None:
            end_date = date.today()
            
        print("\n=== Starting Data Sync Process ===")
        print(f"Date range: {start_date} to {end_date}")
        
        # Fetch data from API
        data = fetch_api_data(start_date, end_date)
        records_processed = len(data)
        
        # Connect to database
        print("\nConnecting to database...")
        with get_database_connection() as conn:
            print("✓ Database connection established")
            # Insert data
            insert_data(conn, data)
            
        print("\n=== Data Sync Process Completed ===")
        logging.info(f"Data sync completed successfully for period {start_date} to {end_date}")
        
        # Send success email
        send_email_notification(
            success=True,
            start_date=start_date,
            end_date=end_date,
            records_processed=records_processed
        )
    except Exception as e:
        error_message = str(e)
        print(f"\n✗ Error: Data sync failed: {error_message}")
        logging.error(f"Data sync failed: {error_message}")
        
        # Send error email
        send_email_notification(
            success=False,
            start_date=start_date,
            end_date=end_date,
            error_message=error_message,
            records_processed=records_processed
        )

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Sync harvest data for a specific date range')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)', default=None)
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)', default=None)
    
    args = parser.parse_args()
    
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date() if args.start_date else None
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date() if args.end_date else None
        
        sync_data(start_date, end_date)
    except ValueError as e:
        logging.error(f"Invalid date format. Please use YYYY-MM-DD. Error: {str(e)}")
        print("Invalid date format. Please use YYYY-MM-DD")
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()
