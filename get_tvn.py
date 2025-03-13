import pandas as pd
import pyodbc
import logging
from datetime import datetime, date
import os
from dotenv import load_dotenv
from supabase import create_client
import argparse

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    filename='tvn_data_sync.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Database configuration from environment variables
DB_CONFIG = {
    'server': os.getenv('DB_SERVER'),
    'database': os.getenv('DB_NAME'),
    'trusted_connection': os.getenv('DB_TRUSTED_CONNECTION', 'yes'),
    'uid': os.getenv('DB_USERNAME'),
    'pwd': os.getenv('DB_PASSWORD')
}

# Supabase configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
SUPABASE_TABLE = os.getenv('SUPABASE_TABLE', 'tracefruit_harvest')  # Your table name

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

def get_supabase_data(start_date=None, end_date=None):
    """Fetch data from Supabase for a date range"""
    try:
        print(f"Fetching data from Supabase for period {start_date} to {end_date}...")
        
        # Initialize Supabase client
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Build the query
        query = supabase.table(SUPABASE_TABLE).select(
            'date',
            'kilos_harvested',
            'kilos_packed'
        )
        
        # Add date filters if provided
        if start_date:
            query = query.gte('date', start_date.strftime('%Y-%m-%d'))
        if end_date:
            query = query.lte('date', end_date.strftime('%Y-%m-%d'))
            
        # Execute the query
        print("Executing Supabase query...")
        response = query.execute()
        
        # Debug response
        print("\nResponse data:", response.data)
        
        if not response.data:
            print("No data returned from Supabase")
            return pd.DataFrame(columns=['date', 'kgs_harvest_tvn', 'kgs_packed_cnd'])
        
        # Convert to DataFrame
        print("\nConverting to DataFrame...")
        df = pd.DataFrame(response.data)
        print("Raw DataFrame columns:", df.columns.tolist())
        
        # Check if required columns exist
        required_columns = ['date', 'kilos_harvested', 'kilos_packed']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in Supabase response: {missing_columns}")
        
        # Rename columns to match SQL table structure
        df = df.rename(columns={
            'kilos_harvested': 'kgs_harvest_tvn',
            'kilos_packed': 'kgs_packed_cnd'
        })
        
        # Convert date column to datetime
        print("\nConverting date column...")
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        print(f"\n✓ Successfully fetched {len(df)} records from Supabase")
        print("\nFinal DataFrame columns:", df.columns.tolist())
        print("\nFirst few rows:")
        print(df.head())
        
        return df
    except Exception as e:
        logging.error(f"Supabase data fetch error: {str(e)}")
        print(f"✗ Error fetching data from Supabase: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print("Full traceback:")
        print(traceback.format_exc())
        raise

def insert_data(conn, df):
    """Insert or update data in database"""
    cursor = conn.cursor()
    total_records = len(df)
    try:
        print(f"\nProcessing {total_records} records...")
        for index, row in df.iterrows():
            if index % 10 == 0:  # Show progress every 10 records
                progress = (index / total_records) * 100
                print(f"Progress: {progress:.1f}% ({index}/{total_records} records)", end='\r')
            
            # Format date as string
            date_str = row['date'].strftime('%Y-%m-%d')
            
            sql = f"""
            MERGE tracefruit_harvest AS target
            USING (SELECT '{date_str}' as date, ? as kgs_harvest_tvn, 
                          ? as kgs_packed_cnd) AS source
            ON target.date = source.date
            WHEN MATCHED THEN
                UPDATE SET 
                    kgs_harvest_tvn = source.kgs_harvest_tvn,
                    kgs_packed_cnd = source.kgs_packed_cnd
            WHEN NOT MATCHED THEN
                INSERT (date, kgs_harvest_tvn, kgs_packed_cnd)
                VALUES (source.date, source.kgs_harvest_tvn, source.kgs_packed_cnd);
            """
            
            cursor.execute(sql, (
                row['kgs_harvest_tvn'],
                row['kgs_packed_cnd']
            ))
        
        print("\nCommitting changes to database...")
        conn.commit()
        print("✓ Database update completed successfully")
        logging.info(f"Successfully processed {total_records} records")
    except Exception as e:
        conn.rollback()
        logging.error(f"Data insertion error: {str(e)}")
        raise
    finally:
        cursor.close()

def sync_data(start_date=None, end_date=None):
    """Main function to sync data with optional date range"""
    try:
        # If no dates provided, use today's date
        if start_date is None:
            start_date = date.today()
        if end_date is None:
            end_date = date.today()
            
        print("\n=== Starting TVN Data Sync Process ===")
        print(f"Date range: {start_date} to {end_date}")
        
        # Fetch data from Supabase
        df = get_supabase_data(start_date, end_date)
        
        # Connect to database and insert data
        print("\nConnecting to database...")
        with get_database_connection() as conn:
            print("✓ Database connection established")
            insert_data(conn, df)
            
        print("\n=== TVN Data Sync Process Completed ===")
        logging.info(f"TVN data sync completed successfully for period {start_date} to {end_date}")
        
    except Exception as e:
        error_message = str(e)
        print(f"\n✗ Error: TVN data sync failed: {error_message}")
        logging.error(f"TVN data sync failed: {error_message}")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Sync TVN data for a specific date range')
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
