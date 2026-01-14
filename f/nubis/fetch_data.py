import wmill
from wmill import set_progress
import json
import requests
import io
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import TypedDict
from datetime import datetime
import time
import csv

class postgresql(TypedDict):
    host: str
    port: int
    user: str
    dbname: str
    sslmode: str
    password: str
    root_certificate_pem: str

def log_with_timestamp(message):
    """Helper function to print messages with timestamps"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def make_request_with_retry(session, url, max_retries, timeout_tuple, file_name, request_type="GET", data=None):
    """Helper function to make HTTP requests with retry logic for server timeouts"""
    for attempt in range(max_retries + 1):  # 0 to max_retries (inclusive)
        try:
            if request_type == "POST":
                response = session.post(url, data=data, timeout=timeout_tuple)
            else:
                response = session.get(url, timeout=timeout_tuple)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            # Check if it's a 504 Gateway Timeout
            if e.response.status_code == 504 and attempt < max_retries:
                wait_time = (2 ** attempt) * 60  # Exponential backoff: 1min, 2min, 4min
                log_with_timestamp(f"  Server timeout (504) for {file_name}, attempt {attempt + 1}/{max_retries + 1}")
                log_with_timestamp(f"  Waiting {wait_time // 60} minutes before retry...")
                time.sleep(wait_time)
                continue
            else:
                # Re-raise for other HTTP errors or if max retries exceeded
                raise e
        except Exception as e:
            # For non-HTTP errors (connection issues, etc.), don't retry
            raise e

def main(db_creds:postgresql, stop_on_error: bool = False, request_timeout_minutes: int = 20, max_retries: int = 3):
    # Check if database credentials are provided
    if not db_creds:
        raise Exception("No database credentials provided. Please configure the database credentials parameter.")

    # Validate required database credential fields
    required_fields = ['host', 'port', 'user', 'password', 'dbname']
    missing_fields = [field for field in required_fields if not db_creds.get(field)]
    if missing_fields:
        raise Exception(f"Missing required database credential fields: {', '.join(missing_fields)}")

    log_with_timestamp("Database credentials validated successfully")

    # Calculate request timeout in seconds and create timeout tuple
    # (connect_timeout, read_timeout) - use 30s for connection, configurable for read
    request_timeout_seconds = request_timeout_minutes * 60
    timeout_tuple = (30, request_timeout_seconds)
    log_with_timestamp(f"Request timeout set to {request_timeout_minutes} minutes ({request_timeout_seconds} seconds)")
    log_with_timestamp(f"Max retries for server timeouts: {max_retries}")

    # Get credentials from Windmill variable
    creds = json.loads(wmill.get_variable("u/admin/suhail_nubis"))

    # URLs to download
    urls_to_download = [
        ("crosslink_respondants","https://survey.cesrusc.org/bol/index.php?r=eNpLtDKyqi62MrFSKkhMT1WyLrYyMrRSKkotTk0sSs5ILdJLSSxJ1MsvATJBkkB1JZUFYHVAZUZK1rVcMKl2FGE,"),
        # no entires ("contacts","https://survey.cesrusc.org/bol/index.php?r=eNpLtDKyqi62MrFSKkhMT1WyLrYyMrRSKkotTk0sSs5ILdJLSSxJ1MsvATJBkkB1JZUFYHVAZcZK1rVcMKl6FGI,"),
        # no entries("remarks","https://survey.cesrusc.org/bol/index.php?r=eNpLtDKyqi62MrFSKkhMT1WyLrYyMrRSKkotTk0sSs5ILdJLSSxJ1MsvATJBkkB1JZUFYHVAZaZK1rVcMKl-FGM,"),
        ("timings","https://survey.cesrusc.org/bol/index.php?r=eNpLtDKyqi62MrFSKkhMT1WyLrYyMrRSKkotTk0sSs5ILdJLSSxJ1MsvATJBkkB1JZUFYHVAZaZK1rVcMKmCFGQ,"),
        ("bn301_305","https://survey.cesrusc.org/bol/index.php?r=eNpLtDKyqi62MrFSKkhMT1WyLrYyMrRSKkotTk0sSs5ILdJLSSxJ1MsvATJBkkB1JZUFEHVWSoZGSta1XDC9yBST"),
    ]

    # Base URLs for two-step downloads
    filter_url_base = "https://survey.cesrusc.org/bol/index.php?r=eNpLtDK0qi62MrFSKkhMT1WyLrYysrBSKkotTk0sSs5ILdJLSSxJ1MtPLi7IKS0Gc5SsawHL-xHx&taskname="
    download_url_base = "https://survey.cesrusc.org/bol/index.php?r=eNpLtDK0qi62MrFSKkhMT1WyBjLNrZSKUotTE4uSM1KL9FISSxL18pOLC3JKiyGc4tTi4sz8vJT88ryc_MSU5OIyJetaXDB0-xnI&taskname="

    two_step_urls = [
        ("raw_sst_practice",
         filter_url_base + "sst_practice&period=45",
         download_url_base + "sst_practice"),
        ("raw_picturenaming",
          filter_url_base + "picturenaming&period=45",
          download_url_base + "picturenaming"),
        ("raw_reaction_practice",
          filter_url_base + "reaction_practice&period=45",
          download_url_base + "reaction_practice"),
        ("raw_reaction",
          filter_url_base + "reaction&period=45",
          download_url_base + "reaction"),
        ("raw_vicky",
          filter_url_base + "vicky&period=45",
          download_url_base + "vicky"),
        ("raw_flanker_practice",
          filter_url_base + "flanker_practice&period=45",
          download_url_base + "flanker_practice"),
        ("raw_sst",
          filter_url_base + "sst&period=45",
          download_url_base + "sst"),
        ("raw_consent",
          filter_url_base + "consent&period=45",
          download_url_base + "consent"),
        ("raw_flanker",
          filter_url_base + "flanker&period=45",
          download_url_base + "flanker"),
        ("raw_picturenaming2",
          filter_url_base + "picturenaming2&period=45",
          download_url_base + "picturenaming2"),
    ]

    login_url = "https://survey.cesrusc.org/bol/index.php"

    # Calculate progress increments based on number of URLs to process
    total_urls = len(urls_to_download) + len(two_step_urls)
    base_progress = 15  # 15% for login only
    url_progress_increment = (100 - base_progress) / total_urls if total_urls > 0 else 0

    # Create session to maintain cookies
    session = requests.Session()

    # Step 1: Get login page and attempt authentication
    log_with_timestamp("Getting login page...")
    login_page_response = session.get(login_url, timeout=timeout_tuple)
    log_with_timestamp("Login page received")

    login_data = {
        'username': creds['username'],
        'password': creds['password']
    }

    log_with_timestamp("Submitting login credentials...")
    login_response = session.post(login_url, data=login_data, timeout=timeout_tuple)
    log_with_timestamp("Login response received")

    # Check if we got a PHPSESSID cookie
    phpsessid = None
    for cookie in session.cookies:
        if cookie.name == 'PHPSESSID':
            phpsessid = cookie.value
            break

    if not phpsessid:
        raise Exception("Failed to obtain PHPSESSID cookie after login")

    print(f"Login successful.")
    set_progress(15)

    # Step 2: Download CSV files and insert into database
    downloaded_files = {}
    current_url_index = 0

    # Combine both arrays for processing
    all_downloads = []

    # Add single-step downloads
    for item in urls_to_download:
        all_downloads.append(('single', item))

    # Add two-step downloads
    for item in two_step_urls:
        all_downloads.append(('two_step', item))

    for download_type, download_info in all_downloads:
        current_url_index += 1

        # Track timing for this file
        file_start_time = time.time()
        download_time = 0
        database_time = 0

        try:
            if download_type == 'single':
                file_name, url = download_info
                log_with_timestamp(f"Starting download {file_name} ({current_url_index}/{total_urls})")

                log_with_timestamp(f"  Requesting {file_name} data...")
                download_start = time.time()
                response = make_request_with_retry(session, url, max_retries, timeout_tuple, file_name)
                download_time = time.time() - download_start
                log_with_timestamp(f"  Received {file_name} response in {download_time:.1f} seconds")

            elif download_type == 'two_step':
                file_name, filter_url, download_url = download_info
                log_with_timestamp(f"Starting download {file_name} ({current_url_index}/{total_urls})")

                # Step 1: Set the data filter/period
                log_with_timestamp(f"  Setting data filter for {file_name}...")
                download_start = time.time()
                filter_response = make_request_with_retry(session, filter_url, max_retries, timeout_tuple, file_name + "_filter")
                filter_time = time.time() - download_start
                log_with_timestamp(f"  Data filter set in {filter_time:.1f} seconds")

                # Step 2: Download the CSV based on filtered data
                log_with_timestamp(f"  Downloading CSV data for {file_name}...")
                csv_start = time.time()
                response = make_request_with_retry(session, download_url, max_retries, timeout_tuple, file_name + "_download")
                csv_time = time.time() - csv_start
                download_time = filter_time + csv_time
                log_with_timestamp(f"  CSV data downloaded in {csv_time:.1f} seconds")

                # Use download_url as the URL for tracking
                url = download_url

            # Common processing for both types starts here

            # Check if response looks like HTML (authentication failure)
            log_with_timestamp(f"  Checking response format for {file_name}...")
            if response.text.strip().lower().startswith('<!doctype html') or response.text.strip().lower().startswith('<html'):
                downloaded_files[file_name] = {
                    'error': 'Response is HTML instead of CSV - authentication may have failed',
                    'url': url
                }
                log_with_timestamp(f"  Error: Got HTML instead of CSV for {file_name}")
                continue

            # Debug: Show raw CSV content first
            log_with_timestamp(f"  DEBUG: Raw CSV first 3 lines:")
            raw_lines = response.text.split('\n')[:3]
            for i, line in enumerate(raw_lines):
                log_with_timestamp(f"    Line {i}: {line[:200]}...")

            # Parse CSV with custom handling for JSON arrays
            log_with_timestamp(f"  Parsing CSV content for {file_name} with JSON array handling...")

            

            # Read CSV line by line with custom JSON array handling
            lines = response.text.strip().split('\n')
            if not lines:
                downloaded_files[file_name] = {
                    'error': 'Empty CSV response',
                    'url': url
                }
                continue

            # Get header - clean trailing commas
            header_line = lines[0].rstrip(',').rstrip()
            headers = [h.strip() for h in header_line.split(',')]
            if file_name == "raw_picturenaming":
                index_positions = [i for i, h in enumerate(headers) if h == "index"]
                if index_positions:
                    headers[index_positions[0]] = "test_index"
                if len(index_positions) > 1:
                    headers[index_positions[1]] = "item_index"
            log_with_timestamp(f"  CSV headers ({len(headers)}): {headers[:5]}...")

            # Parse data rows with smart comma splitting that handles JSON arrays
            data_rows = []
            for line_num, line in enumerate(lines[1:], 2):
                line = line.rstrip(',').rstrip()
                if not line:
                    continue

                # Smart split: handle JSON arrays properly
                fields = []
                current_field = ""
                bracket_depth = 0
                in_quotes = False

                i = 0
                while i < len(line):
                    char = line[i]

                    if char == '"' and (i == 0 or line[i-1] != '\\'):
                        in_quotes = not in_quotes
                        current_field += char
                    elif char == '[' and not in_quotes:
                        bracket_depth += 1
                        current_field += char
                    elif char == ']' and not in_quotes:
                        bracket_depth -= 1
                        current_field += char
                    elif char == ',' and bracket_depth == 0 and not in_quotes:
                        # This comma is a field separator
                        fields.append(current_field.strip())
                        current_field = ""
                    else:
                        current_field += char

                    i += 1

                # Add the last field
                if current_field:
                    fields.append(current_field.strip())

                # Clean up quoted fields - remove surrounding quotes
                cleaned_fields = []
                for field in fields:
                    field = field.strip()
                    # Remove surrounding quotes if present
                    if len(field) >= 2 and field.startswith('"') and field.endswith('"'):
                        field = field[1:-1]  # Remove first and last character (quotes)
                    cleaned_fields.append(field)
                fields = cleaned_fields

                # Pad or trim fields to match header count
                while len(fields) < len(headers):
                    fields.append("")
                fields = fields[:len(headers)]

                data_rows.append(fields)

                # Debug first few rows
                if line_num <= 5:
                    log_with_timestamp(f"    Row {line_num-1}: {len(fields)} fields")
                    if 'direction_1' in headers:
                        dir_idx = headers.index('direction_1') if 'direction_1' in headers else -1
                        if dir_idx >= 0 and dir_idx < len(fields):
                            log_with_timestamp(f"      direction_1: {fields[dir_idx][:50]}...")

            # Create DataFrame
            try:
                df = pd.DataFrame(data_rows, columns=headers)
                log_with_timestamp(f"  Successfully parsed CSV: {len(df)} rows, {len(df.columns)} columns")

                # Debug: check if JSON arrays are preserved
                if 'direction_1' in df.columns:
                    sample_val = df['direction_1'].iloc[0] if len(df) > 0 else "N/A"
                    log_with_timestamp(f"  Sample direction_1 value: {sample_val[:100]}...")

            except Exception as e:
                log_with_timestamp(f"  Error creating DataFrame: {e}")
                downloaded_files[file_name] = {
                    'error': f'Failed to create DataFrame: {e}',
                    'url': url
                }
                continue

            # Connect to database for this specific file
            log_with_timestamp(f"  Connecting to database for {file_name}...")
            database_start = time.time()
            conn = None
            try:
                conn = psycopg2.connect(
                    host=db_creds['host'],
                    port=db_creds['port'],
                    user=db_creds['user'],
                    password=db_creds['password'],
                    database=db_creds['dbname'],
                    connect_timeout=30,  # 30 second connection timeout
                    keepalives=1,        # Enable TCP keepalives
                    keepalives_idle=600, # Start keepalives after 10 minutes of inactivity
                    keepalives_interval=30, # Send keepalive every 30 seconds
                    keepalives_count=3   # Close connection after 3 failed keepalives
                )
                conn.autocommit = True
                log_with_timestamp(f"  Database connection established for {file_name}")

                # Add diagnostic queries to debug table access issue
                cursor = conn.cursor()

                # Check current database
                cursor.execute("SELECT current_database();")
                current_db = cursor.fetchone()[0]
                log_with_timestamp(f"  DEBUG: Connected to database: {current_db}")

                # Check current search path
                cursor.execute("SHOW search_path;")
                search_path = cursor.fetchone()[0]
                log_with_timestamp(f"  DEBUG: Current search_path: {search_path}")

                # Check if nubis schema exists
                cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'nubis';")
                nubis_schema = cursor.fetchone()
                log_with_timestamp(f"  DEBUG: nubis schema exists: {nubis_schema is not None}")

                # Check if we can access the nubis schema
                try:
                    cursor.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = 'nubis' LIMIT 1;")
                    can_access_nubis = cursor.fetchone() is not None
                    log_with_timestamp(f"  DEBUG: Can access nubis schema: {can_access_nubis}")
                except Exception as e:
                    log_with_timestamp(f"  DEBUG: Cannot access nubis schema: {e}")

                # Check specifically for the crosslink_respondants table
                if file_name == "crosslink_respondants":
                    cursor.execute("""
                        SELECT table_name, table_schema
                        FROM information_schema.tables
                        WHERE table_name ILIKE '%crosslink_respondants%'
                    """)
                    tables = cursor.fetchall()
                    log_with_timestamp(f"  DEBUG: Tables matching 'crosslink_respondants': {tables}")

                    # Check exact table with schema
                    cursor.execute("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'nubis' AND table_name = 'crosslink_respondants'
                    """)
                    exact_table = cursor.fetchone()
                    log_with_timestamp(f"  DEBUG: Exact table nubis.crosslink_respondants exists: {exact_table is not None}")

                    # Try to set search path to include nubis
                    try:
                        cursor.execute("SET search_path TO nubis, public;")
                        log_with_timestamp(f"  DEBUG: Set search_path to include nubis schema")
                    except Exception as e:
                        log_with_timestamp(f"  DEBUG: Failed to set search_path: {e}")

                # Insert data into database using bulk operations
                rows_inserted = 0
                duplicates_removed = 0

                # Handle different table schemas with bulk inserts
                if file_name == "crosslink_respondants":
                    # Remove duplicates based on unique constraint (primkey)
                    df_dedupe = df.drop_duplicates(subset=['primkey'], keep='last')
                    duplicates_removed = len(df) - len(df_dedupe)
                    if duplicates_removed > 0:
                        print(f"  Removed {duplicates_removed} duplicate rows")

                    # Prepare data for bulk insert
                    data = [(row['primkey'], row['bolid'], row['ts']) for _, row in df_dedupe.iterrows()]

                    # Bulk insert with upsert
                    insert_query = f"""
                    INSERT INTO nubis.{file_name} (primkey, bolid, ts)
                    VALUES %s
                    ON CONFLICT (primkey) DO UPDATE SET
                        bolid = EXCLUDED.bolid,
                        ts = EXCLUDED.ts,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    execute_values(cursor, insert_query, data, template=None, page_size=250)
                    rows_inserted = len(data)

                elif file_name == "timings":
                    # Remove duplicates based on unique constraint (suid, primkey, variable, timespent, language)
                    df_dedupe = df.drop_duplicates(subset=['suid', 'primkey', 'variable', 'timespent', 'language'], keep='last')
                    duplicates_removed = len(df) - len(df_dedupe)
                    if duplicates_removed > 0:
                        print(f"  Removed {duplicates_removed} duplicate rows")

                    # Prepare data for bulk insert
                    data = [(row['suid'], row['primkey'], row['variable'], row['timespent'], row['language'])
                           for _, row in df_dedupe.iterrows()]

                    # Bulk insert with upsert
                    insert_query = f"""
                    INSERT INTO nubis.{file_name} (suid, primkey, variable, timespent, language)
                    VALUES %s
                    ON CONFLICT (suid, primkey, variable, timespent, language) DO UPDATE SET
                        updated_at = CURRENT_TIMESTAMP
                    """
                    execute_values(cursor, insert_query, data, template=None, page_size=250)
                    rows_inserted = len(data)

                elif file_name == "bn301_305":
                    # Remove duplicates based on unique constraint (primkey, variablename, ts)
                    df_dedupe = df.drop_duplicates(subset=['primkey', 'variablename', 'ts'], keep='last')
                    duplicates_removed = len(df) - len(df_dedupe)
                    if duplicates_removed > 0:
                        print(f"  Removed {duplicates_removed} duplicate rows")

                    # Prepare data for bulk insert
                    data = [(row['primkey'], row['variablename'], row['answer'], row['ts'])
                           for _, row in df_dedupe.iterrows()]

                    # Bulk insert with upsert
                    insert_query = f"""
                    INSERT INTO nubis.{file_name} (primkey, variablename, answer, ts)
                    VALUES %s
                    ON CONFLICT (primkey, variablename, ts) DO UPDATE SET
                        answer = EXCLUDED.answer,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    execute_values(cursor, insert_query, data, template=None, page_size=250)
                    rows_inserted = len(data)

                elif file_name == "raw_sst_practice":
                    # Remove duplicates based on unique constraint (suid, primkey, startts)
                    df_dedupe = df.drop_duplicates(subset=['suid', 'primkey', 'startts'], keep='last')
                    duplicates_removed = len(df) - len(df_dedupe)
                    if duplicates_removed > 0:
                        print(f"  Removed {duplicates_removed} duplicate rows")

                    # Prepare data for bulk insert (handle all 50+ columns)
                    data = []
                    for _, row in df_dedupe.iterrows():
                        # Map CSV column 'index' to SQL column 'index_num'
                        row_data = (
                            row['test'], row['index'], row['suid'], row['ts'], row['primkey'],
                            row['startts'], row['starttsiso'], row['endts'], row['totalcorrect'],
                            # Directions 1-10
                            row['direction_1'], row['clicked_1'], row['correct_1'], row['ts_1'],
                            row['direction_2'], row['clicked_2'], row['correct_2'], row['ts_2'],
                            row['direction_3'], row['clicked_3'], row['correct_3'], row['ts_3'],
                            row['direction_4'], row['clicked_4'], row['correct_4'], row['ts_4'],
                            row['direction_5'], row['clicked_5'], row['correct_5'], row['ts_5'],
                            row['direction_6'], row['clicked_6'], row['correct_6'], row['ts_6'],
                            row['direction_7'], row['clicked_7'], row['correct_7'], row['ts_7'],
                            row['direction_8'], row['clicked_8'], row['correct_8'], row['ts_8'],
                            row['direction_9'], row['clicked_9'], row['correct_9'], row['ts_9'],
                            row['direction_10'], row['clicked_10'], row['correct_10'], row['ts_10']
                        )
                        data.append(row_data)

                    # Bulk insert with upsert (map hyphenated file name to underscored table name)
                    table_name = file_name.replace('-', '_')
                    insert_query = f"""
                    INSERT INTO nubis.{table_name} (
                        test, index_num, suid, ts, primkey, startts, starttsiso, endts, totalcorrect,
                        direction_1, clicked_1, correct_1, ts_1,
                        direction_2, clicked_2, correct_2, ts_2,
                        direction_3, clicked_3, correct_3, ts_3,
                        direction_4, clicked_4, correct_4, ts_4,
                        direction_5, clicked_5, correct_5, ts_5,
                        direction_6, clicked_6, correct_6, ts_6,
                        direction_7, clicked_7, correct_7, ts_7,
                        direction_8, clicked_8, correct_8, ts_8,
                        direction_9, clicked_9, correct_9, ts_9,
                        direction_10, clicked_10, correct_10, ts_10
                    )
                    VALUES %s
                    ON CONFLICT (suid, primkey, startts) DO UPDATE SET
                        test = EXCLUDED.test,
                        index_num = EXCLUDED.index_num,
                        ts = EXCLUDED.ts,
                        endts = EXCLUDED.endts,
                        totalcorrect = EXCLUDED.totalcorrect,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    execute_values(cursor, insert_query, data, template=None, page_size=250)
                    rows_inserted = len(data)

                elif file_name == "raw_picturenaming":
                    # Remove duplicates based on unique constraint (suid, primkey, tstamp_onset)
                    df_dedupe = df.drop_duplicates(subset=['suid', 'primkey', 'tstampOnset'], keep='last')
                    duplicates_removed = len(df) - len(df_dedupe)
                    if duplicates_removed > 0:
                        print(f"  Removed {duplicates_removed} duplicate rows")

                    # Prepare data for bulk insert
                    # Note: CSV has two 'index' columns, pandas renames second one to 'index.1'
                    data = []
                    for _, row in df_dedupe.iterrows():
                        row_data = (
                            row['test'], row['test_index'], row['suid'], row['ts'], row['name'],
                            row['item_index'], row['state'], row['stateDescription'],
                            row['tstampOnset'], row['tstampFinish'], row['durationTotal'],
                            row['variableName'], row['accuracy'], row['accuracyDescription'], row['primkey']
                        )
                        data.append(row_data)

                    # Bulk insert with upsert (map hyphenated file name to underscored table name)
                    table_name = file_name.replace('-', '_')
                    insert_query = f"""
                    INSERT INTO nubis.{table_name} (
                        test, test_index, suid, ts, name, item_index, state, state_description,
                        tstamp_onset, tstamp_finish, duration_total, variable_name,
                        accuracy, accuracy_description, primkey
                    )
                    VALUES %s
                    ON CONFLICT (suid, primkey, tstamp_onset) DO UPDATE SET
                        test = EXCLUDED.test,
                        test_index = EXCLUDED.test_index,
                        ts = EXCLUDED.ts,
                        name = EXCLUDED.name,
                        item_index = EXCLUDED.item_index,
                        state = EXCLUDED.state,
                        state_description = EXCLUDED.state_description,
                        tstamp_finish = EXCLUDED.tstamp_finish,
                        duration_total = EXCLUDED.duration_total,
                        variable_name = EXCLUDED.variable_name,
                        accuracy = EXCLUDED.accuracy,
                        accuracy_description = EXCLUDED.accuracy_description,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    execute_values(cursor, insert_query, data, template=None, page_size=250)
                    rows_inserted = len(data)

                elif file_name == "raw_reaction_practice":
                    # Remove duplicates based on unique constraint (suid, primkey, startts)
                    df_dedupe = df.drop_duplicates(subset=['suid', 'primkey', 'startts'], keep='last')
                    duplicates_removed = len(df) - len(df_dedupe)
                    if duplicates_removed > 0:
                        print(f"  Removed {duplicates_removed} duplicate rows")

                    # Prepare data for bulk insert (handle reaction practice columns)
                    data = []
                    for _, row in df_dedupe.iterrows():
                        # Map CSV column 'index' to SQL column 'index_num'
                        row_data = (
                            row['test'], row['index'], row['suid'], row['ts'], row['primkey'],
                            row['startts'], row['starttsiso'], row['endts'], row['totalcorrect'],
                            # Reactions 1-5
                            row['reaction_1'], row['delay_1'], row['ts_1'],
                            row['reaction_2'], row['delay_2'], row['ts_2'],
                            row['reaction_3'], row['delay_3'], row['ts_3'],
                            row['reaction_4'], row['delay_4'], row['ts_4'],
                            row['reaction_5'], row['delay_5'], row['ts_5']
                        )
                        data.append(row_data)

                    # Bulk insert with upsert (map hyphenated file name to underscored table name)
                    table_name = file_name.replace('-', '_')
                    insert_query = f"""
                    INSERT INTO nubis.{table_name} (
                        test, index_num, suid, ts, primkey, startts, starttsiso, endts, totalcorrect,
                        reaction_1, delay_1, ts_1,
                        reaction_2, delay_2, ts_2,
                        reaction_3, delay_3, ts_3,
                        reaction_4, delay_4, ts_4,
                        reaction_5, delay_5, ts_5
                    )
                    VALUES %s
                    ON CONFLICT (suid, primkey, startts) DO UPDATE SET
                        test = EXCLUDED.test,
                        index_num = EXCLUDED.index_num,
                        ts = EXCLUDED.ts,
                        endts = EXCLUDED.endts,
                        totalcorrect = EXCLUDED.totalcorrect,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    execute_values(cursor, insert_query, data, template=None, page_size=250)
                    rows_inserted = len(data)

                elif file_name == "raw_reaction":
                    # Remove duplicates based on unique constraint (suid, primkey, startts)
                    df_dedupe = df.drop_duplicates(subset=['suid', 'primkey', 'startts'], keep='last')
                    duplicates_removed = len(df) - len(df_dedupe)
                    if duplicates_removed > 0:
                        print(f"  Removed {duplicates_removed} duplicate rows")

                    # Prepare data for bulk insert (handle reaction columns)
                    data = []
                    for _, row in df_dedupe.iterrows():
                        # Map CSV column 'index' to SQL column 'index_num'
                        row_data = (
                            row['test'], row['index'], row['suid'], row['ts'], row['primkey'],
                            row['startts'], row['starttsiso'], row['endts'], row['totalcorrect'],
                            # Reactions 1-5
                            row['reaction_1'], row['delay_1'], row['ts_1'],
                            row['reaction_2'], row['delay_2'], row['ts_2'],
                            row['reaction_3'], row['delay_3'], row['ts_3'],
                            row['reaction_4'], row['delay_4'], row['ts_4'],
                            row['reaction_5'], row['delay_5'], row['ts_5']
                        )
                        data.append(row_data)

                    # Bulk insert with upsert (map hyphenated file name to underscored table name)
                    table_name = file_name.replace('-', '_')
                    insert_query = f"""
                    INSERT INTO nubis.{table_name} (
                        test, index_num, suid, ts, primkey, startts, starttsiso, endts, totalcorrect,
                        reaction_1, delay_1, ts_1,
                        reaction_2, delay_2, ts_2,
                        reaction_3, delay_3, ts_3,
                        reaction_4, delay_4, ts_4,
                        reaction_5, delay_5, ts_5
                    )
                    VALUES %s
                    ON CONFLICT (suid, primkey, startts) DO UPDATE SET
                        test = EXCLUDED.test,
                        index_num = EXCLUDED.index_num,
                        ts = EXCLUDED.ts,
                        endts = EXCLUDED.endts,
                        totalcorrect = EXCLUDED.totalcorrect,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    execute_values(cursor, insert_query, data, template=None, page_size=250)
                    rows_inserted = len(data)

                elif file_name == "raw_vicky":
                    # Remove duplicates based on unique constraint (suid, primkey, local_startts)
                    df_dedupe = df.drop_duplicates(subset=['suid', 'primkey', 'local.startts'], keep='last')
                    duplicates_removed = len(df) - len(df_dedupe)
                    if duplicates_removed > 0:
                        print(f"  Removed {duplicates_removed} duplicate rows")

                    # Prepare data for bulk insert (handle all Vicky columns)
                    data = []
                    for _, row in df_dedupe.iterrows():
                        # Helper function to safely get values (handle empty strings as None)
                        def safe_get(col_name):
                            val = row.get(col_name, None)
                            return None if val == '' or pd.isna(val) else val

                        # Map CSV column 'index' to SQL column 'index_num'
                        row_data = (
                            safe_get('test'), safe_get('index'), safe_get('suid'), safe_get('ts'), safe_get('primkey'),
                            # Introduction and local setup
                            safe_get('intro1.startts'), safe_get('local.startts'), safe_get('local.primkey'),
                            safe_get('local.timezoneoffset'), safe_get('local.timezone'), safe_get('local.endts'),
                            safe_get('intro2.startts'),
                            # Round 1 trials
                            safe_get('1.1.startts'), safe_get('1.1.location'), safe_get('1.1.image'),
                            safe_get('1.1.endts'), safe_get('1.1.cellselectedts'), safe_get('1.1.cellselected'),
                            safe_get('1.2.startts'), safe_get('1.2.location'), safe_get('1.2.image'),
                            safe_get('1.2.endts'), safe_get('1.2.cellselectedts'), safe_get('1.2.cellselected'),
                            safe_get('1.0.endts'),
                            # Round 2 trials
                            safe_get('2.1.startts'), safe_get('2.1.location'), safe_get('2.1.image'),
                            safe_get('2.1.endts'), safe_get('2.1.cellselectedts'), safe_get('2.1.cellselected'),
                            safe_get('2.2.startts'), safe_get('2.2.location'), safe_get('2.2.image'),
                            safe_get('2.2.endts'), safe_get('2.2.cellselectedts'), safe_get('2.2.cellselected'),
                            safe_get('2.0.endts'),
                            # Round 3 trials
                            safe_get('3.1.startts'), safe_get('3.1.location'), safe_get('3.1.image'),
                            safe_get('3.1.endts'), safe_get('3.1.cellselectedts'), safe_get('3.1.cellselected'),
                            safe_get('3.2.startts'), safe_get('3.2.location'), safe_get('3.2.image'),
                            safe_get('3.2.endts'), safe_get('3.2.cellselectedts'), safe_get('3.2.cellselected'),
                            safe_get('3.0.endts'),
                            # Round 4 trials (4 trials)
                            safe_get('4.1.startts'), safe_get('4.1.location'), safe_get('4.1.image'),
                            safe_get('4.1.endts'), safe_get('4.1.cellselectedts'), safe_get('4.1.cellselected'),
                            safe_get('4.2.startts'), safe_get('4.2.location'), safe_get('4.2.image'),
                            safe_get('4.2.endts'), safe_get('4.2.cellselectedts'), safe_get('4.2.cellselected'),
                            safe_get('4.3.startts'), safe_get('4.3.location'), safe_get('4.3.image'),
                            safe_get('4.3.endts'), safe_get('4.3.cellselectedts'), safe_get('4.3.cellselected'),
                            safe_get('4.4.startts'), safe_get('4.4.location'), safe_get('4.4.image'),
                            safe_get('4.4.endts'), safe_get('4.4.cellselectedts'), safe_get('4.4.cellselected'),
                            safe_get('4.0.endts'),
                            # Round 5 trials (4 trials)
                            safe_get('5.1.startts'), safe_get('5.1.location'), safe_get('5.1.image'),
                            safe_get('5.1.endts'), safe_get('5.1.cellselectedts'), safe_get('5.1.cellselected'),
                            safe_get('5.2.startts'), safe_get('5.2.location'), safe_get('5.2.image'),
                            safe_get('5.2.endts'), safe_get('5.2.cellselectedts'), safe_get('5.2.cellselected'),
                            safe_get('5.3.startts'), safe_get('5.3.location'), safe_get('5.3.image'),
                            safe_get('5.3.endts'), safe_get('5.3.cellselectedts'), safe_get('5.3.cellselected'),
                            safe_get('5.4.startts'), safe_get('5.4.location'), safe_get('5.4.image'),
                            safe_get('5.4.endts'), safe_get('5.4.cellselectedts'), safe_get('5.4.cellselected'),
                            safe_get('5.0.endts'),
                            # Additional trials
                            safe_get('2.3.startts'), safe_get('2.3.location'), safe_get('2.3.image'),
                            safe_get('2.3.endts'), safe_get('2.3.cellselectedts'), safe_get('2.3.cellselected'),
                            safe_get('2.4.startts'), safe_get('2.4.location'), safe_get('2.4.image'),
                            safe_get('2.4.endts'), safe_get('2.4.cellselectedts'), safe_get('2.4.cellselected'),
                            safe_get('3.3.startts'), safe_get('3.3.location'), safe_get('3.3.image'),
                            safe_get('3.3.endts'), safe_get('3.3.cellselectedts'), safe_get('3.3.cellselected'),
                            safe_get('3.4.startts'), safe_get('3.4.location'), safe_get('3.4.image'),
                            safe_get('3.4.endts'), safe_get('3.4.cellselectedts'), safe_get('3.4.cellselected')
                        )
                        data.append(row_data)

                    # Bulk insert with upsert (map hyphenated file name to underscored table name)
                    table_name = file_name.replace('-', '_')
                    insert_query = f"""
                    INSERT INTO nubis.{table_name} (
                        test, index_num, suid, ts, primkey,
                        intro1_startts, local_startts, local_primkey, local_timezoneoffset, local_timezone, local_endts, intro2_startts,
                        trial_1_1_startts, trial_1_1_location, trial_1_1_image, trial_1_1_endts, trial_1_1_cellselectedts, trial_1_1_cellselected,
                        trial_1_2_startts, trial_1_2_location, trial_1_2_image, trial_1_2_endts, trial_1_2_cellselectedts, trial_1_2_cellselected,
                        round_1_0_endts,
                        trial_2_1_startts, trial_2_1_location, trial_2_1_image, trial_2_1_endts, trial_2_1_cellselectedts, trial_2_1_cellselected,
                        trial_2_2_startts, trial_2_2_location, trial_2_2_image, trial_2_2_endts, trial_2_2_cellselectedts, trial_2_2_cellselected,
                        round_2_0_endts,
                        trial_3_1_startts, trial_3_1_location, trial_3_1_image, trial_3_1_endts, trial_3_1_cellselectedts, trial_3_1_cellselected,
                        trial_3_2_startts, trial_3_2_location, trial_3_2_image, trial_3_2_endts, trial_3_2_cellselectedts, trial_3_2_cellselected,
                        round_3_0_endts,
                        trial_4_1_startts, trial_4_1_location, trial_4_1_image, trial_4_1_endts, trial_4_1_cellselectedts, trial_4_1_cellselected,
                        trial_4_2_startts, trial_4_2_location, trial_4_2_image, trial_4_2_endts, trial_4_2_cellselectedts, trial_4_2_cellselected,
                        trial_4_3_startts, trial_4_3_location, trial_4_3_image, trial_4_3_endts, trial_4_3_cellselectedts, trial_4_3_cellselected,
                        trial_4_4_startts, trial_4_4_location, trial_4_4_image, trial_4_4_endts, trial_4_4_cellselectedts, trial_4_4_cellselected,
                        round_4_0_endts,
                        trial_5_1_startts, trial_5_1_location, trial_5_1_image, trial_5_1_endts, trial_5_1_cellselectedts, trial_5_1_cellselected,
                        trial_5_2_startts, trial_5_2_location, trial_5_2_image, trial_5_2_endts, trial_5_2_cellselectedts, trial_5_2_cellselected,
                        trial_5_3_startts, trial_5_3_location, trial_5_3_image, trial_5_3_endts, trial_5_3_cellselectedts, trial_5_3_cellselected,
                        trial_5_4_startts, trial_5_4_location, trial_5_4_image, trial_5_4_endts, trial_5_4_cellselectedts, trial_5_4_cellselected,
                        round_5_0_endts,
                        trial_2_3_startts, trial_2_3_location, trial_2_3_image, trial_2_3_endts, trial_2_3_cellselectedts, trial_2_3_cellselected,
                        trial_2_4_startts, trial_2_4_location, trial_2_4_image, trial_2_4_endts, trial_2_4_cellselectedts, trial_2_4_cellselected,
                        trial_3_3_startts, trial_3_3_location, trial_3_3_image, trial_3_3_endts, trial_3_3_cellselectedts, trial_3_3_cellselected,
                        trial_3_4_startts, trial_3_4_location, trial_3_4_image, trial_3_4_endts, trial_3_4_cellselectedts, trial_3_4_cellselected
                    )
                    VALUES %s
                    ON CONFLICT (suid, primkey, local_startts) DO UPDATE SET
                        test = EXCLUDED.test,
                        index_num = EXCLUDED.index_num,
                        ts = EXCLUDED.ts,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    execute_values(cursor, insert_query, data, template=None, page_size=250)
                    rows_inserted = len(data)

                elif file_name == "raw_flanker_practice":
                    # Remove duplicates based on unique constraint (suid, primkey, startts)
                    df_dedupe = df.drop_duplicates(subset=['suid', 'primkey', 'startts'], keep='last')
                    duplicates_removed = len(df) - len(df_dedupe)
                    if duplicates_removed > 0:
                        log_with_timestamp(f"  Removed {duplicates_removed} duplicate rows")

                    log_with_timestamp(f"  DEBUG: raw_flanker_practice column names: {list(df_dedupe.columns)}")
                    log_with_timestamp(f"  DEBUG: First row sample data:")
                    if len(df_dedupe) > 0:
                        first_row = df_dedupe.iloc[0]
                        log_with_timestamp(f"    direction_1: '{first_row['direction_1']}' (type: {type(first_row['direction_1'])})")
                        log_with_timestamp(f"    direction_2: '{first_row['direction_2']}' (type: {type(first_row['direction_2'])})")
                        log_with_timestamp(f"    clicked_1: '{first_row['clicked_1']}' (type: {type(first_row['clicked_1'])})")
                        log_with_timestamp(f"    clicked_2: '{first_row['clicked_2']}' (type: {type(first_row['clicked_2'])})")
                        log_with_timestamp(f"    startts: '{first_row['startts']}' (type: {type(first_row['startts'])})")

                    # Prepare data for bulk insert (handle flanker practice columns)
                    data = []
                    for _, row in df_dedupe.iterrows():
                        # Map CSV column 'index' to SQL column 'index_num'
                        row_data = (
                            row['test'], row['index'], row['suid'], row['ts'], row['primkey'],
                            row['startts'], row['starttsiso'], row['endts'], row['totalcorrect'],
                            # Trial 1 data
                            row['direction_1'], row['clicked_1'], row['correct_1'], row['ts_1'],
                            # Trial 2 data
                            row['direction_2'], row['clicked_2'], row['correct_2'], row['ts_2']
                        )
                        data.append(row_data)

                    # Bulk insert with upsert (map hyphenated file name to underscored table name)
                    table_name = file_name.replace('-', '_')
                    insert_query = f"""
                    INSERT INTO nubis.{table_name} (
                        test, index_num, suid, ts, primkey, startts, starttsiso, endts, totalcorrect,
                        direction_1, clicked_1, correct_1, ts_1,
                        direction_2, clicked_2, correct_2, ts_2
                    )
                    VALUES %s
                    ON CONFLICT (suid, primkey, startts) DO UPDATE SET
                        test = EXCLUDED.test,
                        index_num = EXCLUDED.index_num,
                        ts = EXCLUDED.ts,
                        endts = EXCLUDED.endts,
                        totalcorrect = EXCLUDED.totalcorrect,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    log_with_timestamp(f"  DEBUG: About to insert {len(data)} rows into {table_name}")
                    log_with_timestamp(f"  DEBUG: Sample row data: {data[0] if data else 'No data'}")
                    try:
                        execute_values(cursor, insert_query, data, template=None, page_size=250)
                        rows_inserted = len(data)
                    except Exception as insert_error:
                        log_with_timestamp(f"  DEBUG: Insert failed for {file_name}")
                        log_with_timestamp(f"  DEBUG: Error type: {type(insert_error).__name__}")
                        log_with_timestamp(f"  DEBUG: Error message: {str(insert_error)}")

                        # Show additional PostgreSQL error details if available
                        if hasattr(insert_error, 'pgcode'):
                            log_with_timestamp(f"  DEBUG: PostgreSQL error code: {insert_error.pgcode}")
                        if hasattr(insert_error, 'pgerror'):
                            log_with_timestamp(f"  DEBUG: PostgreSQL error details: {insert_error.pgerror}")

                        log_with_timestamp(f"  DEBUG: First row full data: {data[0] if data else 'No data'}")
                        log_with_timestamp(f"  DEBUG: Table name: {table_name}")

                        # Re-raise the error - will be caught by outer exception handler
                        # which will respect the stop_on_error parameter
                        raise insert_error

                elif file_name == "raw_sst":
                    # Remove duplicates based on unique constraint (suid, primkey, startts)
                    df_dedupe = df.drop_duplicates(subset=['suid', 'primkey', 'startts'], keep='last')
                    duplicates_removed = len(df) - len(df_dedupe)
                    if duplicates_removed > 0:
                        print(f"  Removed {duplicates_removed} duplicate rows")

                    # Prepare data for bulk insert (handle all 24 trials)
                    data = []
                    for _, row in df_dedupe.iterrows():
                        # Map CSV column 'index' to SQL column 'index_num'
                        row_data = (
                            row['test'], row['index'], row['suid'], row['ts'], row['primkey'],
                            row['startts'], row['starttsiso'], row['endts'], row['totalcorrect'],
                            # Directions 1-24 (all trials)
                            row['direction_1'], row['clicked_1'], row['correct_1'], row['ts_1'],
                            row['direction_2'], row['clicked_2'], row['correct_2'], row['ts_2'],
                            row['direction_3'], row['clicked_3'], row['correct_3'], row['ts_3'],
                            row['direction_4'], row['clicked_4'], row['correct_4'], row['ts_4'],
                            row['direction_5'], row['clicked_5'], row['correct_5'], row['ts_5'],
                            row['direction_6'], row['clicked_6'], row['correct_6'], row['ts_6'],
                            row['direction_7'], row['clicked_7'], row['correct_7'], row['ts_7'],
                            row['direction_8'], row['clicked_8'], row['correct_8'], row['ts_8'],
                            row['direction_9'], row['clicked_9'], row['correct_9'], row['ts_9'],
                            row['direction_10'], row['clicked_10'], row['correct_10'], row['ts_10'],
                            row['direction_11'], row['clicked_11'], row['correct_11'], row['ts_11'],
                            row['direction_12'], row['clicked_12'], row['correct_12'], row['ts_12'],
                            row['direction_13'], row['clicked_13'], row['correct_13'], row['ts_13'],
                            row['direction_14'], row['clicked_14'], row['correct_14'], row['ts_14'],
                            row['direction_15'], row['clicked_15'], row['correct_15'], row['ts_15'],
                            row['direction_16'], row['clicked_16'], row['correct_16'], row['ts_16'],
                            row['direction_17'], row['clicked_17'], row['correct_17'], row['ts_17'],
                            row['direction_18'], row['clicked_18'], row['correct_18'], row['ts_18'],
                            row['direction_19'], row['clicked_19'], row['correct_19'], row['ts_19'],
                            row['direction_20'], row['clicked_20'], row['correct_20'], row['ts_20'],
                            row['direction_21'], row['clicked_21'], row['correct_21'], row['ts_21'],
                            row['direction_22'], row['clicked_22'], row['correct_22'], row['ts_22'],
                            row['direction_23'], row['clicked_23'], row['correct_23'], row['ts_23'],
                            row['direction_24'], row['clicked_24'], row['correct_24'], row['ts_24']
                        )
                        data.append(row_data)

                    # Bulk insert with upsert (map hyphenated file name to underscored table name)
                    table_name = file_name.replace('-', '_')
                    insert_query = f"""
                    INSERT INTO nubis.{table_name} (
                        test, index_num, suid, ts, primkey, startts, starttsiso, endts, totalcorrect,
                        direction_1, clicked_1, correct_1, ts_1,
                        direction_2, clicked_2, correct_2, ts_2,
                        direction_3, clicked_3, correct_3, ts_3,
                        direction_4, clicked_4, correct_4, ts_4,
                        direction_5, clicked_5, correct_5, ts_5,
                        direction_6, clicked_6, correct_6, ts_6,
                        direction_7, clicked_7, correct_7, ts_7,
                        direction_8, clicked_8, correct_8, ts_8,
                        direction_9, clicked_9, correct_9, ts_9,
                        direction_10, clicked_10, correct_10, ts_10,
                        direction_11, clicked_11, correct_11, ts_11,
                        direction_12, clicked_12, correct_12, ts_12,
                        direction_13, clicked_13, correct_13, ts_13,
                        direction_14, clicked_14, correct_14, ts_14,
                        direction_15, clicked_15, correct_15, ts_15,
                        direction_16, clicked_16, correct_16, ts_16,
                        direction_17, clicked_17, correct_17, ts_17,
                        direction_18, clicked_18, correct_18, ts_18,
                        direction_19, clicked_19, correct_19, ts_19,
                        direction_20, clicked_20, correct_20, ts_20,
                        direction_21, clicked_21, correct_21, ts_21,
                        direction_22, clicked_22, correct_22, ts_22,
                        direction_23, clicked_23, correct_23, ts_23,
                        direction_24, clicked_24, correct_24, ts_24
                    )
                    VALUES %s
                    ON CONFLICT (suid, primkey, startts) DO UPDATE SET
                        test = EXCLUDED.test,
                        index_num = EXCLUDED.index_num,
                        ts = EXCLUDED.ts,
                        endts = EXCLUDED.endts,
                        totalcorrect = EXCLUDED.totalcorrect,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    execute_values(cursor, insert_query, data, template=None, page_size=250)
                    rows_inserted = len(data)

                elif file_name == "raw_consent":
                    # Remove duplicates based on unique constraint (suid, primkey, tstamp_onset)
                    df_dedupe = df.drop_duplicates(subset=['suid', 'primkey', 'tstampOnset'], keep='last')
                    duplicates_removed = len(df) - len(df_dedupe)
                    if duplicates_removed > 0:
                        print(f"  Removed {duplicates_removed} duplicate rows")

                    # Prepare data for bulk insert
                    # Note: CSV has two 'index' columns, pandas renames second one to 'index.1'
                    data = []
                    for _, row in df_dedupe.iterrows():
                        # Handle potentially empty image field
                        image_val = row.get('image', '')
                        if pd.isna(image_val) or image_val == '':
                            image_val = None

                        row_data = (
                            row['test'], row['index'], row['suid'], row['ts'], row['name'],
                            row['index.1'], row['state'], row['stateDescription'],
                            row['tstampOnset'], row['tstampFinish'], row['durationTotal'],
                            row['variableName'], image_val, row['primkey']
                        )
                        data.append(row_data)

                    # Bulk insert with upsert (map hyphenated file name to underscored table name)
                    table_name = file_name.replace('-', '_')
                    insert_query = f"""
                    INSERT INTO nubis.{table_name} (
                        test, index_num, suid, ts, name, item_index, state, state_description,
                        tstamp_onset, tstamp_finish, duration_total, variable_name,
                        image, primkey
                    )
                    VALUES %s
                    ON CONFLICT (suid, primkey, tstamp_onset) DO UPDATE SET
                        test = EXCLUDED.test,
                        index_num = EXCLUDED.index_num,
                        ts = EXCLUDED.ts,
                        name = EXCLUDED.name,
                        item_index = EXCLUDED.item_index,
                        state = EXCLUDED.state,
                        state_description = EXCLUDED.state_description,
                        tstamp_finish = EXCLUDED.tstamp_finish,
                        duration_total = EXCLUDED.duration_total,
                        variable_name = EXCLUDED.variable_name,
                        image = EXCLUDED.image,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    execute_values(cursor, insert_query, data, template=None, page_size=250)
                    rows_inserted = len(data)

                elif file_name == "raw_flanker":
                    # Remove duplicates based on unique constraint (suid, primkey, startts)
                    df_dedupe = df.drop_duplicates(subset=['suid', 'primkey', 'startts'], keep='last')
                    duplicates_removed = len(df) - len(df_dedupe)
                    if duplicates_removed > 0:
                        log_with_timestamp(f"  Removed {duplicates_removed} duplicate rows")

                    log_with_timestamp(f"  DEBUG: raw_flanker column names: {list(df_dedupe.columns)}")
                    log_with_timestamp(f"  DEBUG: First row sample data:")
                    if len(df_dedupe) > 0:
                        first_row = df_dedupe.iloc[0]
                        log_with_timestamp(f"    direction_1: '{first_row['direction_1']}' (type: {type(first_row['direction_1'])})")
                        log_with_timestamp(f"    direction_2: '{first_row['direction_2']}' (type: {type(first_row['direction_2'])})")
                        log_with_timestamp(f"    clicked_1: '{first_row['clicked_1']}' (type: {type(first_row['clicked_1'])})")
                        log_with_timestamp(f"    startts: '{first_row['startts']}' (type: {type(first_row['startts'])})")

                    # Prepare data for bulk insert (handle all 20 flanker trials)
                    data = []
                    for _, row in df_dedupe.iterrows():
                        # Map CSV column 'index' to SQL column 'index_num'
                        row_data = (
                            row['test'], row['index'], row['suid'], row['ts'], row['primkey'],
                            row['startts'], row['starttsiso'], row['endts'], row['totalcorrect'],
                            # All 20 trials
                            row['direction_1'], row['clicked_1'], row['correct_1'], row['ts_1'],
                            row['direction_2'], row['clicked_2'], row['correct_2'], row['ts_2'],
                            row['direction_3'], row['clicked_3'], row['correct_3'], row['ts_3'],
                            row['direction_4'], row['clicked_4'], row['correct_4'], row['ts_4'],
                            row['direction_5'], row['clicked_5'], row['correct_5'], row['ts_5'],
                            row['direction_6'], row['clicked_6'], row['correct_6'], row['ts_6'],
                            row['direction_7'], row['clicked_7'], row['correct_7'], row['ts_7'],
                            row['direction_8'], row['clicked_8'], row['correct_8'], row['ts_8'],
                            row['direction_9'], row['clicked_9'], row['correct_9'], row['ts_9'],
                            row['direction_10'], row['clicked_10'], row['correct_10'], row['ts_10'],
                            row['direction_11'], row['clicked_11'], row['correct_11'], row['ts_11'],
                            row['direction_12'], row['clicked_12'], row['correct_12'], row['ts_12'],
                            row['direction_13'], row['clicked_13'], row['correct_13'], row['ts_13'],
                            row['direction_14'], row['clicked_14'], row['correct_14'], row['ts_14'],
                            row['direction_15'], row['clicked_15'], row['correct_15'], row['ts_15'],
                            row['direction_16'], row['clicked_16'], row['correct_16'], row['ts_16'],
                            row['direction_17'], row['clicked_17'], row['correct_17'], row['ts_17'],
                            row['direction_18'], row['clicked_18'], row['correct_18'], row['ts_18'],
                            row['direction_19'], row['clicked_19'], row['correct_19'], row['ts_19'],
                            row['direction_20'], row['clicked_20'], row['correct_20'], row['ts_20']
                        )
                        data.append(row_data)

                    # Bulk insert with upsert (map hyphenated file name to underscored table name)
                    table_name = file_name.replace('-', '_')
                    insert_query = f"""
                    INSERT INTO nubis.{table_name} (
                        test, index_num, suid, ts, primkey, startts, starttsiso, endts, totalcorrect,
                        direction_1, clicked_1, correct_1, ts_1,
                        direction_2, clicked_2, correct_2, ts_2,
                        direction_3, clicked_3, correct_3, ts_3,
                        direction_4, clicked_4, correct_4, ts_4,
                        direction_5, clicked_5, correct_5, ts_5,
                        direction_6, clicked_6, correct_6, ts_6,
                        direction_7, clicked_7, correct_7, ts_7,
                        direction_8, clicked_8, correct_8, ts_8,
                        direction_9, clicked_9, correct_9, ts_9,
                        direction_10, clicked_10, correct_10, ts_10,
                        direction_11, clicked_11, correct_11, ts_11,
                        direction_12, clicked_12, correct_12, ts_12,
                        direction_13, clicked_13, correct_13, ts_13,
                        direction_14, clicked_14, correct_14, ts_14,
                        direction_15, clicked_15, correct_15, ts_15,
                        direction_16, clicked_16, correct_16, ts_16,
                        direction_17, clicked_17, correct_17, ts_17,
                        direction_18, clicked_18, correct_18, ts_18,
                        direction_19, clicked_19, correct_19, ts_19,
                        direction_20, clicked_20, correct_20, ts_20
                    )
                    VALUES %s
                    ON CONFLICT (suid, primkey, startts) DO UPDATE SET
                        test = EXCLUDED.test,
                        index_num = EXCLUDED.index_num,
                        ts = EXCLUDED.ts,
                        endts = EXCLUDED.endts,
                        totalcorrect = EXCLUDED.totalcorrect,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    log_with_timestamp(f"  DEBUG: About to insert {len(data)} rows into {table_name}")
                    log_with_timestamp(f"  DEBUG: Sample row data: {data[0] if data else 'No data'}")
                    try:
                        execute_values(cursor, insert_query, data, template=None, page_size=250)
                        rows_inserted = len(data)
                    except Exception as insert_error:
                        log_with_timestamp(f"  DEBUG: Insert failed for {file_name}")
                        log_with_timestamp(f"  DEBUG: Error type: {type(insert_error).__name__}")
                        log_with_timestamp(f"  DEBUG: Error message: {str(insert_error)}")

                        # Show additional PostgreSQL error details if available
                        if hasattr(insert_error, 'pgcode'):
                            log_with_timestamp(f"  DEBUG: PostgreSQL error code: {insert_error.pgcode}")
                        if hasattr(insert_error, 'pgerror'):
                            log_with_timestamp(f"  DEBUG: PostgreSQL error details: {insert_error.pgerror}")

                        log_with_timestamp(f"  DEBUG: First row full data: {data[0] if data else 'No data'}")
                        log_with_timestamp(f"  DEBUG: Table name: {table_name}")

                        # Re-raise the error - will be caught by outer exception handler
                        # which will respect the stop_on_error parameter
                        raise insert_error

                elif file_name == "raw_picturenaming2":
                    # Remove duplicates based on unique constraint (suid, primkey, tstamp_onset, variable_name)
                    df_dedupe = df.drop_duplicates(subset=['suid', 'primkey', 'tstampOnset', 'variableName'], keep='last')
                    duplicates_removed = len(df) - len(df_dedupe)
                    if duplicates_removed > 0:
                        print(f"  Removed {duplicates_removed} duplicate rows")

                    # Prepare data for bulk insert
                    # Note: CSV has two 'index' columns, pandas renames second one to 'index.1'
                    data = []
                    for _, row in df_dedupe.iterrows():
                        row_data = (
                            row['test'], row['index'], row['suid'], row['ts'], row['name'],
                            row['index.1'], row['state'], row['stateDescription'],
                            row['tstampOnset'], row['tstampFinish'], row['durationTotal'],
                            row['variableName'], row['accuracyDescription'], row['accuracy'], row['primkey']
                        )
                        data.append(row_data)

                    # Bulk insert with upsert (map hyphenated file name to underscored table name)
                    table_name = file_name.replace('-', '_')
                    insert_query = f"""
                    INSERT INTO nubis.{table_name} (
                        test, index_num, suid, ts, name, item_index, state, state_description,
                        tstamp_onset, tstamp_finish, duration_total, variable_name,
                        accuracy_description, accuracy, primkey
                    )
                    VALUES %s
                    ON CONFLICT (suid, primkey, tstamp_onset, variable_name) DO UPDATE SET
                        test = EXCLUDED.test,
                        index_num = EXCLUDED.index_num,
                        ts = EXCLUDED.ts,
                        name = EXCLUDED.name,
                        item_index = EXCLUDED.item_index,
                        state = EXCLUDED.state,
                        state_description = EXCLUDED.state_description,
                        tstamp_finish = EXCLUDED.tstamp_finish,
                        duration_total = EXCLUDED.duration_total,
                        accuracy_description = EXCLUDED.accuracy_description,
                        accuracy = EXCLUDED.accuracy,
                        updated_at = CURRENT_TIMESTAMP
                    """
                    execute_values(cursor, insert_query, data, template=None, page_size=250)
                    rows_inserted = len(data)

                else:
                    # Generic insertion for other tables (will need to be customized)
                    database_time = 0  # No database operations performed
                    total_time = time.time() - file_start_time
                    print(f"  Warning: No specific schema defined for table '{file_name}', skipping database insert")
                    downloaded_files[file_name] = {
                        'url': url,
                        'rows_downloaded': len(df),
                        'rows_inserted': 0,
                        'duplicates_removed': 0,
                        'columns': len(df.columns),
                        'warning': f"No database schema defined for table '{file_name}'",
                        'total_time_seconds': round(total_time, 1),
                        'download_time_seconds': round(download_time, 1),
                        'database_time_seconds': round(database_time, 1)
                    }
                    continue

                cursor.close()
                database_time = time.time() - database_start
                total_time = time.time() - file_start_time
                log_with_timestamp(f"  Successfully inserted {rows_inserted} rows into nubis.{file_name}")

                downloaded_files[file_name] = {
                    'url': url,
                    'rows_downloaded': len(df),
                    'rows_inserted': rows_inserted,
                    'duplicates_removed': duplicates_removed,
                    'columns': len(df.columns),
                    'total_time_seconds': round(total_time, 1),
                    'download_time_seconds': round(download_time, 1),
                    'database_time_seconds': round(database_time, 1)
                }

                # Update progress after successful processing
                progress = int(base_progress + (current_url_index * url_progress_increment))
                set_progress(progress)

            except Exception as db_error:
                database_time = time.time() - database_start
                total_time = time.time() - file_start_time

                # Enhanced error logging to show full PostgreSQL error details
                log_with_timestamp(f"  Database error for {file_name}:")
                log_with_timestamp(f"    Error type: {type(db_error).__name__}")
                log_with_timestamp(f"    Error message: {str(db_error)}")

                # Show additional PostgreSQL error details if available
                if hasattr(db_error, 'pgcode'):
                    log_with_timestamp(f"    PostgreSQL error code: {db_error.pgcode}")
                if hasattr(db_error, 'pgerror'):
                    log_with_timestamp(f"    PostgreSQL error details: {db_error.pgerror}")
                if hasattr(db_error, 'diag'):
                    log_with_timestamp(f"    Error diagnostics:")
                    log_with_timestamp(f"      Message primary: {db_error.diag.message_primary}")
                    if db_error.diag.message_detail:
                        log_with_timestamp(f"      Message detail: {db_error.diag.message_detail}")
                    if db_error.diag.message_hint:
                        log_with_timestamp(f"      Message hint: {db_error.diag.message_hint}")
                    if db_error.diag.statement_position:
                        log_with_timestamp(f"      Statement position: {db_error.diag.statement_position}")
                    if db_error.diag.context:
                        log_with_timestamp(f"      Context: {db_error.diag.context}")

                # Show the SQL query that failed if we can determine it
                if file_name == "crosslink_respondants":
                    log_with_timestamp(f"    Failed query was attempting to INSERT INTO nubis.{file_name}")
                    log_with_timestamp(f"    Table schema expected: (primkey, bolid, ts)")

                downloaded_files[file_name] = {
                    'url': url,
                    'rows_downloaded': len(df),
                    'rows_inserted': 0,
                    'duplicates_removed': duplicates_removed if 'duplicates_removed' in locals() else 0,
                    'error': f"Database insert failed: {str(db_error)}",
                    'total_time_seconds': round(total_time, 1),
                    'download_time_seconds': round(download_time, 1),
                    'database_time_seconds': round(database_time, 1)
                }

                # If stop_on_error is True, re-raise the exception to halt execution
                if stop_on_error:
                    log_with_timestamp(f"  Stopping execution due to stop_on_error=True")
                    raise db_error
            finally:
                # Always close the database connection for this file
                if conn:
                    try:
                        conn.close()
                        log_with_timestamp(f"  Database connection closed for {file_name}")
                    except:
                        pass

        except Exception as e:
            total_time = time.time() - file_start_time
            log_with_timestamp(f"  Error processing {file_name}: {str(e)}")
            downloaded_files[file_name] = {
                'error': str(e),
                'url': url if 'url' in locals() else 'unknown',
                'rows_downloaded': 0,
                'rows_inserted': 0,
                'duplicates_removed': 0,
                'total_time_seconds': round(total_time, 1),
                'download_time_seconds': round(download_time, 1),
                'database_time_seconds': round(database_time, 1)
            }

            # If stop_on_error is True, re-raise the exception to halt execution
            if stop_on_error:
                log_with_timestamp(f"  Stopping execution due to stop_on_error=True")
                raise e

            # Update progress even for failed downloads
            progress = int(base_progress + (current_url_index * url_progress_increment))
            set_progress(progress)

    # Calculate summary statistics
    total_downloaded = sum(f.get('rows_downloaded', 0) for f in downloaded_files.values())
    total_inserted = sum(f.get('rows_inserted', 0) for f in downloaded_files.values())
    total_duplicates_removed = sum(f.get('duplicates_removed', 0) for f in downloaded_files.values())
    files_successful = len([f for f in downloaded_files.values() if 'error' not in f])
    files_failed = len([f for f in downloaded_files.values() if 'error' in f])

    # Calculate timing statistics
    total_time = sum(f.get('total_time_seconds', 0) for f in downloaded_files.values())
    total_download_time = sum(f.get('download_time_seconds', 0) for f in downloaded_files.values())
    total_database_time = sum(f.get('database_time_seconds', 0) for f in downloaded_files.values())

    # Set final progress to 100%
    set_progress(100)

    # Log final summary
    log_with_timestamp(f"Processing complete: {files_successful} successful, {files_failed} failed")
    log_with_timestamp(f"Total: {total_downloaded} rows downloaded, {total_inserted} rows inserted")
    log_with_timestamp(f"Timing: {total_time:.1f}s total ({total_download_time:.1f}s download, {total_database_time:.1f}s database)")

    # Return summary of downloaded and inserted data
    return {
        'files_processed': len(downloaded_files),
        'files_successful': files_successful,
        'files_failed': files_failed,
        'total_rows_downloaded': total_downloaded,
        'total_rows_inserted': total_inserted,
        'total_duplicates_removed': total_duplicates_removed,
        'total_time_seconds': round(total_time, 1),
        'total_download_time_seconds': round(total_download_time, 1),
        'total_database_time_seconds': round(total_database_time, 1),
        'details': downloaded_files
    }
