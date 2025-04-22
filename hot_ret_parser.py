import logging
import pandas as pd
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_mysql(user, password, host, database):
    connection_link = f'mysql+mysqlconnector://{user}:{password}@{host}/{database}'
    engine = create_engine(connection_link)
    return engine

def table_mapping(engine):
    Session = sessionmaker(bind=engine)
    with Session() as session:
        table_mapping_query = text("SELECT src_id, src_table, parsed_data from iata_metadata.source_file_info")
        result = session.execute(table_mapping_query)
        table_mapping_df = pd.DataFrame(result.fetchall(), columns=['src_id', 'src_table', 'parsed_data'])
        table_mapping_dict = table_mapping_df.set_index('src_id').to_dict(orient='index')
    return table_mapping_dict

def fetch_metadata_for_table(engine, table_name):
    metadata_query = text(f"SELECT db_element_name, start_pstn, element_lgth, data_type FROM iata_metadata.{table_name}")
    try:
        with engine.connect() as connection:
            result = connection.execute(metadata_query)
            metadata = pd.DataFrame(result.fetchall(), columns=['db_element_name', 'start_pstn', 'element_lgth', 'data_type'])
        if not metadata.empty:
            metadata['start_pstn'] = pd.to_numeric(metadata['start_pstn'], errors='coerce')
            metadata['element_lgth'] = pd.to_numeric(metadata['element_lgth'], errors='coerce')
        return metadata
    except Exception as e:
        logging.error(f"Error fetching metadata for table {table_name}: {e}")
        return pd.DataFrame()

def extract_field(line, start_pos, length):
    return line[start_pos:start_pos + length].strip()

def identify_table(line, table_map):
    source_code = extract_field(line, 0, 3)
    source_id   = extract_field(line, 11, 2)
    key = f"{source_code}{source_id}"
    return table_map.get(key, {"src_table": "Unknown Table", "parsed_data": "Unknown Table"})

def parse_line(line, metadata):
    record = {}
    for _, row in metadata.iterrows():
        field_name = row['db_element_name']
        start_pos = row['start_pstn']
        length = row['element_lgth']

        if pd.isnull(start_pos) or pd.isnull(length):
            continue

        start_pos = int(start_pos) - 1
        length = int(length)

        if start_pos < 0 or (start_pos + length) > len(line):
            continue

        value = extract_field(line, start_pos, length)
        record[field_name] = value

    return record

def insert_parsed_data(conn, parsed_data, actual_table_name):
    df = pd.DataFrame(parsed_data)
    metadata = MetaData()
    table = Table(actual_table_name, metadata, autoload_with=conn.engine)
    data = df.to_dict(orient='records')
    conn.execute(table.insert().values(data))
    logging.info(f"Successfully inserted {len(parsed_data)} records into {actual_table_name}")

def process_line(line, table_map, metadata_cache, instance_id, engine):
    table_info = identify_table(line, table_map)
    metadata_table = table_info['src_table']
    actual_data_table = table_info['parsed_data']

    if metadata_table == "Unknown Table" or actual_data_table == "Unknown Table":
        return None, None, line

    if metadata_table in metadata_cache:
        metadata = metadata_cache[metadata_table]
    else:
        metadata = fetch_metadata_for_table(engine, metadata_table)
        if not metadata.empty:
            metadata_cache[metadata_table] = metadata
        else:
            return None, None, line

    parsed_record = parse_line(line, metadata)
    parsed_record['instance_id'] = instance_id
    return actual_data_table, parsed_record, None

def normalize_base_name(file_name):
    base = os.path.splitext(file_name)[0]
    for prefix in ['NO_MATCHING_METADATA_', 'FAILED_INSERT_']:
        if base.startswith(prefix):
            base = base[len(prefix):]
    return base

def process_file(file_path, engine, table_map):
    failed_records = []
    not_inserted_records = []
    try:
        metadata_cache = {}
        parsed_data_per_table = {}

        file_name = os.path.basename(file_path)
        base_file_name = normalize_base_name(file_name)
        date_match = re.search(r'D(\d{6})', file_name)
        file_date = date_match.group(1) if date_match else 'unknown_date'

        with open(file_path, 'r', encoding='utf-8') as file:
            first_line = file.readline()
            instance_id = f'{base_file_name}'
            file.seek(0)

            start_time = datetime.now()

            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = {
                    executor.submit(process_line, line, table_map, metadata_cache, instance_id, engine): line
                    for line in file
                }

                for future in as_completed(futures):
                    try:
                        actual_data_table, parsed_record, original_line = future.result()
                        if actual_data_table and parsed_record:
                            parsed_data_per_table.setdefault(actual_data_table, []).append(parsed_record)
                        elif original_line:
                            not_inserted_records.append(original_line)
                    except Exception as e:
                        logging.exception(f"Unexpected error while processing a line: {e}")
                        not_inserted_records.append(futures[future])

            if not_inserted_records:
                raise Exception("Metadata mismatch or parsing errors detected")

            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    for actual_table_name, records in parsed_data_per_table.items():
                        insert_parsed_data(conn, records, actual_table_name)
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    failed_records.append((None, str(e)))

            end_time = datetime.now()
            elapsed_time = (end_time - start_time).total_seconds()
            logging.info(f"Processed file '{file_path}' in {elapsed_time:.2f} seconds.")

    except Exception as e:
        logging.error(f"Error processing file '{file_path}': {e}")
        failed_records.append((None, str(e)))

        if not_inserted_records:
            not_inserted_file_path = os.path.join(os.path.dirname(file_path), f'NO_MATCHING_METADATA_{base_file_name}.txt')
            with open(not_inserted_file_path, 'w', encoding='utf-8') as not_inserted_file:
                for record in not_inserted_records:
                    not_inserted_file.write(f"{record}\n")
            logging.info(f"Not inserted records log written to {not_inserted_file_path}")

        fail_file_path = os.path.join(os.path.dirname(file_path), f'FAILED_INSERT_{base_file_name}.txt')
        with open(fail_file_path, 'w', encoding='utf-8') as fail_file:
            for record, error_detail in failed_records:
                fail_file.write(f"ERROR - Bulk insert failed: {error_detail}\n")
        logging.info(f"Failed records log written to {fail_file_path}")

    return failed_records

def process_files_in_directory(source_folder, engine, table_map):
    try:
        success_folder = os.path.join(source_folder, 'Processed Successfully')
        failed_folder = os.path.join(source_folder, 'Failed')
        no_metadata_folder = os.path.join(source_folder, 'No Matching Metadata')
        os.makedirs(success_folder, exist_ok=True)
        os.makedirs(failed_folder, exist_ok=True)
        os.makedirs(no_metadata_folder, exist_ok=True)

        for file_name in os.listdir(source_folder):
            file_path = os.path.join(source_folder, file_name)
            if not os.path.isfile(file_path):
                continue

            logging.info(f"Processing file: {file_name}")
            failed_records = process_file(file_path, engine, table_map)

            base_file_name = normalize_base_name(file_name)
            has_failed = bool(failed_records)

            if not has_failed:
                new_path = os.path.join(success_folder, file_name)
                os.rename(file_path, new_path)
                logging.info(f"Successfully processed and moved file: {file_name} → {new_path}")
            else:
                has_metadata_issues = any(
                    os.path.exists(os.path.join(source_folder, f"NO_MATCHING_METADATA_{base_file_name}.txt"))
                )
                target_folder = no_metadata_folder if has_metadata_issues else failed_folder
                target_path = os.path.join(target_folder, file_name)
                os.rename(file_path, target_path)
                logging.warning(f"File '{file_name}' encountered issues; moved to {target_folder} folder.")

                for log_prefix in ['FAILED_INSERT', 'NO_MATCHING_METADATA']:
                    log_filename = f"{log_prefix}_{base_file_name}.txt"
                    log_filepath = os.path.join(source_folder, log_filename)
                    if os.path.exists(log_filepath):
                        new_log_path = os.path.join(target_folder, log_filename)
                        os.rename(log_filepath, new_log_path)
                        logging.info(f"Moved log '{log_filename}' to {target_folder} folder.")

    except Exception as e:
        logging.error(f"Error processing files in directory '{source_folder}': {e}")

if __name__ == "__main__":
    user        = os.getenv('MYSQL_USER')
    password    = os.getenv('MYSQL_PASSWORD')
    host        = os.getenv('MYSQL_HOST')
    base_folder = os.getenv('SOURCE_BASE_FOLDER')

    subfolders_to_db = {'HOT': 'prd_hot_files','RET':'prd_ret_files'}

    for subfolder, db_name in subfolders_to_db.items():
        folder_path = os.path.join(base_folder, subfolder)
        if not os.path.isdir(folder_path):
            logging.warning(f"Subfolder '{folder_path}' does not exist. Skipping...")
            continue

        logging.info(f"--- Processing subfolder: {folder_path} → database: {db_name} ---")
        try:
            engine = connect_to_mysql(user, password, host, db_name)
            t_map = table_mapping(engine)
            process_files_in_directory(folder_path, engine, t_map)
        except Exception as ex:
            logging.error(f"Error setting up or processing subfolder '{folder_path}': {ex}")
        finally:
            engine.dispose()

    logging.info("All processing complete.")