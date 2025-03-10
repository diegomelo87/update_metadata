import psycopg2
from psycopg2 import Error
import time

def connect_to_database(schema="public"):
    try:
        # Establish database connection
        connection = psycopg2.connect(
            host="***",     # Server address
            database="***", # Database name
            user="***",     # Database username
            password="***"  # Database password
        )

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        
        # Set the schema
        cursor.execute(f"SET search_path TO {schema}")
        
        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print("Connected to PostgreSQL version:", db_version)
        print(f"Using schema: {schema}")

        return connection, cursor

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL:", error)
        return None, None

def close_connection(connection, cursor):
    if cursor is not None:
        cursor.close()
    if connection is not None:
        connection.close()
        print("PostgreSQL connection is closed")

def execute_query(cursor, query):
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        return records
    except (Exception, Error) as error:
        print("Error executing query:", error)
        return None

def get_customers_without_metadata(cursor, schema="public"):
    """
    Fetch customers that don't have specific metadata and meet certain criteria
    """
    query = f"""
    select
        c.uuid,
        c.identifier
    from
        {schema}.customers c
    join {schema}.interactions i on i.customer_uuid = c.uuid
    join {schema}.distributions d on d.uuid = i.distribution_uuid
    where
        not exists (
        select
            1
        from
            {schema}.customer_metadata cm
        where
            cm.customer_uuid = c.uuid
            and cm.metadata_uuid = 'e1b93a8e-ccdc-4a37-b1fd-68ac47f2a956'
    )
        and c.deleted_at is null
        and d.distribution_channel = 12 -- 12 is the distribution channel for manual insertions
        and c.identifier != ''
        order by c.identifier
    """
    return execute_query(cursor, query)

def insert_customer_metadata(cursor, connection, customers, schema="public", metadata_uuid='e1b93a8e-ccdc-4a37-b1fd-68ac47f2a956'):
    """
    Insert metadata for each customer, skipping duplicates
    """
    current_timestamp = int(time.time())
    
    insert_query = f"""
    INSERT INTO {schema}.customer_metadata 
    (customer_uuid, metadata_uuid, value, created_at)
    VALUES (%s, %s, %s, %s)
    """
    
    successful_inserts = 0
    skipped_records = []
    
    for customer in customers:
        try:
            customer_uuid = customer[0]
            identifier = customer[1]
            
            cursor.execute(insert_query, (
                customer_uuid,
                metadata_uuid,
                identifier,
                current_timestamp
            ))
            connection.commit()
            successful_inserts += 1
            
        except psycopg2.IntegrityError as e:
            # Register duplicate key cases
            connection.rollback()
            skipped_records.append((customer_uuid, identifier))
            print(f"Duplicate key skipped - UUID: {customer_uuid}, Identifier: {identifier}")
            continue
            
        except Exception as e:
            connection.rollback()
            print(f"Error inserting record - UUID: {customer_uuid}, Identifier: {identifier}")
            print(f"Error: {str(e)}")
            continue
    
    print(f"Processing completed:")
    print(f"- Records successfully inserted: {successful_inserts}")
    print(f"- Records skipped (duplicates): {len(skipped_records)}")

def main():
    # Define the schema to be used
    schema = "schema"  # Change this to your actual schema name
    
    # Connect to database with specific schema
    connection, cursor = connect_to_database(schema)
    
    if connection and cursor:
        try:
            # Get customers without metadata
            customers = get_customers_without_metadata(cursor, schema)
            
            if customers:
                print(f"Found {len(customers)} customers without metadata")
                # Insert metadata for these customers
                insert_customer_metadata(cursor, connection, customers, schema)
            else:
                print("No customers found that match the criteria")
                
        finally:
            # Close database connection
            close_connection(connection, cursor)

if __name__ == "__main__":
    main() 