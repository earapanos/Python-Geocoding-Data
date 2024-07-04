### ARCGIS API ADDRESS GEOCODING ###
### THIS CODE WAS DEVELOPED BY EDUARDO ADRIANI RAPANOS - https://github.com/earapanos/earapanos ###
### DATE OF RELEASE: 2023-11-13 ###
### 1.0 Version ###

from geopy.geocoders import ArcGIS
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed

def connect_database(config):
    try:
        connection = psycopg2.connect(**config)
        print('Connection successful!')
        return connection
    except psycopg2.Error as e:
        print('Error connecting to the database:', e)
        return None

def count_geocodable_elements(cursor, schema, table, municipalities_condition, address_column):
    cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table} WHERE {municipalities_condition}")
    total_elements = cursor.fetchone()[0]

    cursor.execute(
        f"SELECT COUNT(DISTINCT {address_column}) FROM {schema}.{table} WHERE {address_column} IS NOT NULL AND (latitude IS NULL OR longitude IS NULL) AND {municipalities_condition}"
    )
    geocodable_elements = cursor.fetchone()[0]

    return total_elements, geocodable_elements

def geocode_address(address, geolocator):
    try:
        location = geolocator.geocode(address)
        if location:
            return address, location.latitude, location.longitude, location.address
    except Exception as e:
        print(f'Error geocoding address: {address} - {e}')
    return address, None, None, None

def update_geocoded_table(cursor, connection, schema, table, address_column, address_output_column, municipalities_condition, geolocator, max_workers=4):
    cursor.execute(f"SELECT {address_column} FROM {schema}.{table} WHERE {municipalities_condition} AND ativo = True AND {address_output_column} IS NULL")
    addresses = cursor.fetchall()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_address = {executor.submit(geocode_address, address[0], geolocator): address for address in addresses}
        
        for future in as_completed(future_to_address):
            address = future_to_address[future]
            try:
                address, latitude, longitude, address_output = future.result()
                if latitude is not None and longitude is not None:
                    cursor.execute(
                        f"UPDATE {schema}.{table} SET latitude = %s, longitude = %s, {address_output_column} = %s WHERE {address_column} = %s",
                        (latitude, longitude, address_output.replace("'", " "), address)
                    )
                    connection.commit()
                    print(f'Address: {address} geocoded. Latitude: {latitude}, Longitude: {longitude}, Formatted Address: {address_output}')
                else:
                    print(f'Error geocoding address: {address}')
            except Exception as exc:
                print(f'Address: {address} generated an exception: {exc}')

def main(db_config, schema, table, address_column, address_output_column, municipalities_condition, max_workers=4):
    while True: 
        try:
            with connect_database(db_config) as connection:
                if connection is None:
                    break
                cursor = connection.cursor()

                total_elements, geocodable_elements = count_geocodable_elements(cursor, schema, table, municipalities_condition, address_column)
                print(f'Total elements in table {schema}.{table} for {municipalities_condition}: {total_elements}')
                print(f'Total distinct elements that can be geocoded in table {schema}.{table} for {municipalities_condition}: {geocodable_elements}')

                if total_elements <= 5:
                    print("The number of records is less than or equal to 5. Restarting the code...")
                    break

                geolocator = ArcGIS(timeout=0.25)
                update_geocoded_table(cursor, connection, schema, table, address_column, address_output_column, municipalities_condition, geolocator, max_workers)

        except Exception as e:
            print(f'An error occurred: {e}')
            print('Restarting the code...')
            continue  

if __name__ == '__main__':
    db_config = {
        'host': 'your_host',
        'database': 'your_database',
        'user': 'your_user',   
        'password': 'your_password'
    }
    schema = 'your_schema'
    table = 'your_table'
    address_column = 'address_input'
    address_output_column = 'address_output'
    municipalities_condition = "city = 'AnyCity'" 

    main(db_config, schema, table, address_column, address_output_column, municipalities_condition)