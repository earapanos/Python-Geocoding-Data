### ARCGIS API ADDRESS GEOCODING ###
### This code geocodes addresses from a PostgreSQL database using the ArcGIS API ###
### It uses the psycopg2 library to connect to the database and the geopy library to geocode the addresses ###
### The code is divided into functions to connect to the database, count the geocodable elements, geocode the addresses, update the geocoded table, and close the connection ###
### The main function calls the other functions and uses a while loop to restart the code in case of an error ###
### THIS CODE WAS DEVELOPED BY EDUARDO ADRIANI RAPANOS - https://github.com/earapanos/earapanos ###
### LAST UPDATE: 2024-05-13 ###

from geopy.geocoders import ArcGIS
import psycopg2

def connect_database(host, database, user, password):
    try:
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        print('Connection successful!')
        return connection
    except psycopg2.Error as e:
        print('Error connecting to the database:', e)
        return None

def count_geocodable_elements(connection, schema, table, municipalities, address_column):
    cursor = connection.cursor()
    total_query = f"SELECT COUNT(*) FROM {schema}.{table} WHERE {municipalities}"
    geocodable_query = f"SELECT COUNT(DISTINCT {address_column}) FROM {schema}.{table} WHERE ativo = True AND {address_column} IS NOT NULL AND (latitude IS NULL OR longitude IS NULL) and {municipalities}"

    cursor.execute(total_query)
    total_elements = cursor.fetchone()[0]

    cursor.execute(geocodable_query)
    geocodable_elements = cursor.fetchone()[0]

    cursor.close()

    return total_elements, geocodable_elements

def geocode_address(address, geolocator):
    location = geolocator.geocode(address)
    if location:
        latitude = location.latitude
        longitude = location.longitude
        precision = 'ArcGIS API'  # ArcGIS doesn't directly provide this information
        endereco_formatado = location.address
        return latitude, longitude, precision, endereco_formatado
    else:
        print(f'Error geocoding address: {address}')
        return None, None, None, None

def update_geocoded_table(connection, schema, table, address_column, precision_column, endereco_formatado, municipalities, geolocator):
    cursor = connection.cursor()
    query = f"SELECT {address_column}, latitude, longitude, ativo FROM {schema}.{table} WHERE {municipalities} AND ativo = True AND precisao_geocoding IS NULL"  # This is the filter
 # This is the filter
    cursor.execute(query)
    addresses = cursor.fetchall()

    batch_size = 100  # Define batch size
    for i in range(0, len(addresses), batch_size):
        batch = addresses[i:i + batch_size]
        process_addresses(connection, schema, table, address_column, precision_column, endereco_formatado, municipalities, geolocator, batch)

    cursor.close()

def process_addresses(connection, schema, table, address_column, precision_column, endereco_formatado, municipalities, geolocator, addresses):
    for address in addresses:
        address, latitude, longitude, active = address[0], address[1], address[2], address[3]
        if active and (latitude is None or longitude is None):
            latitude, longitude, precision, endereco_formatado = geocode_address(address, geolocator)
            if latitude is not None and longitude is not None:
                endereco_formatado = endereco_formatado.replace("'", " ")
                update_query = f'UPDATE {schema}.{table} SET latitude = {latitude}, longitude = {longitude}, {precision_column} = \'{precision}\', endereco_formatado = \'{endereco_formatado}\' WHERE {address_column} = %s'
                with connection.cursor() as cursor:
                    cursor.execute(update_query, (address,))
                    connection.commit()
                    print(f'Address: {address} geocoded. Latitude: {latitude}, Longitude: {longitude}, Precision: {precision}, Formatted Address: {endereco_formatado}')
            else:
                print(f'Error geocoding address: {address}')
        else:
            print(f'Address: {address} will not be geocoded.')

def close_connection(connection):
    if connection is not None:
        connection.close()

def main():
    while True:  # Infinite loop to restart the code in case of error
        try:
            host = 'your_host'
            database = 'your_database'
            user = 'your_user'
            password = 'your_password'

            connection = connect_database(host, database, user, password)

            if connection is not None:
                schema = 'your_schema'
                table = 'your_table'
                address_column = 'your_adress_column'
                precision_column = 'precisao_geocoding'
                endereco_formatado = 'your_formatted_address_column'
                municipalities = f"nm_mun ILIKE any(ARRAY['Moscow', 'Pequim', 'Hanói'])" # here the municipality name

                total_elements, geocodable_elements = count_geocodable_elements(connection, schema, table, municipalities, address_column)
                
                print(f'Total elements in table {schema}.{table} for {municipalities}: {total_elements}')
                print(f'Total distinct elements that can be geocoded in table {schema}.{table} for {municipalities}: {geocodable_elements}')

                geolocator = ArcGIS(timeout=10)

                if total_elements <= 5:
                    print("The number of records is less than or equal to 5. Restarting the code...")
                    break

                update_geocoded_table(connection, schema, table, address_column, precision_column, endereco_formatado, municipalities, geolocator)

            close_connection(connection)

        except Exception as e:
            print(f'An error occurred: {e}')
            print('Restarting the code...')
            continue  # Restart the loop after an error

if __name__ == '__main__':
    main()
