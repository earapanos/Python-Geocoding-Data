### ARCGIS API ADDRESS GEOCODING ###
### This code geocodes addresses from a PostgreSQL database using the ArcGIS API ###
### It uses the psycopg2 library to connect to the database and the geopy library to geocode the addresses ###
### The code is divided into functions to connect to the database, count the geocodable elements, geocode the addresses, update the geocoded table, and close the connection ###
### The main function calls the other functions and uses a while loop to restart the code in case of an error ###
### THIS CODE WAS DEVELOPED BY EDUARDO ADRIANI RAPANOS - https://github.com/earapanos/earapanos ###
### LAST UPDATE: 2024-05-13 ###
### 1.5 Version - Including ThreadPoolExecutor with MaxWorkers. V ###


from geopy.geocoders import ArcGIS
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    try:
        location = geolocator.geocode(address)
        if location:
            latitude = location.latitude
            longitude = location.longitude
            precision = 'ArcGIS API'  # ArcGIS doesn't directly provide this information
            endereco_formatado = location.address
            return address, latitude, longitude, precision, endereco_formatado
    except Exception as e:
        print(f'Error geocoding address: {address} - {e}')
    
    return address, None, None, None, None

def update_geocoded_table(connection, schema, table, address_column, precision_column, endereco_formatado_column, municipalities, geolocator, max_workers=4):
    cursor = connection.cursor()
    query = f"SELECT {address_column}, latitude, longitude, ativo FROM {schema}.{table} WHERE {municipalities} AND ativo = True AND precisao_geocoding IS NULL"
    cursor.execute(query)
    addresses = cursor.fetchall()
    cursor.close()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_address = {executor.submit(geocode_address, address[0], geolocator): address for address in addresses}
        
        for future in as_completed(future_to_address):
            address = future_to_address[future]
            try:
                address, latitude, longitude, precision, endereco_formatado = future.result()
                if latitude is not None and longitude is not None:
                    endereco_formatado = endereco_formatado.replace("'", " ")
                    update_query = f'UPDATE {schema}.{table} SET latitude = %s, longitude = %s, {precision_column} = %s, {endereco_formatado_column} = %s WHERE {address_column} = %s'
                    with connection.cursor() as update_cursor:
                        update_cursor.execute(update_query, (latitude, longitude, precision, endereco_formatado, address))
                        connection.commit()
                        print(f'Address: {address} geocoded. Latitude: {latitude}, Longitude: {longitude}, Precision: {precision}, Formatted Address: {endereco_formatado}')
                else:
                    print(f'Error geocoding address: {address}')
            except Exception as exc:
                print(f'Address: {address} generated an exception: {exc}')

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
                schema = 'cno'
                table = 'tb_localizacao'
                address_column = 'endereco_consulta'
                precision_column = 'precisao_geocoding'
                endereco_formatado = 'endereco_formatado'
                municipalities = f"nm_mun ILIKE any(ARRAY['NITERÓI'])" # here the municipality name

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