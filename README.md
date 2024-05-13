### Address Geocoding with ArcGIS API

#### Description

This Python script geocodes addresses from a PostgreSQL database using the ArcGIS API. It connects to the database using psycopg2, a PostgreSQL adapter for Python, and geocodes addresses using the geopy library's ArcGIS geocoder. The script is divided into functions to handle database connection, counting geocodable elements, geocoding addresses, updating the geocoded table, and closing the database connection. The main function orchestrates the process and includes error handling to restart the code in case of failure.

#### Features

*   **Database Connection:** Establishes a connection to a PostgreSQL database.
*   **Element Counting:** Counts the total number of elements and the number of geocodable elements in a specified table and schema.
*   **Address Geocoding:** Uses the ArcGIS geocoder to geocode addresses from the database.
*   **Table Update:** Updates the geocoded table in the database with latitude, longitude, geocoding precision, and formatted address.
*   **Error Handling:** Restarts the code in case of an error, ensuring continuous operation.

#### Usage

1.  Update the database connection parameters (`host`, `database`, `user`, `password`).
2.  Specify the schema, table, address column, precision column, and formatted address column in the `main` function.
3.  Define the municipalities to geocode in the `municipalities` variable.
4.  Run the script to start the geocoding process.

#### Requirements

*   Python 3.x
*   psycopg2
*   geopy

#### Author

Eduardo Adriani Rapanos

#### Last Update

2024-05-13

#### GitHub Repository

[earapanos/earapanos](https://github.com/earapanos/earapanos)

Feel free to use, modify, and contribute to this code!
