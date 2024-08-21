import csv
import sqlite3

# Database connection details
DATABASE_NAME = "shipping_data.db"

# File paths
SPREADSHEET_0_PATH = "shipping_data_0.csv"
SPREADSHEET_1_PATH = "shipping_data_1.csv"
SPREADSHEET_2_PATH = "shipping_data_2.csv"

def insert_data(path, table_name, columns):
    with open(path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        data = []
        for row in reader:
            data.append(tuple(row[col] for col in columns))

        connection = sqlite3.connect(DATABASE_NAME)
        cursor = connection.cursor()
        cursor.executemany(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})", data)
        connection.commit()
        connection.close()

def process_shipment_data():
    # Read data from spreadsheet 1
    shipment_products = {}
    with open(SPREADSHEET_1_PATH, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            shipment_id = row['shipment_identifier']
            product = row['product']
            on_time = row['on_time']
            if shipment_id not in shipment_products:
                shipment_products[shipment_id] = {'products': [], 'on_time': on_time}
            shipment_products[shipment_id]['products'].append(product)

    # Read data from spreadsheet 2 and combine with spreadsheet 1
    with open(SPREADSHEET_2_PATH, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            shipment_id = row['shipment_identifier']
            origin = row['origin_warehouse']
            destination = row['destination_store']
            driver_identifier = row['driver_identifier']
            if shipment_id in shipment_products:
                shipment_products[shipment_id]['origin'] = origin
                shipment_products[shipment_id]['destination'] = destination
                shipment_products[shipment_id]['driver_identifier'] = driver_identifier

    # Insert data into the database
    for shipment_id, data in shipment_products.items():
        shipment_data = (
            shipment_id,
            data['origin'],
            data['destination'],
            data['driver_identifier'],
            data['on_time']
        )
        insert_data(SPREADSHEET_0_PATH, "shipments", ["shipment_id", "origin", "destination", "driver_identifier", "on_time"], shipment_data)

        for product in data['products']:
            product_data = (
                shipment_id,
                product,
                len(data['products'])  # Assuming equal quantities for products in a shipment
            )
            insert_data(SPREADSHEET_0_PATH, "shipment_products", ["shipment_id", "product", "quantity"], product_data)

if __name__ == "__main__":
    process_shipment_data()