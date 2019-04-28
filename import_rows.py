import csv
import psycopg2 as psy

row_count = 0
conn = psy.connect(host='localhost', port=5432, database='rideshare', user='rideshare', password='rideshare')
cur = conn.cursor()
sql = 'INSERT INTO trip (trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds, trip_miles, pickup_census_tract, dropoff_census_tract, fare, tip, additional_charges, shared_trip_authorized, trips_pooled, pickup_centroid_location, dropoff_centroid_location) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
with open('tripdata.csv', 'rb') as f:
    reader = csv.DictReader(f)
    for row in reader:
        row_count += 1
        if row_count % 1000 == 0:
            print row_count
        trip_id = row['Trip ID']
        trip_start_timestamp = row['Trip Start Timestamp']
        trip_end_timestamp = row['Trip End Timestamp']
        trip_seconds = int(row['Trip Seconds']) if row['Trip Seconds'] else 0
        trip_miles = float(row['Trip Miles']) if row['Trip Miles'] else 0
        pickup_census_tract = int(row['Pickup Census Tract']) if row['Pickup Census Tract'] else None
        dropoff_census_tract = int(row['Dropoff Census Tract']) if row['Dropoff Census Tract'] else None
        fare = float(row['Fare']) if row['Fare'] else 0
        tip = int(row['Tip']) if row['Tip'] else 0
        additional_charges = float(row['Additional Charges']) if row['Additional Charges'] else 0
        shared_trip_authorized = bool(row['Shared Trip Authorized'])
        trips_pooled = int(row['Trips Pooled'])
        pickup_centroid_location = 'SRID=4326;' + row['Pickup Centroid Location'] if row['Pickup Centroid Location'] != '' else None
        dropoff_centroid_location = 'SRID=4326;' + row['Dropoff Centroid Location'] if row['Dropoff Centroid Location'] != '' else None
#         print (trip_start_timestamp, trip_end_timestamp, trip_seconds, trip_miles, pickup_census_tract, dropoff_census_tract, fare, tip, additional_charges, shared_trip_authorized, trips_pooled, pickup_centroid_location, dropoff_centroid_location)
        cur.execute(sql, (trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds, trip_miles, pickup_census_tract, dropoff_census_tract, fare, tip, additional_charges, shared_trip_authorized, trips_pooled, pickup_centroid_location, dropoff_centroid_location))
conn.commit()
