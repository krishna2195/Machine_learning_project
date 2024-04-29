import csv
import urllib3
import xml.etree.ElementTree as ET
import boto3
from botocore.exceptions import NoCredentialsError

# Lambda function handler
def lambda_handler(event, context):
    # URL of the XML data
    url = "https://www.ctabustracker.com/bustime/api/v2/getvehicles?key=KUjdqGNmEKxrAZWavxMZVRuxr&rt=3"

    http = urllib3.PoolManager()
    # Send a GET request to fetch the XML data
    response = http.request('GET', url)

    # Check if the request was successful
    if response.status == 200:
        # Parse the XML
        root = ET.fromstring(response.data)

        # Read existing CSV file from S3 if it exists
        try:
            s3 = boto3.client("s3")
            bucket_name = "ctabus3"
            existing_data = s3.get_object(Bucket=bucket_name, Key="bus3/bus_data_lambda.csv")
            existing_data_str = existing_data['Body'].read().decode('utf-8')
            csv_data = [row.split(",") for row in existing_data_str.split("\n")]
            new_file = False
        except s3.exceptions.NoSuchKey:
            csv_data = [["Vehicle ID", "Timestamp", "Latitude", "Longitude", "Heading", "Pattern ID", "Route", "Destination", "Distance", "Delay"]]
            new_file = True

        # Append new data to CSV data list
        for vehicle in root.findall("./vehicle"):
            vid = vehicle.find("vid").text
            tmstmp = vehicle.find("tmstmp").text
            lat = vehicle.find("lat").text
            lon = vehicle.find("lon").text
            hdg = vehicle.find("hdg").text
            pid = vehicle.find("pid").text
            rt = vehicle.find("rt").text
            des = vehicle.find("des").text
            pdist = vehicle.find("pdist").text
            dly = vehicle.find("dly").text
            csv_data.append([vid, tmstmp, lat, lon, hdg, pid, rt, des, pdist, dly])

        # Convert CSV data to string
        csv_data_string = "\n".join([",".join(row) for row in csv_data])

        # Upload the updated CSV data to S3 bucket
        try:
            if new_file:
                s3.put_object(Body=csv_data_string, Bucket=bucket_name, Key="bus3/bus_data_lambda.csv")
                print("New CSV file created.")
            else:
                s3.put_object(Body=csv_data_string, Bucket=bucket_name, Key="bus3/bus_data_lambda.csv")
                print("CSV data appended successfully.")
        except NoCredentialsError:
            print("Unable to access AWS credentials. Please configure your credentials.")

    else:
        print("Failed to fetch XML data.")
