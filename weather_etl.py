import pandas as pd
import json
# from datetime import datetime as dt
import requests
import boto3 as b3
from botocore.exceptions import NoCredentialsError, ClientError
from config import OPENWEATHER_API_KEY


def run_weather_etl():
    # grab data from OpenWeather
    # https://openweathermap.org/forecast5 - 5-day weather forecast at 3hours interval.
    lat = 51.21989      # Antwerp area
    lon = 4.40346
    apikey = OPENWEATHER_API_KEY
    url = "https://api.openweathermap.org/data/2.5/forecast?"

    response = requests.get(f"{url}lat={lat}&lon={lon}&appid={apikey}")
    # Check the response status code
    if response.status_code == 200:
        # Parse the JSON data
        print(response.status_code)
        weather_data = response.json()
        weather_data_pretty = json.dumps(weather_data, indent=4)
        print(weather_data_pretty)

    else:
        print(f"Error: Unable to fetch data (Status code: {response.status_code})")

    # grap necessary data from API store data in a DataFrame
    the_weather_data = []

    for each in range(0, 40):
        datetime_ = weather_data['list'][each]["dt_txt"]
        temperature = round(weather_data['list'][each]["main"]["temp"] - 273.15)
        pressure = round(weather_data['list'][each]["main"]["pressure"])
        humidity = round(weather_data['list'][each]["main"]["humidity"])
        weather = weather_data['list'][0]['weather'][0]["main"]
        weather_description = weather_data['list'][each]['weather'][0]["description"]
        wind_speed = weather_data['list'][each]['wind']['speed']

        the_weather_data.append((datetime_, temperature, pressure, humidity, weather, weather_description, wind_speed))

    # column names
    columns = ['Datetime', 'Temperature', 'Pressure', 'Humidity', 'Weather', 'Weather_Description', 'Wind_Speed']

    # Creating the DataFrame
    df = pd.DataFrame(data=the_weather_data, columns=columns)
    print(df.head(10))

    # Store CSV file into Amazon S3
    df.to_csv("Antwerp-5-Days-Weather.csv")


# run_weather_etl()

# variables first
bucket_name = "james-weather-af-etl"
# directory in local machine to load file from or just filename if no directories in local machine
the_file = "Antwerp-5-Days-Weather.csv"
# directory in S3 bucket to load file into or just filename if no directories in bucket
s3_destination = "Antwerp-5-Days-Weather.csv"

# Create an S3 client and upload file
s3_client = b3.client('s3')
try:
    # Upload the file
    s3_client.upload_file(the_file, bucket_name, s3_destination)
    final_destination = f"{bucket_name}/{s3_destination}"
    print(f"File {the_file} uploaded to {final_destination} successfully.")
except FileNotFoundError:
    print(f"The file {the_file} was not found.")
except NoCredentialsError:
    print("Credentials not available.")
except ClientError as e:
    print(f"Client error: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
