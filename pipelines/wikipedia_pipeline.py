import json
import os
from datetime import datetime
from typing import Any, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from geopy.geocoders import Nominatim  # type: ignore

NO_IMAGE = "https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png"  # noqa: E501


# Get the HTML content of a Wikipedia page
def get_content_wikipedia(url: str) -> str:

    print("Getting content from Wikipedia page ... ", url)
    try:
        response = requests.get(url, timeout=10)
        # Check if the request is successful
        response.raise_for_status()
        return response.text
    except requests.RequestException as req_err:
        print(f"HTTP error occurred: {req_err}")
        return ""


# Get the list of Football stadiums by capacity in the wikipedia page
def get_data_wikipedia(html: str) -> list[Any]:

    print("Extracting data from Wikipedia page ...")
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find_all("table", {"class": "wikitable sortable sticky-header"})[0]
    rows: list[Any] = table.find_all("tr")
    return rows


# Clean the data extracted from the Wikipedia page
def clean_data(text: str) -> str:
    if text.find(" ♦"):
        text = text.split(" ♦")[0]
    if text.find("["):
        text = text.split("[")[0]
    if text.find("\n"):
        text = text.replace("\n", "")
    return text.strip()


# Extract data from the Wikipedia page
def extract_wikipedia_data(**kwargs: Any) -> str:

    url = kwargs["url"]
    html = get_content_wikipedia(url)
    rows = get_data_wikipedia(html)

    data: list[Any] = []

    # Start the loop at 1 to skip the header row
    for i in range(1, len(rows)):
        columns = rows[i].find_all("td")
        if len(columns) > 0:
            data.append(
                {
                    "rank": i,
                    "stadium": clean_data(columns[0].text),
                    "capacity": clean_data(columns[1].text).replace(",", "").replace(".", ""),
                    "region": clean_data(columns[2].text),
                    "country": clean_data(columns[3].text),
                    "city": clean_data(columns[4].text),
                    "image": "https://"
                    + (
                        columns[5].find("img").get("src").split("//")[1]
                        if columns[5].find("img")
                        else "NO_IMAGE"
                    ),
                    "home_team": clean_data(columns[6].text),
                }
            )
    json_rows = json.dumps(data)
    # Push the json data into xcom to communicate within the DAG tasks
    kwargs["ti"].xcom_push(key="rows", value=json_rows)
    return "OK"


# Get the latitude and longitude of a country and a city
def get_latitude_longitude(country: str, city: str) -> Optional[Tuple[float, float]]:
    geolocator = Nominatim(user_agent="wikipedia_pipeline")
    location = geolocator.geocode(f"{city}, {country}")
    if location:
        return location.latitude, location.longitude
    return None


# Transform the data extracted from the Wikipedia page
def transform_wikipedia_data(**kwargs: Any) -> str:

    json_rows = kwargs["ti"].xcom_pull(task_ids="extract_data_from_wikipedia", key="rows")
    rows = json.loads(json_rows)

    data_df = pd.DataFrame(rows)
    # Change the default image if the image is not available
    data_df["image"] = data_df["image"].apply(
        lambda x: NO_IMAGE if x in ["NO_IMAGE", "", None] else x
    )
    # Change the capacity to integer
    data_df["capacity"] = data_df["capacity"].astype(int)
    # Get the latitude and longitude of the stadium and country
    # city is not used due to maybe more than one stadium in a city
    data_df["location"] = data_df.apply(
        lambda x: get_latitude_longitude(x["country"], x["stadium"]), axis=1
    )
    # In case the location is duplicated, use the city to get the unique location
    duplicate_df = data_df[data_df.duplicated("location")]
    duplicate_df["location"] = duplicate_df.apply(
        lambda x: get_latitude_longitude(x["country"], x["city"]), axis=1
    )
    data_df.update(duplicate_df)

    # Push the transformed data into xcom to communicate within the DAG tasks
    kwargs["ti"].xcom_push(key="rows", value=data_df.to_json())

    return "OK"


# Write transformed data into a database
def load_wikipedia_data(**kwargs: Any) -> str:

    json_rows = kwargs["ti"].xcom_pull(task_ids="transform_wikipedia_data", key="rows")
    data = json.loads(json_rows)

    # loading variables from .env file
    load_dotenv()

    # Write the data into a csv file
    data_df = pd.DataFrame(data)
    file_name = "wikipedia_stadiums_" + str(datetime.now().date()) + ".csv"
    access_key = os.getenv("ACCOUNT_KEY")
    data_df.to_csv(
        "abfs://footballdata@footballdatasa.dfs.core.windows.net/data/" + file_name,
        storage_options={"account_key": f"{access_key}"},
        index=False,
    )

    return "OK"
