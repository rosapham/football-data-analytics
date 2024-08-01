from typing import Any


# Get the HTML content of a Wikipedia page
def get_content_wikipedia(url: str) -> str:
    import requests

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
    from typing import Any

    from bs4 import BeautifulSoup

    print("Extracting data from Wikipedia page ...")
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find_all("table", {"class": "wikitable sortable sticky-header"})[0]
    rows: list[Any] = table.find_all("tr")
    return rows


# Extract data from the Wikipedia page
def extract_wikipedia_page(**kwargs: Any) -> list[Any]:

    import pandas as pd

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
                    "stadium": columns[0].text.strip(),
                    "capacity": columns[1].text.strip(),
                    "region": columns[2].text.strip(),
                    "country": columns[3].text.strip(),
                    "city": columns[4].text.strip(),
                    "image": (
                        columns[5].find("img").get("src").split("//")[1]
                        if columns[5].find("img")
                        else "NO_IMAGE"
                    ),
                    "home_team": columns[6].text.strip(),
                }
            )
    data_df = pd.DataFrame(data)
    data_df.to_csv("data/wikipedia_stadiums.csv", index=False, header=True)
    return data
