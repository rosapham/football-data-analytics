import requests


def get_data_wikipedia(url):
    print("Getting data from Wikipedia page ... ", url)
    try:
        response = requests.get(url)
        # Check if the request is successful
        response.raise_for_status()
        return response.text
    except requests.RequestException as req_err:
        print(f"HTTP error occurred: {req_err}")
