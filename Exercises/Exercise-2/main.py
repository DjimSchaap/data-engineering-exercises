import re

import pandas as pd
import requests

url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"


def main():
    file_number = None

    response = requests.get(url)
    html_data = response.text

    rows = re.findall(r"<tr.*?>(.*?)</tr>", html_data, re.DOTALL)

    for row in rows:
        cells = re.findall(r"<td.*?>(.*?)</td>", row, re.DOTALL)
        if len(cells) >= 2 and cells[1] == "2024-01-19 14:57":
            match = re.search(r'href="([^"]+)"', cells[0])
            if match:
                file_number = match.group(1)

    if file_number is not None:
        download_url = url + "/" + file_number

        response = requests.get(download_url)
        response.raise_for_status()

        with open("file.csv", "wb") as f:
            f.write(response.content)

        df = pd.read_csv("file.csv")

        max_temp = df["HourlyDryBulbTemperature"].max()
        hottest = df[df["HourlyDryBulbTemperature"] == max_temp]

        print(hottest)

    pass


if __name__ == "__main__":
    main()
