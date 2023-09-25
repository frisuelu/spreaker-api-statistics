"""
Helper functions to connect to the Spreaker API and obtain statistical data for the podcasts in a DataFrame format.
"""

import typing
import requests
import pandas as pd
import datetime


def last_update(show_id: int) -> str:
    """
    Description
    -----------
    Obtain last uploaded podcast.

    Arguments
    ---------
        - show_id (`int`): internal Spreaker ID for the podcast show

    Returns:
        - `str`: last known upload date
    """

    r1 = requests.get("https://api.spreaker.com/v2/shows/" + str(show_id))
    r1.raise_for_status()

    return r1.json()["response"]["show"]["last_episode_at"]


def podcast_list(show_id: int, date: str) -> typing.List[int]:
    """
    Description
    -----------
    Obtain podcasts from the show that happened after a certain date (strict,
    meaning date > date2 and not >=).

    Arguments
    ---------
        - show_id (`int`): internal Spreaker ID for the podcast show
        - date (`str`): date limit in str format
    Returns:
        - f (`list(int)`): list of internal podcast IDs in the selected period
    """

    r2 = requests.get("https://api.spreaker.com/v2/shows/" + str(show_id) + "/episodes")
    r2.raise_for_status()

    f = []

    for i in r2.json()["response"]["items"]:
        if i["published_at"] > date:
            f.append(i["episode_id"])
        else:
            pass

    return f


def statistics(episode_id: int, auth_token: str) -> requests.request.json:
    """
    Description
    -----------
    Statistics for a specific episode. Needs the OAUTH TOKEN to access the
    information.

    Arguments
    ---------
        - episode_id (`int`): internal Spreaker ID for the podcast episode
        - auth_token (`str`): OAuth token for Spreaker data retrieval

    Returns:
        - `requests.request.json`: JSON output with the required statistics
    """

    r3 = requests.get(
        "https://api.spreaker.com/v2/episodes/" + str(episode_id) + "/statistics",
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(auth_token),
        },
    )

    r3.raise_for_status()

    return r3.json()


def parse_to_df(json_list: typing.List[requests.request.json]) -> pd.DataFrame:
    """
    Description
    -----------
    Parse the JSON outputs of the episodes into a single Pandas DataFrame for
    loading into BQ.

    Also audits the execution time, so the BQ table will contain the registry
    of changes for the metrics.

    Arguments
    ---------
        - json_list (`list(requests.request.json)`): list of individual JSON
        statistic outputs for multiple episodes

    Returns:
        - df (`pd.DataFrame`): pandas DataFrame containing the required
        statistics
    """

    df = pd.DataFrame()

    rows = []

    for val in json_list:
        dict = {
            "episode_title": val["response"]["statistics"]["episode"]["title"],
            "plays_count": val["response"]["statistics"]["plays_count"],
            "plays_ondemand_count": val["response"]["statistics"][
                "plays_ondemand_count"
            ],
            "plays_live_count": val["response"]["statistics"]["plays_live_count"],
            "chapters_count": val["response"]["statistics"]["chapters_count"],
            "messages_count": val["response"]["statistics"]["messages_count"],
            "likes_count": val["response"]["statistics"]["likes_count"],
            "downloads_count": val["response"]["statistics"]["downloads_count"],
            "published_at": val["response"]["statistics"]["episode"]["published_at"],
            "date": datetime.datetime.now(),
        }

        rows.append(dict)

    df = pd.DataFrame(rows)

    return df
