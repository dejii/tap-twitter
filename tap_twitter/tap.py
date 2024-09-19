"""twitter tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_twitter import streams


class Taptwitter(Tap):
    """twitter tap class."""

    name = "tap-twitter"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "user_id",
            th.StringType,
            description="Twitter user ID retrieved from https://api.x.com/2/users/by/username/{handle}",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.x.com",
            description="The url for the API service",
        ),
        th.Property(
            "max_pages",
            th.IntegerType,
            default=15,
            description="The maximum number of pages to retrieve from /2/users/{user_id}/tweets. 20 pages = 2000 tweets (ie 20 * 100)",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.TwitterStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.TweetStream(self),
        ]


if __name__ == "__main__":
    Taptwitter.cli()
