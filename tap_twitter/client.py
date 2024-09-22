"""REST client handling, including twitterStream base class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Iterable

from requests import Response
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from http import HTTPStatus
import logging
import time

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

if TYPE_CHECKING:
    import requests



class TwitterStream(RESTStream):
    """twitter stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$[*]"

    # Update this value if necessary or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        # TODO: hardcode a value here, or retrieve it from self.config
        return self.config.get("api_url", "https://api.x.com")

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config.get("auth_token", ""),
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        else: 
            headers["User-Agent"] = "tap-twitter"
        return headers

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        Checks for error status codes and whether they are fatal or retriable.

        Args:
            response: A :class:`requests.Response` object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.
        """
        if response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR:
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)

        # rate limits: https://developer.x.com/en/docs/x-api/rate-limits
        if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            msg = self.response_error_message(response)
            rate_limit_remaining = (
                int(response.headers.get("x-rate-limit-remaining"))
                if response.headers.get("x-rate-limit-remaining")
                else None
            )
            rate_limit_reset = (
                int(response.headers.get("x-rate-limit-reset"))
                if response.headers.get("x-rate-limit-reset")
                else None
            )
            current_time_seconds = int(time.time())
            seconds_till_reset = rate_limit_reset - current_time_seconds
            logging.info(
                f"Rate limit reached. x-rate-limit-remaining: {rate_limit_remaining}. Waiting for reset..."
            )
            logging.info(
                f"x-rate-limit-reset at: {rate_limit_reset}. current time is: {current_time_seconds}. seconds till reset: {seconds_till_reset}"
            )
            sleep_seconds = seconds_till_reset + 60 # add 60 seconds buffer
            logging.info(
                f"Sleeping for {sleep_seconds} seconds or {round(sleep_seconds/60, 2)} minutes"
            )
            time.sleep(sleep_seconds)
            raise RetriableAPIError(msg, response)

        if (
            HTTPStatus.BAD_REQUEST
            <= response.status_code
            < HTTPStatus.INTERNAL_SERVER_ERROR
        ):
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)
