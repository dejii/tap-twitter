"""Stream type classes for tap-twitter."""

from __future__ import annotations

import sys
import typing as t

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk import metrics
from singer_sdk.helpers.types import Context
from requests import Response, PreparedRequest
from singer_sdk.pagination import SinglePagePaginator  # noqa: TCH002

from tap_twitter.client import TwitterStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources


class TwitterJSONPaginator(SinglePagePaginator):

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)

    def has_more(self, response: Response) -> bool:
        resp = response.json()
        meta = resp.get("meta") or {}
        return meta.get("next_token") is not None

    def get_next(self, response: Response) -> None:
        resp = response.json()
        meta = resp.get("meta") or {}
        return meta.get("next_token")


class TweetStream(TwitterStream):
    """Define custom stream."""

    name = "tweets"
    path = "/2/users/{user_id}/tweets"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.data[*]"
    max_results = 100
    tweet_fields: list[str] = [
        "id",
        "text",
        "attachments",
        "author_id",
        "context_annotations",
        "conversation_id",
        "created_at",
        "entities",
        "geo",
        "in_reply_to_user_id",
        "lang",
        "possibly_sensitive",
        "public_metrics",
        "referenced_tweets",
        "reply_settings",
        "source",
        "withheld",
    ]
    schema = th.PropertiesList(
        th.Property("id", th.StringType()),
        th.Property("raw", th.ObjectType(additional_properties=True)),
        th.Property("created_at", th.DateTimeType()),
    ).to_dict()

    def get_new_paginator(self) -> TwitterJSONPaginator:
        return TwitterJSONPaginator()

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """
        paginator = self.get_new_paginator()
        decorated_request = self.request_decorator(self._request)
        pages = 0

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context

            while not paginator.finished and pages < self.config.get("max_pages"):
                prepared_request = self.prepare_request(
                    context,
                    next_page_token=paginator.current_value,
                )
                resp = decorated_request(prepared_request, context)
                request_counter.increment()
                self.update_sync_costs(prepared_request, resp, context)
                records = iter(self.parse_response(resp))
                try:
                    first_record = next(records)
                except StopIteration:
                    self.logger.info(
                        "Pagination stopped after %d pages because no records were "
                        "found in the last response",
                        pages,
                    )
                    break
                yield first_record
                yield from records
                pages += 1

                paginator.advance(resp)

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["pagination_token"] = next_page_token
        params["max_results"] = self.max_results
        params["tweet.fields"] = ",".join(self.tweet_fields)
        return params

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """Post-process each record returned from the API."""
        return {"id": row.get("id"), "raw": row, "created_at": row.get("created_at")}

    def calculate_sync_cost(  # noqa: PLR6301
        self,
        request: PreparedRequest,  # noqa: ARG002
        response: Response,  # noqa: ARG002
        context: Context | None,  # noqa: ARG002
    ) -> dict[str, int]:
        resp = response.json()
        meta = resp.get("meta") or {}
        result_count = meta.get("result_count", 0)
        return {"result_count": result_count, "pages": 1}
