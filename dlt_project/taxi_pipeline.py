"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


# if no argument is provided, `access_token` is read from `.dlt/secrets.toml`
@dlt.source(name="taxi_pipeline")
def taxi_pipeline_rest_api_source():
    """Define dlt resources from REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            # TODO set base URL for the REST API
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
            # TODO configure the right authentication method or remove
            #"auth": {"type": "bearer", "token": access_token},
        },
        "resources": [
            # TODO define resources per endpoint
            {
                "name": "nyc_taxi_trips",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    # API returns a JSON list at the root
                    "data_selector": "$",
                    # Page-number pagination: ?page=1,2,3...
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "page_param": "page",
                        # No total pages info, stop on empty page
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                },
            }
        ],
        # set `resource_defaults` to apply configuration to all endpoints
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='taxi_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)  # noqa: T201
