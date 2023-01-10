# tap-helpshift

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

-   Pulls raw data from the [Helpshift API](https://apidocs.helpshift.com/#/)
-   Extracts the following resources:
    -   Issue
    -   Issues Analytics
    -   Messages
    -   Apps
    -   Agents
-   Outputs the schema for each resource
-   Incrementally pulls data based on the input state

**config.json**

```python
{
    "api_key": "api_key", # Required, string
    "subdomain": "subdomain", # Required, string
    "start_date": "2022-01-01T12:00:00Z", # Required, string, iso-formatted datetime string
    "end_date": "2022-01-02T12:00:00Z", # Optional, string, iso-formatted datetime string
    "rate_limit_allowance_percent": 1, # Optional, number, defaults to 1
    "rate_limit_concurrent_allowance_percent": 0.333, # Optional, number, defaults to 1
    "timeout": 300 # Optional, number, defaults to None
}

```

---

Copyright &copy; 2023 Pathlight
