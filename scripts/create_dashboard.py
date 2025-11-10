import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import requests

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

DASHBOARD_NAME = "Travel Hacking KPIs"

QUERIES: List[Dict[str, Any]] = [
    {
        "name": "gold_price_volatility_overview",
        "query": """
                SELECT route_id,
                       origin_city,
                       destination_city,
                       purchase_year,
                       purchase_week_of_year,
                       avg_price_usd,
                       std_price_usd,
                       volatility_index
                FROM workspace.gold_travel.gold_price_volatility
                ORDER BY purchase_year DESC, purchase_week_of_year DESC
                LIMIT 200
                """,
    },
    {
        "name": "gold_opportunity_index_top_routes",
        "query": """
                SELECT route_id,
                       travel_date,
                       opportunity_index,
                       event_type,
                       bundle_price_usd
                FROM workspace.gold_travel.gold_opportunity_index
                ORDER BY opportunity_index DESC
                LIMIT 200
                """,
    },
]


class DashboardClient:
    def __init__(self, host: str, token: str):
        if not host or not token:
            raise SystemExit("Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")
        self._base_url = host.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        url = f"{self._base_url}{path}"
        response = requests.request(method, url, headers=self._headers, json=payload)
        if response.status_code >= 400:
            raise RuntimeError(f"{method} {path} failed: {response.status_code} {response.text}")
        if response.text:
            return response.json()
        return {}

    def find_existing_dashboard(self, name: str) -> str | None:
        payload = {"page_size": 250}
        result = self._request("GET", "/api/2.0/sql/dashboards", payload)
        for dashboard in result.get("results", []):
            if dashboard.get("name") == name:
                return dashboard.get("id")
        return None

    def create_query(self, name: str, raw_query: str) -> str:
        payload = {
            "name": name,
            "data_source_id": "sql/pro",
            "query": raw_query,
        }
        result = self._request("POST", "/api/2.0/sql/queries", payload)
        return result["id"]

    def create_dashboard(self, name: str) -> str:
        payload = {"name": name}
        result = self._request("POST", "/api/2.0/sql/dashboards", payload)
        return result["id"]

    def add_widget(self, dashboard_id: str, query_id: str, title: str, position: Dict[str, int]) -> None:
        payload = {
            "dashboard_id": dashboard_id,
            "visualization_spec": {
                "display": "table",
                "query_id": query_id,
            },
            "title": title,
            "text": "",
            "parameters": {},
            "position": position,
        }
        self._request("POST", "/api/2.0/sql/widgets", payload)


def main() -> None:
    client = DashboardClient(DATABRICKS_HOST, DATABRICKS_TOKEN)
    dashboard_id = client.find_existing_dashboard(DASHBOARD_NAME)
    if dashboard_id is None:
        dashboard_id = client.create_dashboard(DASHBOARD_NAME)
        print(f"Created dashboard {dashboard_id}")
    else:
        print(f"Reusing dashboard {dashboard_id}")

    for index, query_spec in enumerate(QUERIES):
        query_id = client.create_query(query_spec["name"], query_spec["query"])
        print(f"Created query {query_spec['name']} => {query_id}")
        client.add_widget(
            dashboard_id,
            query_id,
            title=query_spec["name"].replace("_", " ").title(),
            position={"size_x": 6, "size_y": 8, "row": 0, "col": index * 6},
        )

    print("Dashboard provisioning complete.")


if __name__ == "__main__":
    main()
