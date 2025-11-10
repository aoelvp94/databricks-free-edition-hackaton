import os
from typing import Any, Dict, List

import requests

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

AGENT_NAME = "Travel Hacking Genie"
AGENT_DESCRIPTION = (
    "Answer travel-hacking questions using Gold layer tables and the data "
    "dictionary."
)

GOLD_TABLES = [
    "workspace.gold_travel.gold_price_volatility",
    "workspace.gold_travel.gold_opportunity_index",
    "workspace.gold_travel.gold_demand_forecast",
    "workspace.gold_travel.gold_predictions",
]

KNOWLEDGE_BASES = [
    {
        "type": "TABLE",
        "name": "gold_tables",
        "display_name": "Travel Gold Tables",
        "tables": GOLD_TABLES,
    },
    {
        "type": "DOCS",
        "name": "data_dictionary",
        "display_name": "Travel Data Dictionary",
        "path": (
            "/Workspace/Users/aoelvp94@gmail.com/"
            "travel-hacking-latam/dev/files/docs/data_dictionary.md"
        ),
    },
]


class LakehouseAIClient:
    def __init__(self, host: str, token: str):
        if not host or not token:
            raise SystemExit("Set DATABRICKS_HOST and DATABRICKS_TOKEN.")
        self._base_url = host.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self._timeout = 30

    def _request(
        self,
        method: str,
        path: str,
        payload: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        url = f"{self._base_url}{path}"
        response = requests.request(
            method,
            url,
            headers=self._headers,
            json=payload,
            timeout=self._timeout,
        )
        if response.status_code >= 400:
            error_msg = (
                f"{method} {path} failed: "
                f"{response.status_code} {response.text}"
            )
            raise RuntimeError(error_msg)
        if response.text:
            return response.json()
        return {}

    def list_agents(self) -> List[Dict[str, Any]]:
        result = self._request("GET", "/api/2.0/lakehouseIQ/agents")
        return result.get("agents", [])

    def ensure_knowledge_base(self, kb_spec: Dict[str, Any]) -> str:
        payload = {
            "name": kb_spec["name"],
            "display_name": kb_spec["display_name"],
            "type": kb_spec["type"],
        }
        if kb_spec["type"] == "TABLE":
            payload["tables"] = kb_spec["tables"]
        elif kb_spec["type"] == "DOCS":
            payload["workspace_paths"] = [kb_spec["path"]]
        result = self._request(
            "POST",
            "/api/2.0/lakehouseIQ/knowledge-bases",
            payload,
        )
        return result["id"]

    def ensure_agent(
        self,
        name: str,
        description: str,
        knowledge_base_ids: List[str],
    ) -> str:
        agents = self.list_agents()
        for agent in agents:
            if agent.get("display_name") == name:
                agent_id = agent["id"]
                print(f"Reusing agent {agent_id}")
                break
        else:
            payload = {"display_name": name, "description": description}
            result = self._request(
                "POST",
                "/api/2.0/lakehouseIQ/agents",
                payload,
            )
            agent_id = result["id"]
            print(f"Created agent {agent_id}")

        payload = {"knowledge_base_ids": knowledge_base_ids}
        self._request(
            "PUT",
            "/api/2.0/lakehouseIQ/agents/"
            f"{agent_id}/knowledge-bases",
            payload,
        )
        return agent_id


def main() -> None:
    client = LakehouseAIClient(DATABRICKS_HOST, DATABRICKS_TOKEN)
    kb_ids: List[str] = []
    for kb_spec in KNOWLEDGE_BASES:
        kb_id = client.ensure_knowledge_base(kb_spec)
        kb_ids.append(kb_id)
        print(f"Configured knowledge base {kb_spec['name']} => {kb_id}")

    agent_id = client.ensure_agent(AGENT_NAME, AGENT_DESCRIPTION, kb_ids)
    print(f"Lakehouse AI agent ready: {agent_id}")


if __name__ == "__main__":
    main()
