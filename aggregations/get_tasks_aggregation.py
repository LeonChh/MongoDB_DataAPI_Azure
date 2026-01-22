from typing import Any, Dict, List
from .base import BaseAggregation


class GetTasksAggregation(BaseAggregation):
    """
    Haalt tasks op met Title en Description.

    Parameters:
        title_contains (str, optional): Filter op substring in Title (case-insensitive)
        limit (int, optional): Maximum aantal resultaten (default: 100)
    """

    database = "erpDb"
    collection = "Tasks"

    def build_pipeline(self, params: Dict[str, Any]) -> List[Dict]:
        pipeline = []

        # Filter op title substring (case-insensitive)
        # Negeert None, lege string, en whitespace-only
        title_contains = params.get("title_contains")
        if title_contains and str(title_contains).strip():
            pipeline.append({
                "$match": {
                    "Title": {"$regex": str(title_contains).strip(), "$options": "i"}
                }
            })

        # Alleen Title en Description teruggeven
        pipeline.append({
            "$project": {
                "_id": 0,
                "Title": 1,
                "Description": 1
            }
        })

        # Limiet instellen (default 100)
        # Negeert None, null, en ongeldige waarden
        limit = params.get("limit")
        if limit is None or limit == "":
            limit = 100
        else:
            try:
                limit = int(limit)
                if limit <= 0:
                    limit = 100
            except (ValueError, TypeError):
                limit = 100
        pipeline.append({"$limit": limit})

        return pipeline
