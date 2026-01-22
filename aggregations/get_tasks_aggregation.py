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
        title_contains = params.get("title_contains")
        if title_contains:
            pipeline.append({
                "$match": {
                    "Title": {"$regex": title_contains, "$options": "i"}
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
        limit = params.get("limit", 100)
        pipeline.append({"$limit": limit})

        return pipeline
