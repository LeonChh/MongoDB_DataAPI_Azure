from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseAggregation(ABC):
    """Base class voor alle custom aggregations."""

    # Configuratie - override in subclass
    database: str = None
    collection: str = None

    @abstractmethod
    def build_pipeline(self, params: Dict[str, Any]) -> List[Dict]:
        """Bouw de MongoDB aggregation pipeline op basis van parameters."""
        pass

    def execute(self, client, params: Dict[str, Any]) -> List[Dict]:
        """Voer de aggregation uit en return resultaten."""
        if not self.database or not self.collection:
            raise ValueError("database en collection moeten gedefinieerd zijn")

        pipeline = self.build_pipeline(params)
        db = client[self.database]
        coll = db[self.collection]

        return list(coll.aggregate(pipeline))
