"""
Filter functies voor Tasks aggregations.

Alle filters werken op de geformatteerde output (na FORMAT_TASKS).
Dus filteren op 'status' = "Open", niet op 'Status' = 1.

Beschikbare status waarden:
    "Nieuw", "Open", "Bezig", "Uitgesteld", "Meer info nodig", "Gesloten"

Beschikbare type waarden:
    "Planning", "Werfopvolging", "Klacht", "Meerwerk", "Offerte",
    "Administratie", "Publiciteit", "Protest", "Opvolging betaling",
    "Contract nakijken", "Opvolgen", "Contacteren", "Facturatie",
    "Aankoop", "Reminder", "Voorbereiding offerte", "Project",
    "Uitvoering", "Werkvoorbereiding", "Nasleep", "Nazorg"
"""
from typing import List, Optional


class TaskFilters:
    """Statische filter methodes voor task queries."""

    @staticmethod
    def by_status(status_list: List[str]) -> List[dict]:
        """
        Filter taken op status.

        Args:
            status_list: Lijst van statussen, bijv. ["Open", "Bezig"]
        """
        if not status_list:
            return []
        return [{"$match": {"status": {"$in": status_list}}}]

    @staticmethod
    def by_type(type_list: List[str]) -> List[dict]:
        """
        Filter taken op type.

        Args:
            type_list: Lijst van types, bijv. ["Facturatie", "Klacht"]
        """
        if not type_list:
            return []
        return [{"$match": {"type": {"$in": type_list}}}]

    @staticmethod
    def by_user(user_id: str) -> List[dict]:
        """
        Filter taken van specifieke gebruiker.

        Args:
            user_id: User ID string
        """
        if not user_id:
            return []
        return [{"$match": {"toegewezenAan": user_id}}]

    @staticmethod
    def by_team(team: str) -> List[dict]:
        """
        Filter taken van specifiek team.

        Args:
            team: Team naam
        """
        if not team:
            return []
        return [{"$match": {"team": team}}]

    @staticmethod
    def by_project_number(project_number: str) -> List[dict]:
        """
        Filter taken voor specifiek project.

        Args:
            project_number: Project nummer, bijv. "PR/2024/0001"
        """
        if not project_number:
            return []
        return [{"$match": {"project.nummer": project_number}}]

    @staticmethod
    def by_title(search_term: str) -> List[dict]:
        """
        Zoek taken op titel (substring, case-insensitive).

        Args:
            search_term: Zoekterm
        """
        if not search_term or not str(search_term).strip():
            return []
        return [{"$match": {"titel": {"$regex": str(search_term).strip(), "$options": "i"}}}]

    @staticmethod
    def has_notes(min_notes: int = 1) -> List[dict]:
        """
        Filter taken met minimaal X notities.

        Args:
            min_notes: Minimum aantal notities (default 1)
        """
        return [{"$match": {"aantalNotes": {"$gte": min_notes}}}]

    @staticmethod
    def has_subtasks(min_subtasks: int = 1) -> List[dict]:
        """
        Filter taken met minimaal X subtaken.

        Args:
            min_subtasks: Minimum aantal subtaken (default 1)
        """
        return [{"$match": {"aantalSubtaken": {"$gte": min_subtasks}}}]

    @staticmethod
    def has_incomplete_subtasks() -> List[dict]:
        """Filter taken met onvoltooide subtaken."""
        return [{"$match": {"$expr": {"$gt": ["$aantalSubtaken", "$voltooideSubtaken"]}}}]

    @staticmethod
    def by_version(version: str) -> List[dict]:
        """
        Filter taken op specifieke versie.

        Args:
            version: Versie string, bijv. "1.0.2"
        """
        if not version:
            return []
        return [{"$match": {"versie": version}}]

    @staticmethod
    def by_project_status(status_list: List[str]) -> List[dict]:
        """
        Filter taken waarvan het project een bepaalde status heeft.

        Args:
            status_list: Lijst van project statussen
        """
        if not status_list:
            return []
        return [{"$match": {"project.status": {"$in": status_list}}}]

    # === SORTING ===

    @staticmethod
    def sort_by_deadline(ascending: bool = True) -> List[dict]:
        """Sorteer op deadline."""
        return [{"$sort": {"deadline": 1 if ascending else -1}}]

    @staticmethod
    def sort_by_created(ascending: bool = False) -> List[dict]:
        """Sorteer op aanmaakdatum (nieuwste eerst by default)."""
        return [{"$sort": {"aangemaakt": 1 if ascending else -1}}]

    # === LIMIT ===

    @staticmethod
    def limit(count: int) -> List[dict]:
        """Beperk aantal resultaten."""
        if count is None or count == "":
            count = 100
        try:
            count = int(count)
            if count <= 0:
                count = 100
        except (ValueError, TypeError):
            count = 100
        return [{"$limit": count}]
