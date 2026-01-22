"""
Get Tasks Aggregation

Haalt tasks op met volledige formatting en project context.
Ondersteunt diverse filters en sorting opties.

Parameters:
    status (list[str], optional): Filter op status ["Open", "Bezig", ...]
    type (list[str], optional): Filter op type ["Facturatie", "Klacht", ...]
    user_id (str, optional): Filter op toegewezen gebruiker
    team (str, optional): Filter op team
    project_number (str, optional): Filter op project nummer
    title_contains (str, optional): Zoek in titel (case-insensitive)
    has_notes (bool, optional): Alleen taken met notities
    has_subtasks (bool, optional): Alleen taken met subtaken
    has_incomplete_subtasks (bool, optional): Alleen taken met onvoltooide subtaken
    project_status (list[str], optional): Filter op project status
    sort_by (str, optional): "deadline" of "created"
    sort_ascending (bool, optional): Sorteer oplopend (default: True voor deadline, False voor created)
    limit (int, optional): Maximum aantal resultaten (default: 100)
"""
from typing import Any, Dict, List
from .base import BaseAggregation
from .pipelines import JOIN_PROJECTS, FORMAT_TASKS
from .filters import TaskFilters


class GetTasksAggregation(BaseAggregation):
    """Haalt tasks op met volledige formatting, project context en filters."""

    database = "erpDb"
    collection = "Tasks"

    def build_pipeline(self, params: Dict[str, Any]) -> List[Dict]:
        pipeline = []

        # === STAP 1: Join met Projects ===
        pipeline.extend(JOIN_PROJECTS)

        # === STAP 2: Format naar leesbaar formaat ===
        pipeline.extend(FORMAT_TASKS)

        # === STAP 3: Filters toepassen (op geformatteerde data) ===

        # Status filter
        status = params.get("status")
        if status:
            if isinstance(status, str):
                status = [status]
            pipeline.extend(TaskFilters.by_status(status))

        # Type filter
        task_type = params.get("type")
        if task_type:
            if isinstance(task_type, str):
                task_type = [task_type]
            pipeline.extend(TaskFilters.by_type(task_type))

        # User filter
        user_id = params.get("user_id")
        if user_id and str(user_id).strip():
            pipeline.extend(TaskFilters.by_user(str(user_id).strip()))

        # Team filter
        team = params.get("team")
        if team and str(team).strip():
            pipeline.extend(TaskFilters.by_team(str(team).strip()))

        # Project number filter
        project_number = params.get("project_number")
        if project_number and str(project_number).strip():
            pipeline.extend(TaskFilters.by_project_number(str(project_number).strip()))

        # Title search
        title_contains = params.get("title_contains")
        if title_contains and str(title_contains).strip():
            pipeline.extend(TaskFilters.by_title(title_contains))

        # Has notes filter
        if params.get("has_notes"):
            pipeline.extend(TaskFilters.has_notes())

        # Has subtasks filter
        if params.get("has_subtasks"):
            pipeline.extend(TaskFilters.has_subtasks())

        # Has incomplete subtasks filter
        if params.get("has_incomplete_subtasks"):
            pipeline.extend(TaskFilters.has_incomplete_subtasks())

        # Project status filter
        project_status = params.get("project_status")
        if project_status:
            if isinstance(project_status, str):
                project_status = [project_status]
            pipeline.extend(TaskFilters.by_project_status(project_status))

        # === STAP 4: Sorting ===
        sort_by = params.get("sort_by")
        if sort_by == "deadline":
            ascending = params.get("sort_ascending", True)
            pipeline.extend(TaskFilters.sort_by_deadline(ascending))
        elif sort_by == "created":
            ascending = params.get("sort_ascending", False)
            pipeline.extend(TaskFilters.sort_by_created(ascending))

        # === STAP 5: Limit ===
        pipeline.extend(TaskFilters.limit(params.get("limit")))

        return pipeline
