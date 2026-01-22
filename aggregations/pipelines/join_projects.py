"""
Pipeline blok: Koppel Projects aan Tasks via $lookup
"""

JOIN_PROJECTS = [
    {
        "$addFields": {
            "projectIdConverted": {
                "$cond": [
                    {"$gt": [{"$strLenCP": {"$ifNull": ["$ProjectId", ""]}}, 0]},
                    {"$toObjectId": "$ProjectId"},
                    None
                ]
            }
        }
    },
    {
        "$lookup": {
            "from": "Projects",
            "localField": "projectIdConverted",
            "foreignField": "_id",
            "as": "ProjectDetails"
        }
    },
    {
        "$unwind": {
            "path": "$ProjectDetails",
            "preserveNullAndEmptyArrays": True
        }
    }
]
