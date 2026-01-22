"""
Pipeline blok: Formatteer Tasks naar leesbaar formaat voor Agent

Bevat:
- Task details (titel, beschrijving, status, type, deadline, etc.)
- Project context (naam, nummer, status, locatie, etc.)
- Notes
- Subtaken (TaskList)
- Extra info (opmetingen, offertes, hoofdcontact, etc.)
"""

# Status mappings
TASK_STATUS_MAP = [
    {"case": {"$eq": ["$Status", 0]}, "then": "Nieuw"},
    {"case": {"$eq": ["$Status", 1]}, "then": "Open"},
    {"case": {"$eq": ["$Status", 2]}, "then": "Bezig"},
    {"case": {"$eq": ["$Status", 3]}, "then": "Uitgesteld"},
    {"case": {"$eq": ["$Status", 4]}, "then": "Meer info nodig"},
    {"case": {"$eq": ["$Status", 5]}, "then": "Gesloten"},
]

TASK_TYPE_MAP = [
    {"case": {"$eq": ["$Type", 0]}, "then": "Planning"},
    {"case": {"$eq": ["$Type", 1]}, "then": "Werfopvolging"},
    {"case": {"$eq": ["$Type", 2]}, "then": "Klacht"},
    {"case": {"$eq": ["$Type", 3]}, "then": "Meerwerk"},
    {"case": {"$eq": ["$Type", 4]}, "then": "Offerte"},
    {"case": {"$eq": ["$Type", 5]}, "then": "Administratie"},
    {"case": {"$eq": ["$Type", 6]}, "then": "Publiciteit"},
    {"case": {"$eq": ["$Type", 7]}, "then": "Protest"},
    {"case": {"$eq": ["$Type", 8]}, "then": "Opvolging betaling"},
    {"case": {"$eq": ["$Type", 9]}, "then": "Contract nakijken"},
    {"case": {"$eq": ["$Type", 10]}, "then": "Opvolgen"},
    {"case": {"$eq": ["$Type", 11]}, "then": "Contacteren"},
    {"case": {"$eq": ["$Type", 12]}, "then": "Facturatie"},
    {"case": {"$eq": ["$Type", 13]}, "then": "Aankoop"},
    {"case": {"$eq": ["$Type", 14]}, "then": "Reminder"},
    {"case": {"$eq": ["$Type", 15]}, "then": "Voorbereiding offerte"},
    {"case": {"$eq": ["$Type", 16]}, "then": "Project"},
    {"case": {"$eq": ["$Type", 17]}, "then": "Uitvoering"},
    {"case": {"$eq": ["$Type", 18]}, "then": "Werkvoorbereiding"},
    {"case": {"$eq": ["$Type", 19]}, "then": "Nasleep"},
    {"case": {"$eq": ["$Type", 20]}, "then": "Nazorg"},
]

PROJECT_STATUS_MAP = [
    {"case": {"$eq": ["$ProjectDetails.Status", 0]}, "then": "Nieuw"},
    {"case": {"$eq": ["$ProjectDetails.Status", 1]}, "then": "Afspraak gemaakt"},
    {"case": {"$eq": ["$ProjectDetails.Status", 2]}, "then": "Opmeting / Meetstaat"},
    {"case": {"$eq": ["$ProjectDetails.Status", 3]}, "then": "Offerte verstuurd"},
    {"case": {"$eq": ["$ProjectDetails.Status", 4]}, "then": "Aanvaard"},
    {"case": {"$eq": ["$ProjectDetails.Status", 5]}, "then": "Niet aanvaard"},
    {"case": {"$eq": ["$ProjectDetails.Status", 6]}, "then": "Ingepland"},
    {"case": {"$eq": ["$ProjectDetails.Status", 7]}, "then": "Lopende fase"},
    {"case": {"$eq": ["$ProjectDetails.Status", 9]}, "then": "Nazorg"},
    {"case": {"$eq": ["$ProjectDetails.Status", 10]}, "then": "Beëindigd werk"},
    {"case": {"$eq": ["$ProjectDetails.Status", 11]}, "then": "Gefactureerd"},
    {"case": {"$eq": ["$ProjectDetails.Status", 12]}, "then": "Afgesloten"},
    {"case": {"$eq": ["$ProjectDetails.Status", 13]}, "then": "Contract in opmaak"},
    {"case": {"$eq": ["$ProjectDetails.Status", 14]}, "then": "Deels betaald"},
    {"case": {"$eq": ["$ProjectDetails.Status", 15]}, "then": "In overleg"},
    {"case": {"$eq": ["$ProjectDetails.Status", 16]}, "then": "Advocaat"},
    {"case": {"$eq": ["$ProjectDetails.Status", 17]}, "then": "Geen interesse vanuit OS"},
    {"case": {"$eq": ["$ProjectDetails.Status", 18]}, "then": "ON HOLD"},
    {"case": {"$eq": ["$ProjectDetails.Status", 19]}, "then": "ANNULATIE"},
]

PROJECT_TYPE_MAP = [
    {"case": {"$eq": ["$ProjectDetails.Type", 0]}, "then": "Standard"},
    {"case": {"$eq": ["$ProjectDetails.Type", 1]}, "then": "Expert"},
    {"case": {"$eq": ["$ProjectDetails.Type", 2]}, "then": "PRO"},
    {"case": {"$eq": ["$ProjectDetails.Type", 3]}, "then": "PRO Plus"},
    {"case": {"$eq": ["$ProjectDetails.Type", 4]}, "then": "Ease"},
    {"case": {"$eq": ["$ProjectDetails.Type", 5]}, "then": "Ease Plus"},
    {"case": {"$eq": ["$ProjectDetails.Type", 6]}, "then": "Brut"},
]


def _convert_dotnet_ticks_to_date(field_path: str, date_format: str = "%d-%m-%Y") -> dict:
    """
    Converteer .NET ticks naar MongoDB date string.
    .NET ticks = 100-nanoseconde intervallen sinds 1/1/0001
    Unix epoch start op 1/1/1970 = 621355968000000000 ticks
    """
    return {
        "$dateToString": {
            "format": date_format,
            "date": {
                "$toDate": {
                    "$divide": [
                        {"$subtract": [{"$arrayElemAt": [field_path, 0]}, 621355968000000000]},
                        10000
                    ]
                }
            }
        }
    }


FORMAT_TASKS = [
    {
        "$project": {
            "_id": 0,

            # === TAAK DETAILS ===
            "taskId": {"$toString": "$_id"},
            "titel": "$Title",
            "beschrijving": "$Description",
            "status": {
                "$switch": {
                    "branches": TASK_STATUS_MAP,
                    "default": "Onbekend"
                }
            },
            "type": {
                "$switch": {
                    "branches": TASK_TYPE_MAP,
                    "default": "$Type"
                }
            },
            "deadline": _convert_dotnet_ticks_to_date("$DueDate"),
            "toegewezenAan": "$UserId",
            "team": "$Team",
            "aangemaakt": _convert_dotnet_ticks_to_date("$CreatedOn"),

            # === PROJECT CONTEXT ===
            "project": {
                "naam": "$ProjectDetails.Name",
                "nummer": "$ProjectDetails.ProjectNumber",
                "status": {
                    "$switch": {
                        "branches": PROJECT_STATUS_MAP,
                        "default": "Onbekend"
                    }
                },
                "type": {
                    "$switch": {
                        "branches": PROJECT_TYPE_MAP,
                        "default": "$ProjectDetails.Type"
                    }
                },
                "voortgang": {
                    "$concat": [
                        {"$toString": {"$ifNull": ["$ProjectDetails.ExecutedPercentage", 0]}},
                        "%"
                    ]
                },
                "locatie": {
                    "$concat": [
                        {"$ifNull": ["$ProjectDetails.Address.Addressline1", ""]},
                        ", ",
                        {"$ifNull": ["$ProjectDetails.Address.Zip", ""]},
                        " ",
                        {"$ifNull": ["$ProjectDetails.Address.City", ""]}
                    ]
                },
                "klantReferentie": "$ProjectDetails.CustomerReference",
                "gewensteStartdatum": {
                    "$cond": [
                        {"$gt": [{"$arrayElemAt": ["$ProjectDetails.RequestedExecutionDate", 0]}, 0]},
                        _convert_dotnet_ticks_to_date("$ProjectDetails.RequestedExecutionDate"),
                        None
                    ]
                }
            },

            # === NOTITIES ===
            "notes": {
                "$map": {
                    "input": {"$ifNull": ["$Notes", []]},
                    "as": "note",
                    "in": {
                        "bericht": "$$note.Message",
                        "auteur": "$$note.Username",
                        "auteurId": "$$note.UserId",
                        "datum": {
                            "$cond": [
                                {"$gt": [{"$arrayElemAt": ["$$note.Moment", 0]}, 0]},
                                {
                                    "$dateToString": {
                                        "format": "%d-%m-%Y %H:%M",
                                        "date": {
                                            "$toDate": {
                                                "$divide": [
                                                    {"$subtract": [{"$arrayElemAt": ["$$note.Moment", 0]}, 621355968000000000]},
                                                    10000
                                                ]
                                            }
                                        }
                                    }
                                },
                                None
                            ]
                        }
                    }
                }
            },
            "aantalNotes": {"$size": {"$ifNull": ["$Notes", []]}},

            # === SUBTAKEN (TaskList) ===
            "subtaken": {
                "$map": {
                    "input": {"$ifNull": ["$TaskList", []]},
                    "as": "subtask",
                    "in": {
                        "titel": "$$subtask.Title",
                        "voltooid": "$$subtask.Completed",
                        "volgorde": "$$subtask.Order"
                    }
                }
            },
            "aantalSubtaken": {"$size": {"$ifNull": ["$TaskList", []]}},
            "voltooideSubtaken": {
                "$size": {
                    "$filter": {
                        "input": {"$ifNull": ["$TaskList", []]},
                        "as": "s",
                        "cond": {"$eq": ["$$s.Completed", True]}
                    }
                }
            },

            # === VERSIE INFO ===
            "versie": "$Version",

            # === EXTRA INFO ===
            "extra": {
                "aantalOpmetingen": {"$size": {"$ifNull": ["$ProjectDetails.Measurements", []]}},
                "openOffertes": "$ProjectDetails.OpenQuotations",
                "geschatteOmzet": "$ProjectDetails.Stats.EstimatedTurnover",
                "hoofdcontact": {
                    "$let": {
                        "vars": {
                            "klant": {
                                "$arrayElemAt": [
                                    {
                                        "$filter": {
                                            "input": {"$ifNull": ["$ProjectDetails.Contacts", []]},
                                            "as": "c",
                                            "cond": {"$in": ["Klant", "$$c.Tags"]}
                                        }
                                    },
                                    0
                                ]
                            }
                        },
                        "in": {
                            "naam": "$$klant.Contact.DisplayName",
                            "telefoon": "$$klant.Contact.Phone",
                            "email": "$$klant.Contact.Email"
                        }
                    }
                },
                "sharepointLink": "$ProjectDetails.WebUrl"
            }
        }
    }
]
