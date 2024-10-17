import os
import requests
import traceback
from urllib.parse import quote

TEAMS_ENDPOINT_URL = os.getenv("TEAMS_ENDPOINT_URL")
AIRFLOW_WEBSERVER_HOST = os.getenv("AIRFLOW_WEBSERVER_HOST")
AIRFLOW_WEBSERVER_PORT = os.getenv("AIRFLOW_WEBSERVER_PORT")


def format_traceback(traceback_list: list) -> str:
    formatted_tracebacks = [tb.replace(" ", "&emsp13;").replace("\n", "\n\n") for tb in traceback_list]
    return "\n\n".join(formatted_tracebacks)


def format_duration(seconds: int | float) -> str:
    hours = round(seconds // 3600)
    minutes = round((seconds % 3600) // 60)
    seconds = round(seconds % 60, 3)

    parts = []
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    if seconds > 0 or not parts:
        parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")

    return " ".join(parts)


def send_teams_notification(context) -> None:
    print("Teams notification is sending...")
    TAG = context["dag"].tags
    DAG_ID = context["ti"].dag_id
    TASK_ID = context["ti"].task_id
    LOGICAL_DATE = context["logical_date"]
    FORMATTED_LOGICAL_DATE = LOGICAL_DATE.in_tz("Asia/Bangkok").format("YYYY-MM-DD HH:mm:ss")
    DURATION = format_duration(context["ti"].duration)
    EXCEPTION = context["exception"]

    if EXCEPTION:
        FORMATTED_TRACEBACK = format_traceback(traceback.format_tb(tb=EXCEPTION.__traceback__))
        FORMATTED_EXCEPTION_ONLY = format_traceback(
            traceback.format_exception_only(type(EXCEPTION), EXCEPTION)
        )  # The function does not accept an `etype` keyword
    else:
        FORMATTED_TRACEBACK = "N/A"
        FORMATTED_EXCEPTION_ONLY = "No exception found in context!"

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.5",
                    "msteams": {"width": "Full"},
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": "Airflow Task Failed!!!",
                            "wrap": True,
                            "fontType": "default",
                            "weight": "bolder",
                            "style": "heading",
                            "size": "large",
                            "color": "attention",
                            "horizontalAlignment": "left",
                            "height": "stretch",
                        },
                        {
                            "type": "ColumnSet",
                            "columns": [
                                {
                                    "type": "Column",
                                    "width": "50px",
                                    "items": [
                                        {
                                            "type": "Image",
                                            "url": "https://astro-provider-logos.s3.us-east-2.amazonaws.com/apache-airflow.png",
                                            "spacing": "None",
                                            "horizontalAlignment": "Center",
                                            "size": "Stretch",
                                        }
                                    ],
                                },
                                {
                                    "type": "Column",
                                    "width": "stretch",
                                    "items": [
                                        {
                                            "type": "FactSet",
                                            "facts": [
                                                {
                                                    "title": "Project ID:",
                                                    "value": "envilink",
                                                },
                                                {
                                                    "title": "Tags:",
                                                    "value": f"[ {', '.join([repr(t) for t in TAG])} ]",
                                                },
                                            ],
                                        }
                                    ],
                                },
                            ],
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {
                                    "title": "DAG ID:",
                                    "value": DAG_ID,
                                },
                                {
                                    "title": "Task ID:",
                                    "value": TASK_ID,
                                },
                                {
                                    "title": "Date Time:",
                                    "value": FORMATTED_LOGICAL_DATE,
                                },
                                {
                                    "title": "Duration:",
                                    "value": DURATION,
                                },
                                {
                                    "title": "Traceback:",
                                    "value": FORMATTED_TRACEBACK,
                                },
                                {
                                    "title": "Exception:",
                                    "value": FORMATTED_EXCEPTION_ONLY,
                                },
                            ],
                        },
                        {
                            "type": "ActionSet",
                            "actions": [
                                {
                                    "type": "Action.OpenUrl",
                                    "title": "View Logs",
                                    "url": f"http://{AIRFLOW_WEBSERVER_HOST}:{AIRFLOW_WEBSERVER_PORT}/log?dag_id={DAG_ID}&task_id={TASK_ID}&execution_date={quote(f'{LOGICAL_DATE}')}",
                                    "iconUrl": "https://cdn-icons-png.freepik.com/512/3214/3214679.png",
                                }
                            ],
                        },
                    ],
                },
            }
        ],
    }

    headers = {"context-type": "application/json"}

    try:
        response = requests.post(TEAMS_ENDPOINT_URL, json=payload, headers=headers)
        response.raise_for_status()
        print("Teams notification sent successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to send Teams notification! | Status code: {response.status_code if response else 'N/A'}")
        print(f"Response: {response.text if response else 'N/A'}")
