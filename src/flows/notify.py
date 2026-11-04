"""Notification par email en cas d'échec d'un flow.

Hook partagé par les flows `decp_processing`, `sirene_preprocess` et
`run_on_scaleway` : chaque flow l'enregistre via `<flow>.on_failure`.
"""

import asyncio

from prefect.context import get_run_context
from prefect_email import EmailServerCredentials, email_send_message

from src.config import PREFECT_API_URL


def notify_exception_by_email(flow, flow_run, state):
    if PREFECT_API_URL:
        context = get_run_context()
        flow_run_name = context.flow_run.name
        email_server_credentials = EmailServerCredentials.load("email-notifier")
        message = (
            f"Your job {flow_run.name} entered {state.name} "
            f"with message:\n\n"
            f"See <https://{PREFECT_API_URL}/flow-runs/flow-run/{flow_run.id}>\n\n"
            f"Scheduled start: {flow_run.expected_start_time}"
        )

        # email_send_message est une task prefect : appelée directement depuis un
        # hook synchrone, elle retourne une coroutine jamais attendue — l'envoi
        # n'a pas lieu alors que le hook "réussit" sans erreur. On exécute la
        # fonction sous-jacente de façon bloquante.
        asyncio.run(
            email_send_message.fn(
                email_server_credentials=email_server_credentials,
                subject=f"Flow run {flow_run_name!r} failed",
                msg=message,
                email_to=email_server_credentials.username,
            )
        )
