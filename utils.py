import logging

from simple_salesforce import Salesforce


def salesforce_login(creds: list) -> Salesforce:
    """salesforce_login(creds: list)

    Args:
        creds (list): Salesforce instance & Bearer Token

    Returns:
        Salesforce: An instance of Salesforce, a handy way to wrap a Salesforce session
                    for easy use of the Salesforce REST API.
    """
    try:
        sf = Salesforce(
            instance=creds[0],
            session_id=creds[1],
            version="55.0",
        )

        sf.is_sandbox()
        return sf

    except Exception as e:
        logging.error(e)
        return None
