from simple_salesforce import Salesforce


def salesforce_login(creds: list):
    """salesforce_login(creds: list)

    Args:
        creds (list): sf_credentials

    Returns:
        sf (Salesforce): simple_salesforce Salesforce Client
    """
    sf = Salesforce(
        instance=creds[0],
        session_id=creds[1],
    )

    # Test to see if we have a working session.
    try:
        sf.is_sandbox()
        return sf
    except Exception:
        return None
