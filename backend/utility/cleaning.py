import unicodedata


def clean_column_name(name: str) -> str:
    """
    Cleans up the given string so it can be used as value in Neo4J and other databases.
    """
    # For now, just remove control characters, ref: https://stackoverflow.com/a/19016117
    cleaned_name = "".join(ch for ch in name if unicodedata.category(ch)[0] != "C")
    return cleaned_name
