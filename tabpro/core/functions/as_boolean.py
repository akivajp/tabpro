def as_boolean(value):
    """
    Convert a value to a boolean.

    Args:
        value: The value to convert.

    Returns:
        bool: The converted boolean value.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'y')
    return bool(value)
