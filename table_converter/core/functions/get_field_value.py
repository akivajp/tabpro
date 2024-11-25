# Description: Get the value of a field in a dictionary.

from collections import OrderedDict

def get_field_value(
    data: OrderedDict,
    field: str,
):
    if field in data:
        return data[field], True
    if '.' in field:
        field, rest = field.split('.', 1)
        if field in data:
            return get_field_value(data[field], rest)
    return None, False
