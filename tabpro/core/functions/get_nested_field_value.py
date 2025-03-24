# Description: Get the value of a field in a dictionary.

from collections import OrderedDict

from icecream import ic

def get_nested_field_value(
    data: OrderedDict | list,
    field: str,
):
    if isinstance(data, list):
        #ic(data, field)
        if field.isdigit():
            index = int(field)
            if index < len(data):
                return data[index], True
            return None, False
        if '.' in field:
            field, rest = field.split('.', 1)
            if field.isdigit():
                index = int(field)
                if index < len(data):
                    return get_nested_field_value(data[index], rest)
    if isinstance(data, dict):
        if field in data:
            return data[field], True
        if '.' in field:
            field, rest = field.split('.', 1)
            if field in data:
                return get_nested_field_value(data[field], rest)
    return None, False
