# Description: Get the value of a field in a dictionary.

from collections import OrderedDict

from icecream import ic

def get_nested_field_value(
    data: OrderedDict | list,
    #field: str,
    field: str | int,
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
        if isinstance(field, int):
            if field in data:
                return data[field], True
            str_field = str(field)
            if str_field in data:
                return data[str_field], True
        elif isinstance(field, str):
            if field.isdigit():
                index = int(field)
                if index in data:
                    return data[index], True
            if '.' in field:
                field, rest = field.split('.', 1)
                if field in data:
                    return get_nested_field_value(data[field], rest)
        else:
            raise TypeError(f'unsupported field type: {type(field)}')
    return None, False
