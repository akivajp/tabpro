'''
Set the value of a field in a nested dictionary.
'''

from collections import OrderedDict
from icecream import ic

def set_nested_field_value(
    data: OrderedDict | list,
    field: str,
    value: any,
):
    if isinstance(field, str) and  '.' in field:
        field, rest = field.split('.', 1)
        if isinstance(data, dict):
            sub_data = data.get(field)
        elif isinstance(data, list):
            field = int(field)
            if field < len(data):
                sub_data = data[field]
            else:
                sub_data = None
        if not isinstance(sub_data, dict):
            do_create_dict = True
            if isinstance(sub_data, list):
                # NOTE: 続くフィールド文字列が数字の場合は、リストの要素として扱う
                field2 = rest.split('.', 1)[0]
                if field2.isdigit():
                    do_create_dict = False
            if do_create_dict:
                data[field] = OrderedDict()
        set_nested_field_value(data[field], rest, value)
    else:
        try:
            if isinstance(data, dict):
                data[field] = value
            elif isinstance(data, list):
                data[int(field)] = value
        except:
            ic(data, field, value)
            raise
