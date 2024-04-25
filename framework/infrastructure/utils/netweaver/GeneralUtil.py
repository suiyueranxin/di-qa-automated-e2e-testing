

def dict_capital_to_upper(dict_info):
    """return a new dict object with recursively changed keys to upper"""
    new_dict = {}
    for i, j in dict_info.items():
        new_dict[i.upper()] = j
    return new_dict
