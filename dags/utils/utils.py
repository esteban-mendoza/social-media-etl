
def flatten_dict(d, parent_key='', sep='_'):
    """
    Recursively flattens all levels of a dictionary and concatenates all the keys.

    Args:
        d (dict): The dictionary to be flattened.
        parent_key (str, optional): The parent key for recursive calls. Defaults to ''.
        sep (str, optional): The separator to be used for concatenating keys. Defaults to '_'.

    Returns:
        dict: The flattened dictionary.
    """
    flattened_dict = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            flattened_dict.update(flatten_dict(v, new_key, sep))
        else:
            flattened_dict[new_key] = v
    return flattened_dict
