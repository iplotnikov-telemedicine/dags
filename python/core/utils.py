def get_fields_for(table_type, fields_map):
    if table_type not in ['source', 'target']:
        raise ValueError('Wrong type for table to get field names')
    _fields = [getattr(item, table_type) for item in fields_map]
    if not _fields:
        print('Error: Number of columns less than 1')
        raise ValueError(f'No columns to map from {table_type.upper()}')
    columns = ','.join(_fields)

    return columns