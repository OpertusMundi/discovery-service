import json

from typing import Tuple, TextIO, List


# Format: IP, port
def parse_ip(ip_string: str) -> Tuple[str, int]:
    """
    Parses an IP:PORT-string into a tuple that has the IP as string and the port as integer.
    """
    split = ip_string.split(":")
    return split[0], int(split[1])


# Format: (dependant_table_path/column, referenced_table_path/column)
def parse_binder_results(data: TextIO) -> List[Tuple[str, str]]:
    """
    Parses results obtained from the BINDER algorithm ran by Metanome.
    """
    records = []
    buffer = ""
    for line in data.splitlines():
        buffer += line
        try:
            records.append(json.loads(buffer))
            buffer = ""
        except:
            pass

    dependants = [(x['dependant']['columnIdentifiers'][0]['tableIdentifier'],
                   x['dependant']['columnIdentifiers'][0]['columnIdentifier']) for x in records]
    references = [(x['referenced']['columnIdentifiers'][0]['tableIdentifier'],
                   x['referenced']['columnIdentifiers'][0]['columnIdentifier']) for x in records]

    # Ignore when dependant and referenced table are the same, we want inter-table relationships
    constraints = [('/'.join(x[0]), '/'.join(x[1])) for x in zip(dependants, references) if x[0][0] != x[1][0]]

    return constraints
