import json

from typing import Tuple, TextIO, List


# Format: IP, port
def parse_ip(ip_string: str) -> Tuple[str, int]:
    """
    Parses an IP:PORT-string into a tuple that has the IP as string and the port as integer.
    """
    split = ip_string.split(":")
    return split[0], int(split[1])
