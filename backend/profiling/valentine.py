import os
from typing import Dict, Any, Tuple

import pandas as pd
from valentine.algorithms import Coma

from backend.discovery import relation_types
from backend.discovery.crud import create_relation, set_relation_properties
from backend.search import mongo_tools
from valentine import valentine_match

threshold = float(os.environ['VALENTINE_THRESHOLD'])


def match(df1: pd.DataFrame, df2: pd.DataFrame) -> Dict[Tuple[Any, str], Tuple[Any, str]]:
    return valentine_match(df1, df2, Coma(strategy="COMA_OPT"))


def process_match(table1_path: str, table2_path: str, matches: Dict[Tuple[Any, str], Tuple[Any, str]]) -> None:
    node_ids_t1 = mongo_tools.get_node_ids(table1_path)
    node_ids_t2 = mongo_tools.get_node_ids(table2_path)
    for ((_, col_from), (_, col_to)), similarity in matches.items():
        if similarity > threshold:
            create_relation(node_ids_t1[col_from], node_ids_t2[col_to], relation_types.MATCH)
            set_relation_properties(node_ids_t1[col_from], node_ids_t2[col_to], relation_types.MATCH, coma=similarity,
                                    from_id=node_ids_t1[col_from], to_id=node_ids_t2[col_to])
