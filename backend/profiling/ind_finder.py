import logging
import os

from backend.discovery import relation_types
from backend.discovery.crud import create_relation, set_relation_properties

from typing import Set

from itertools import product
from dataclasses import dataclass
from backend.search import io_tools


@dataclass(frozen=True)
class Ref:
    table: str
    columns: frozenset

    def __repr__(self):
        return "&".join([f"{self.table}/{column}" for column in self.columns])

    def __iter__(self):
        return self.columns.__iter__()


def find_inclusion_dependencies(table_paths: Set[str]) -> None:
    """
    Finds unary INDs between the given tables.
    """
    columns = {}
    for path in table_paths:
        df = io_tools.get_df(path)
        for c in df:
            ref = Ref(path, frozenset({c}))
            columns[ref] = df[c].dropna()

    # Start selecting suitable candidates for INDs
    cands = {c: set() for c in columns}
    for A, B in product(columns, repeat=2):
        col_A = columns[A]
        col_B = columns[B]

        bl_same_table = A.table == B.table
        bl_same_column = col_A is col_B
        bl_different_dtypes = col_A.dtype != col_B.dtype
        bl_empty = len(col_A) == 0
        if any([bl_same_column, bl_different_dtypes, bl_same_table, bl_empty]):
            continue

        # Check if column fully contains other column
        A_in_B = col_A.isin(col_B)
        if A_in_B.all():
            logging.info(f"{A}-->{B}")
            cands[A].add(B)

    # Add the found unary INDs to Neo4J
    inds = {c: s for c in cands for s in cands[c]}
    for frm, to in inds.items():
        create_relation(repr(frm), repr(to), relation_types.FOREIGN_KEY_IND)
        set_relation_properties(repr(frm), repr(to), relation_types.FOREIGN_KEY_IND, from_id=repr(frm), to_id=repr(to))
