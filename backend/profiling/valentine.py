from valentine import valentine_match, valentine_metrics
from valentine.algorithms import Coma

def match(df1, df2):
    return valentine_match(df1, df2, Coma(strategy="COMA_OPT"))