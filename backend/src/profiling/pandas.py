import numpy as np
from types import SimpleNamespace
import logging
import pandas as pd
from ..clients import dask

pm = SimpleNamespace()

# Profiling methods
pm.cardinality              = lambda series : len(series.unique())
pm.row_count                = lambda series : len(series.dropna())
pm.min                      = lambda series : series.min()
pm.max                      = lambda series : series.max()
pm.str_min                  = lambda series : series.str.len().min()
pm.str_max                  = lambda series : series.str.len().max()
pm.null_values              = lambda series : series.isna().sum()
pm.distinct                 = lambda series : pm.cardinality(series) == pm.row_count(series)
pm.uniqueness               = lambda series : pm.cardinality(series) / pm.row_count(series)
pm.data_type                = lambda series : str(series.dtype)


def decorate(func, name):
    def inner(series):
        try:
            res = func(series)
            if hasattr(res, 'compute'):
                return res.compute()
            return res
        except Exception as e:
            logging.warning(f"Failed apply profiling function `{name}` to column {series.name} because: {e}")
            return np.nan
    return inner


def get_profile(ddf, ddf_name):
    profile = {}
    futures = {}

    functions = list(pm.__dict__.items())
    for name, function in functions:
        futures[name] = dask.get_client().map(decorate(function, name), [ddf[col] for col in ddf.columns])
    
    for name, _ in functions:
        profile[name] = dask.get_client().gather(futures[name])

    return pd.DataFrame.from_dict(profile, orient='index', columns=ddf.columns)


def np_converter(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.dtype):
        return str(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return obj


def convert_to_python_types(profile):
    for key in profile:
        value = profile[key]
        if isinstance(value, list):
            new_value = []
            for v in value:
                new_value.append(np_converter(v))
        else:
            new_value = np_converter(value)
        profile[key] = new_value
    return profile
