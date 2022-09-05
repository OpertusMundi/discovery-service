import numpy as np
import logging
import pandas as pd

from typing import Dict, Any

from types import SimpleNamespace

from ..clients import dask

pm: SimpleNamespace = SimpleNamespace()

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


def get_profile_column(series: pd.Series, python_types: bool=True) -> Dict[str, Any]:
    """
    Gets a profile for the given series, according to the available profiling methods in the "pm" namespace.

    The python_types flag can be set to make sure that the resulting values in the profile are native python types.
    """ 
    profile = {}
    for name, function in pm.__dict__.items():
        try:
            val = function(series)
            profile[name] = val
        except Exception as e:
            logging.warning(f"Failed to apply profiling function `{name}` to column {series.name} because: {e}")
            profile[name] = ''

    if python_types:
        profile = convert_to_python_types(profile)

    return profile


def get_profile(df: pd.DataFrame, name: str, python_types: bool=True) -> Dict[str, Any]:
    """
    Gets profiles per column for the entire dataframe. The name corresponds to the name of the dataframe/table.

    The python_types flag can be set to make sure that the resulting values in the profile are native python types.
    """ 
    profile = {}
    for column_name in df.columns:
        profile[f"{name}/{column_name}"] = get_profile_column(df[column_name], python_types=python_types)
    return


# def get_profile(ddf, ddf_name):
#     profile = {}
#     futures = {}
#
#     functions = list(pm.__dict__.items())
#     for name, function in functions:
#         futures[name] = dask.get_client().map(decorate(function, name), [ddf[col] for col in ddf.columns])
#
#     for name, _ in functions:
#         profile[name] = dask.get_client().gather(futures[name])
#
#     return pd.DataFrame.from_dict(profile, orient='index', columns=ddf.columns)
    return profile


def np_converter(obj: Any) -> Any:
    """
    Converts a numpy type to an equivalent python type, used mostly for serialization.
    """ 
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


def convert_to_python_types(profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converts the numpy types in the given profile to python types.
    """ 
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
