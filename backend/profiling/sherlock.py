from sherlock.deploy.predict_sherlock import predict_sherlock
from sherlock.features.preprocessing import extract_features, convert_string_lists_to_lists, prepare_feature_extraction

def predict(df):
    lists = df.head(100).transpose().apply(lambda x: x.to_list(), axis=1)
    return predict_sherlock(extract_features(lists), nn_id='sherlock' )