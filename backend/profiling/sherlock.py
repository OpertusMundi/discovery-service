from typing import Any

import pandas as pd
from sherlock.deploy.model import SherlockModel


class ProfileSherlock:
    """
    Class used for executing Sherlock profiles.
    """

    def __init__(self):
        self.model: SherlockModel = None
        self.init_sherlock()

    def predict(self, df: pd.DataFrame) -> Any:
        lists = df.head(100).transpose().apply(lambda x: x.to_list(), axis=1)
        print(lists)
        return self.model.predict(lists, 'sherlock')

    def init_sherlock(self):
        # prepare_feature_extraction()
        # initialise_word_embeddings()
        # initialise_pretrained_model(400)
        # initialise_nltk()

        self.model = SherlockModel()
        self.model.initialize_model_from_json(with_weights=True, model_id="sherlock")
