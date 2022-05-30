import os

from sherlock.deploy.model import SherlockModel
from sherlock.features.paragraph_vectors import initialise_pretrained_model, initialise_nltk
from sherlock.features.preprocessing import prepare_feature_extraction
from sherlock.features.word_embeddings import initialise_word_embeddings


class ProfileSherlock:

    def __init__(self):
        self.model = None
        self.init_sherlock()

    def predict(self, df):
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


