import pytest
import pandas as pd

@pytest.fixture
def train_df():
    df = pd.read_csv("https://covid.ourworldindata.org/data/owid-covid-data.csv")
    return df