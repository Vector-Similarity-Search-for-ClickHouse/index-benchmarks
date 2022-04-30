from src.common.dataset import Dataset
from src.common.client import client

# Setup dataset
name = 'laion1'
dim = 512
dataset = Dataset(client, name, dim)
