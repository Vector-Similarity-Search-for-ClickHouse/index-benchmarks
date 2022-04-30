from src.datasets.laion1.dataset import dataset

# Create and insert
paths = ['/home/ubuntu/disk/makarov/laion/laion0.csv']
dataset.Create()
dataset.Insert(paths)
