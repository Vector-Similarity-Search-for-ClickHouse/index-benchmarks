from src.common.benchmark import Benchmark
from src.common.client import client
from src.datasets.laion1.dataset import name, dim

indexes = [
    {
        'label': 'IVF16Flat_laion1',
        'idx_str': 'IVF16,Flat',
        'params': [],
        'granularity': 1,
        'populate': False,
    },
    {
        'label': 'OPQ32_128IVF16PQ32_laion1',
        'idx_str': 'OPQ32_128,IVF16,PQ32',
        'params': [],
        'granularity': 8,
        'populate': True,
    },
]
indexes_names = [index['label'] for index in indexes]

benchmark = Benchmark(client, name, dim, indexes)
