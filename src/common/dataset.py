import time

from tqdm import tqdm

class Dataset:
    def __init__(self, client, name, dim):
        self.__client = client
        self.__name = name
        self.__dim = dim
    
    def Create(self):
        self.__client.createWithoutIdx(self.__name, self.__dim)
        
    def Insert(self, paths):
        before = time.time()
        
        for path in tqdm(paths):
            self.__client.insertFromFile(self.__name, path)
        self.__client.optimize(self.__name)
        
        after = time.time()
        return after - before
        
    def Drop(self):
        self.__client.drop(self.__name)
