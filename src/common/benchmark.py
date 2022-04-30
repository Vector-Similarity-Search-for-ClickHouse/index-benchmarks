import time

from tqdm import tqdm

class Benchmark:
    def __init__(self, client, dataset_name, dim, indexes):
        self.__client = client 
        self.__dataset_name = dataset_name
        self.__dim = dim
        self.__indexes = indexes
        
    def CreateAllTables(self):
        for index in tqdm(self.__indexes):
            if index['populate']:
                self.__client.createWithIdx(index['label'], \
                                            index['idx_str'], \
                                            self.__dim, \
                                            index['params'], \
                                            index['granularity'])
    def DropAllTables(self):
        for index in tqdm(self.__indexes):
            self.__client.drop(index['label'])
            
    def PopulateAllTables(self):
        times = {}
        for index in tqdm(self.__indexes):
            if index['populate']:
                before = time.time()
                self.__client.insertFromTable(index['label'], self.__dataset_name)
                after = time.time()
                times[index['label']] = after - before
            
        return times
    
    def SelectFromAllTables(self, what, x, distance):
        return self.selectFrom(self.__indexes, what, x, distance)

    def SelectFromTables(self, table_names, what, x, distance):
        table_names = set(table_names)

        indexes = []
        for index in self.__indexes:
            if index['label'] in table_names:
                indexes.append(index)
        
        return self.selectFrom(indexes, what, x, distance)

    def selectFrom(self, indexes, what, x, distance):
        results = {}
        times = {}
        
        results[self.__dataset_name] = self.__client.select(self.__dataset_name, what, x, distance)
        times[self.__dataset_name] = self.__client.queryStat()
        for index in tqdm(indexes):
            label = index['label']
            results[label] = self.__client.select(label, what, x, distance)
            times[label] = self.__client.queryStat()
        
        return times, results
