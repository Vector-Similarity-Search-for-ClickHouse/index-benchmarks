import subprocess
from clickhouse_driver import Client


class ClientWrapper:
    def __init__(self, host, port, client_path, index_name):
        self.__host = host
        self.__port = port
        self.__client_path = client_path
        self.__index_name = index_name
        
        self.__client = Client(host=host, \
                               port=port, \
                               connect_timeout=1000, \
                               send_receive_timeout=1000, \
                               sync_request_timeout=1000)
    
    def getCreateWithIdxFmt(self, name, key_str, dim, params, granularity):
        tuple_inner = ', '.join(['Float32' for _ in range(dim)])
        params_str = ', '.join([str(i) for i in params])
        if len(params_str) > 0:
            params_str = ', ' + params_str

        if key_str != '':
            key_str = f"""'{key_str}'"""

        base_str = f"""
        CREATE TABLE {name}
        (
            id UInt64,
            url String, 
            embedding Tuple({tuple_inner}),
            INDEX x embedding TYPE {self.__index_name}({key_str}{params_str}) GRANULARITY {granularity}
        )
        ENGINE = MergeTree
        ORDER BY id
        """
        return base_str
    
    def getCreateWithoutIdxFmt(self, name, dim):
        tuple_inner = ', '.join(['Float32' for _ in range(dim)])

        base_str = f"""
        CREATE TABLE {name}
        (
            id UInt64,
            url String, 
            embedding Tuple({tuple_inner})
        )
        ENGINE = MergeTree
        ORDER BY id
        """
        return base_str
    
    def getDropFmt(self, name):
        base_str = f"""
        DROP TABLE IF EXISTS {name}
        """
        return base_str
    
    def getInsertFromFileFmt(self, name, file):
        base_str = f"""
        INSERT INTO {name}
        FROM INFILE \'{file}\'
        """
        return base_str
    
    def getInsertFromTableFmt(self, name, table_name):
        base_str = f"""
        INSERT INTO {name}
        SELECT * FROM {table_name}
        """
        return base_str
    
    def getSelectFmt(self, name, what, x, distance):
        base_str = f"""
        SELECT {what}
        FROM {name}
        WHERE L2Distance(embedding, tuple%(x)s) < %(d)s
        """
        return base_str
    
    def getOptimizeFmt(self, name):
        base_str = f"""
        OPTIMIZE TABLE {name}
        """
        return base_str
    
    def createWithIdx(self, name, key_str, dim=3, params=[], granularity=1):
        req = self.getCreateWithIdxFmt(name, key_str, dim, params, granularity)
        return self.__client.execute(req)

    def createWithoutIdx(self, name, dim=3):
        req = self.getCreateWithoutIdxFmt(name, dim)
        return self.__client.execute(req)

    def drop(self, name):
        req = self.getDropFmt(name)
        return self.__client.execute(req)
    
    def optimize(self, name):
        req = self.getOptimizeFmt(name)
        return self.__client.execute(req)

    def insertFromFile(self, name, file):
        req = self.getInsertFromFileFmt(name, file)
        subprocess.call(f"./clickhouse-client --port {self.__port} --query=\"{req}\"", 
                        cwd=f'{self.__client_path}', 
                        shell=True)
        
    def insertFromTable(self, name, table_name):
        req = self.getInsertFromTableFmt(name, table_name)
        subprocess.call(f"./clickhouse-client --port {self.__port} --query=\"{req}\"", 
                        cwd=f'{self.__client_path}', 
                        shell=True)
        
    def queryStat(self):
        time = self.__client.last_query.elapsed
        return time

    def select(self, name, what, x, distance):
        req = self.getSelectFmt(name, what, x, distance)
        return self.__client.execute(req, {'x': x, 'd': distance})
