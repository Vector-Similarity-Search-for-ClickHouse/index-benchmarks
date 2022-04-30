from src.common.clientwrapper import ClientWrapper

# Setup
host = 'localhost'
port = 8081
client_path = '/home/ubuntu/nikitaevs/ClickHouse/build/programs'
index_name = 'ivfflat'
client = ClientWrapper(host, port, client_path, index_name)
