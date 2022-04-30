from src.indexes.laion1.indexes import benchmark, indexes_names

import matplotlib.pyplot as plt

benchmark.CreateAllTables()
times = benchmark.PopulateAllTables()

print(times)

times_arr = []
for n in indexes_names:
    times_arr.append(times[n])

names = indexes_names

fig = plt.figure(figsize=(15, 5))
plt.bar(names, times_arr)
plt.title('Сравнение времени INSERT запроса')
plt.ylabel('с')
fig.savefig('output/laion1/insert.png')

