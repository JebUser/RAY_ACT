import ray
import time

def makeSet(v):
    p[v], rango[v] = v, 0

def findSet(v):
    ans = None
    if v == p[v]: ans = v
    else:
        p[v] = findSet(p[v])
        ans = p[v]
    return ans

def unionSet(u, v):
    u, v = findSet(u), findSet(v)

    if u != v:
        if rango[u] < rango[v]: u, v = v, u
        p[v] = u
        if rango[u] == rango[v]: rango[u] += 1

def kruskal(aristas,n):
    global p, rango
    mst = []
    connected = 0
    p, rango = [0 for _ in range(100)], [0 for _ in range(100)]

    for i in range(n):
        makeSet(i)
    i = 0
    while i != len(aristas) and connected != n-1:
        u,v = aristas[i][0],aristas[i][1]

        if findSet(u) != findSet(v):
            mst.append(aristas[i])
            unionSet(u,v)
            connected +=1
        i+=1

    return mst

@ray.remote
def makeSet_rem(v):
    p[v], rango[v] = v, 0
@ray.remote
def findSet_rem(v):
    ans = None
    f v == p[v]: ans = v
    else:
        p[v] = findSet(p[v])
        ans = p[v]
    return ans
@ray.remote
def unionSet_rem(u, v):
    u, v = findSet(u), findSet(v)

    if u != v:
        if rango[u] < rango[v]: u, v = v, u
        p[v] = u
        if rango[u] == rango[v]: rango[u] += 1
@ray.remote
def kruskal_rem(aristas,n):
    global p, rango
    mst = []
    connected = 0
    p, rango = [0 for _ in range(100)], [0 for _ in range(100)]

    for i in range(n):
        makeSet(i)
    i = 0
    while i != len(aristas) and connected != n-1:
        u,v = aristas[i][0],aristas[i][1]

        if findSet(u) != findSet(v):
            mst.append(aristas[i])
            unionSet(u,v)
            connected +=1
        i+=1

    return mst

def correrlocal():
    tiempo_inicial = time.time()
    aristas = [[0,1,3], [1,2,5], [2,3,6], [3,4,3], [1,4,2]]
    aristas.sort(key = lambda x: x[2])
    print(kruskal(aristas, 5))
    duracion = time.time() - tiempo_inicial
    print("Tiempo de ejecucion local: {}".format(duracion))

def correr_remoto():
    ray.init(address='auto')
    tiempo_inicial = time.time()
    aristas = [[0,1,3], [1,2,5], [2,3,6], [3,4,3], [1,4,2]]
    aristas.sort(key = lambda x: x[2])
    print(kruskal(aristas, 5)) 
    duracion = time.time() - tiempo_inicial
    print("Tiempo de ejecucion remoto: {}".format(duracion))

correrlocal()
correr_remoto()
