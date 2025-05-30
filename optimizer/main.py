from optimize import optimize, search_candidates
from examples import *

tasks = [ 
    covid(client_mem=10000000, server_mem=1000000000), 
    sdss(client_mem=10000000, server_mem=1000000000),
    immens(client_mem=10000000, server_mem=1000000000, latency=FAST, switchon=MID), 
    flights(client_mem=10000000, server_mem=1000000000, latency=FAST, switchon=MID),
    liquor(client_mem=10000000, server_mem=1000000000, latency=FAST, switchon=MID)
]

for task in tasks:
    res = optimize(task)
    print(res)
    print("======================")
