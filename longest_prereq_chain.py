import requests, re, time, sys, pickle, os, json
from collections import deque, defaultdict
from ufds import UFDS

## CONSTANTS
"""
YEARS: the years you want to pull data from
LOG_ENABLED: print log statements?
SAMPLE_USED: use sample modules?
PICKLE_USED: use cached pickle files?
PRECLUSIONS_CONTRACTED: if B is A's preclusion, do we include them as a single vertex?
MODULE_INFO_REVISITED: pull data from moduleInfo endpoint for more information?
SAMPLE_MODULE_CODES: self-explanatory
"""
YEARS = [2017, 2018, 2019, 2020, 2021, 2022]
LOG_ENABLED = True
SAMPLE_USED = False
PICKLE_USED = True
PRECLUSIONS_CONTRACTED = False
MODULE_INFO_REVISITED = False
SAMPLE_MODULE_CODES = [
        'MA5236', 'MA5209', 'MA4266', 'MA3209',
        'CS1010S', 'CS3244', 'CS2040S', 'CS1010E',
        'CS3243', 'CS2020', 'CS2030', 'CS2030S',
        'DSA1101', 'ST3247', 'MA1100', 'CS1231',
        'MA1100T', 'MA2202', 'MA3201', 'MA5203',
        'MA5202', 'MA1101R', 'MA2001', 'MA2002',
        'MA1102R', 'MA2101', 'MA2101S', 'MA2108', 'MA2108S']

def log(*msg, showTime=True):
    if LOG_ENABLED:
        if showTime:    print(round(time.time() - t, 4), '\t'.rjust(5), *msg)
        else:           print(*msg)

def emergencyDump():
    os.makedirs('pickle', exist_ok=True)
    try:
        pickle.dump(graph, open(os.path.join('pickle', 'graph.pkl'), 'wb+'))
        pickle.dump(meta, open(os.path.join('pickle', 'meta.pkl'), 'wb+'))
        pickle.dump(unionFind.p, open(os.path.join('pickle', 'ufds_p.pkl'), 'wb+'))
        pickle.dump(unionFind.rank, open(os.path.join('pickle', 'ufds_rank.pkl'), 'wb+'))
        pickle.dump(unionFind.set, open(os.path.join('pickle', 'ufds_set.pkl'), 'wb+'))
        pickle.dump(moduleCodes, open(os.path.join('pickle', 'moduleCodes.pkl'), 'wb+'))
        pickle.dump(moduleInfos, open(os.path.join('pickle', 'moduleInfos.pkl'), 'wb+'))
    except:
        print('\n\nFailed exporting some, sad.')

def contract(mod1, mod2):
    if mod1 not in modToIndex or mod2 not in modToIndex: return
    u, v = modToIndex[mod1], modToIndex[mod2]
    unionFind.union(u, v)

def addEdge(graph, mod1, mod2, meta=None, info=''):
    if mod1 not in modToIndex or mod2 not in modToIndex: return
    u, v = modToIndex[mod1], modToIndex[mod2]
    u, v = unionFind.findSet(u), unionFind.findSet(v)
    if u not in graph: graph[u] = set()
    graph[u].add(v)
    if type(meta) == dict:
        mcu, mcv = moduleCodes[u], moduleCodes[v]
        if mcu not in meta: meta[mcu] = {}
        meta[mcu][mcv] = meta[mcu].get(mcv, set()) | {info}

def populateGraph(graph, moduleCode, prereqTree, meta=None, info=''):
    if type(prereqTree) == str:
        return addEdge(graph, moduleCode, prereqTree)
    for op in ['and', 'or']:
        for req in prereqTree.get(op, []):
            populateGraph(graph, moduleCode, req, meta, info)

def getSources(graph):
    indeg = {}
    for v in graph:
        for w in graph[v]:
            if w not in indeg: indeg[w] = 0
            indeg[w] += 1
    return [v for v in graph if v not in indeg]

def checkCycles(graph):
    cycle = [False, []]
    def dfs(u, vis=set()):
        if u in vis or cycle[0]:
            if not cycle[0]: cycle[1] = vis
            cycle[0] = True
            return
        new = vis | {u}
        if u in graph:
            for v in graph[u]:
                dfs(v, new)
    for v in graph:
        dfs(v)
    if cycle[0]:
        raise Exception('Cycle detected!', cycle[1])

def bfs(graph, srcs):
    D = defaultdict(lambda: (float('inf'), ()))
    q = deque()
    for src in srcs:
        q.append(src)
        D[src] = (0, (src,))
    vis = set()
    while q:
        u = q.popleft()
        if u in vis: continue
        if u in graph:
            for v in graph[u]:
                if D[v][0] >= D[u][0]-1:
                    D[v] = (D[u][0]-1, D[u][1] + (v,))
                    q.append(v)
    #for k, v in D.items(): log(' -> '.join(map(lambda x: moduleCodes[x], v[1][::-1])), showTime=False) if len(v[1]) > 1 else None
    return D

try:
    # Start defining some useful variables
    t = time.time()
    moduleRegex = r'[A-Z]{2,4}[0-9]{4}[A-Z]*'
    moduleCodes = set()

    # Create JSON directory
    log(f'Creating JSON directory...')
    os.makedirs('json', exist_ok=True)
    for dirs in ['moduleList', 'moduleCode', 'moduleInfo']:
        os.makedirs(os.path.join('json', dirs), exist_ok=True)
    for year in YEARS:
        acadYear = f'{year}-{year+1}'
        os.makedirs(os.path.join('json', 'moduleCode', acadYear), exist_ok=True)
    log(f'Done creating JSON directory...')

    # Create graph
    graph, meta = {}, {}
    if PICKLE_USED:
        graph = pickle.load(open(os.path.join('pickle', 'graph.pkl'), 'rb'))
        meta = pickle.load(open(os.path.join('pickle', 'meta.pkl'), 'rb'))

    # Get all module codes
    for year in YEARS:
        acadYear = f'{year}-{year+1}'
        log(f'Getting module list from {acadYear}...')
        if not PICKLE_USED:
            try:
                moduleCodes |= {mod['moduleCode'] for mod in json.load(open(os.path.join('json', 'moduleList', f'{acadYear}.json'), 'r'))}
            except:
                response = requests.get(f'https://api.nusmods.com/v2/{acadYear}/moduleList.json')
                if response.ok:
                    moduleCodes |= {mod['moduleCode'] for mod in response.json()}
                    json.dump(response.json(), open(os.path.join('json', 'moduleList', f'{acadYear}.json'), 'w+'))
    if SAMPLE_USED:
        moduleCodes = SAMPLE_MODULE_CODES
    elif PICKLE_USED:
        moduleCodes = pickle.load(open(os.path.join('pickle', 'moduleCodes.pkl'), 'rb'))
    moduleCodes = sorted(moduleCodes)
    modToIndex = dict(map(reversed, enumerate(moduleCodes)))
    log(f'Found {len(moduleCodes)} module codes!')

    # Create UFDS
    unionFind = UFDS(len(moduleCodes))
    if PICKLE_USED:
        for attr in ['p', 'rank', 'set']:
            setattr(unionFind, attr, pickle.load(open(os.path.join('pickle', f'ufds_{attr}.pkl'), 'rb')))
    log(f'Creating UFDS...')

    ## My best attempt, not 100% accurate lol
    # Start with contracting edges if needed, makes use of the UFDS
    moduleInfos = []
    if PICKLE_USED:
        moduleInfos = pickle.load(open(os.path.join('pickle', 'moduleInfos.pkl'), 'rb'))
    if PRECLUSIONS_CONTRACTED:
        for year in YEARS:
            acadYear = f'{year}-{year+1}'
            log(f'Getting all-module information from {acadYear} to contract edges...')
            if not PICKLE_USED:
                try:
                    moduleInfos.append(json.load(open(os.path.join('json', 'moduleInfo', f'{acadYear}.json'), 'r')))
                except:
                    response = requests.get(f'https://api.nusmods.com/v2/{acadYear}/moduleInfo.json')
                    if response.ok: [moduleInfos.append(response.json()), json.dump(moduleInfos[-1], open(os.path.join('json', 'moduleInfo', f'{acadYear}.json'), 'w+'))]
                    else:           continue
                for moduleInfo in moduleInfos[-1]:
                    moduleCode = moduleInfo['moduleCode']
                    if moduleCode in moduleCodes:
                        preclusions = re.findall(moduleRegex, moduleInfo.get('preclusion', ''))
                        for preclusion in preclusions:
                            contract(moduleCode, preclusion)
    log('Populating edges of graph...')
    # Then connect the edges accordingly
    for year in YEARS:
        acadYear = f'{year}-{year+1}'
        log(f'Getting per-module information from {acadYear}...')
        if not PICKLE_USED:
            for moduleCode in moduleCodes:
                prereqTree, fulfillRequirements = {}, []
                try:
                    r = json.load(open(os.path.join('json', 'moduleCode', f'{acadYear}', f'{moduleCode}.json'), 'r'))
                    prereqTree = r.get('prereqTree', {})
                    fulfillRequirements = r.get('fulfillRequirements', [])
                except:
                    response = requests.get(f'https://api.nusmods.com/v2/{acadYear}/modules/{moduleCode}.json')
                    if response.ok:
                        prereqTree = response.json().get('prereqTree', {})
                        fulfillRequirements = response.json().get('fulfillRequirements', [])
                        json.dump(response.json(), open(os.path.join('json', 'moduleCode', f'{acadYear}', f'{moduleCode}.json'), 'w+'))
                # moduleCode -> prereq
                populateGraph(graph, moduleCode, prereqTree, meta, acadYear)
                for req in fulfillRequirements:
                    # req -> moduleCode
                    addEdge(graph, req, moduleCode, meta, acadYear)
    # Get extra info from moduleInfo as well
    if MODULE_INFO_REVISITED:
        for idx, year in enumerate(YEARS):
            acadYear = f'{year}-{year+1}'
            log(f'(Re)visiting all-module information from {acadYear}...')
            if not PICKLE_USED:
                if not PRECLUSIONS_CONTRACTED: # this means moduleInfos is still an empty list!
                    try:
                        moduleInfos.append(json.load(open(os.path.join('json', 'moduleInfo', f'{acadYear}.json'), 'r')))
                    except:
                        response = requests.get(f'https://api.nusmods.com/v2/{acadYear}/moduleInfo.json')
                        if response.ok: [moduleInfos.append(response.json()), json.dump(moduleInfos[-1], open(os.path.join('json', 'moduleInfo', f'{acadYear}.json'), 'w+'))]
                        else:           continue
                for moduleInfo in moduleInfos[idx]:
                    moduleCode = moduleInfo['moduleCode']
                    if moduleCode in moduleCodes:
                        # Quite a strong assumption about how I obtain prerequisites from moduleInfo but oh well :(
                        prerequisites = re.findall(moduleRegex, moduleInfo.get('prerequisite', ''))
                        for prerequisite in prerequisites:
                            # moduleCode -> prereq
                            addEdge(graph, moduleCode, prerequisite, meta, acadYear)

    # Sanity check, any cycles?
    log('Checking for cycles...')
    checkCycles(graph)
    log('Done checking for cycles...')

    # Get sources for BFS
    log('Getting sources for BFS...')
    srcs = getSources(graph)
    log('BFS source list obtained...')

    # Run toposort+BFS and get longest paths
    paths = {}
    log('Running BFS...')
    bfsResult = bfs(graph, srcs)
    for v in bfsResult.values():
        paths[-v[0]+1] = paths.get(-v[0]+1, []) + [' -> '.join(map(lambda x: '{' + ', '.join(map(lambda idx: moduleCodes[idx], unionFind.set[x])) + '}', v[1]))]
    maxD = max(paths)
    log('Path(s) obtained!...')
    for i in range(4, maxD): # length 4+ for curiousity??
        for path in paths.get(i, []):
            log(path, showTime=False)
        log('='*30, showTime=False)
    for path in paths[maxD]:
        print(path)
    log(f'All done! Found longest distance of {maxD}')
except Exception as e:
    emergencyDump()
    raise e
