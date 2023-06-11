# deteksi pasangan join

# import
from helper import generalHelper

# fungs-fungsi

def setInDegreeInNodes(session):
    q_in_degree = '''        
                match (x)-[i {rel:'Sequence'}]->(a:RefModel)
                with a, collect(i) as ilist, count(i) as i_degree
                set a.i_degree = i_degree;
                '''
    session.run(q_in_degree)
    return None

# Deteksi JoinGW terdekat
def setJoinNodes(session):
    # Join detection
    q_join = '''
            MATCH (a:RefModel)-[r:DFG]->(n)
            WHERE n.i_degree > 1
            set r.join = True, n.join_gate = True;
            '''
    records = session.run(q_join)

    return None

def getAllJoinNodes(session):
    q_getAllJoinNodes = '''
            OPTIONAL MATCH (a:RefModel {join_gate:True})
            RETURN collect(a.Name) as joinNodes
    '''
    result = session.run(q_getAllJoinNodes)

    for rec in result:
        if rec is not None:
            for joinNodes in rec:
                return joinNodes
        else:
            return None

# new
def getAllJoinNodesAndItsExitNodes(session):
    q_getAllJoinNodes = '''
            OPTIONAL MATCH (x)-->(a:RefModel {join_gate:True})<--(y)
            WHERE (x)-[:CONCURRENT]-(y) and ID(x) < ID(y)
            RETURN collect(DISTINCT[x.Name,y.Name,a.Name])
    '''
    result = session.run(q_getAllJoinNodes)

    for rec in result:
        if rec is not None:
            for joinNodes in rec:
                return joinNodes  # [[exit0,exit1,joinNode],...,[exit0,exit1,joinNode]]
        else:
            return None

# print(getAllJoinNodes())

def checkJoinNodeDistanceFrom(conPair, joinNode):
    initial0 = conPair[0]
    initial1 = conPair[1]
    target = joinNode
    distanceFromConPair0 = calculateLength(initial0, target)
    distanceFromConPair1 = calculateLength(initial1, target)
    if (distanceFromConPair0 is None) or (distanceFromConPair1 is None):
        return None
    else:
        return distanceFromConPair0 + distanceFromConPair1

# temukan join node terdekat dari sepasang node konkuren (conPair, joinNode)
def findTheClosestJoinNodeFromConcurrentPairWithItsDistance(concurrentPair, joinNodes):
    conPair = concurrentPair
    print("conPair: ", conPair)
    # 1. temukan semua node join yang valid dengan conPair

    # 2. temukan node join yang terjauh dari conPair sebagai yg terpilih

    # Cari yang terdekat
    theClosestDistance = 1000
    theClosestJoinNode = None
    for joinNode in joinNodes:
        distance = checkJoinNodeDistanceFrom(conPair, joinNode)
        if distance is None:
            pass
        elif distance < theClosestDistance:
            theClosestDistance = distance
            theClosestJoinNode = joinNode
    return concurrentPair, theClosestJoinNode, theClosestDistance

def findTheClosestHierarchy(concurrentPairs, allJoinNodes):
    shortest = 1000
    theClosestConPairHierarchy = []
    for conPair in concurrentPairs:
        # cari conPair dengan hierarki paling awal, serta join node nya
        result = findTheClosestJoinNodeFromConcurrentPairWithItsDistance(conPair, allJoinNodes)
        distance = result[2]
        if distance < shortest:
            shortest = distance
            theClosestConPairHierarchy = conPair
            joinNode = result[1]
    return theClosestConPairHierarchy  # , joinNode # dapat pasangan conPair terdekat dan node join nya

def entranceDetector(session, splitGW):
    q_getEntrancesOfAndSplit = '''
        OPTIONAL MATCH (x {Name:$splitGW})-[r:DFG]->(y)
        RETURN collect(y.Name)
    '''
    results = session.run(q_getEntrancesOfAndSplit, splitGW=splitGW)

    entrances = None
    for rec in results:
        if rec is not None:
            #             print(rec)
            for entrances in rec:
                return entrances  # [[exit0,exit1,joinNode],...,[exit0,exit1,joinNode]]
        else:
            return None
    return entrances



def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    if len(lst3) > 0:
        return True
    else:
        return False

#     return lst3
def getValidEntrancesToJoinPaths(paths0, paths1):  # entrance to exit
    validEntrancesToJoinPaths = []
    # paths0  [['DISCHARGE', 'JOB_DEL'], ['DISCHARGE', 'STACK']]
    # paths1  [['DISCHARGE', 'JOB_DEL'], ['DISCHARGE', 'STACK']]
    # pathsx [['VESSEL_ATB', 'DISCHARGE', 'JOB_DEL'], ['VESSEL_ATB', 'DISCHARGE', 'STACK']]
    status = 'Not Found'
    for path0 in paths0:
        for path1 in paths1:
            common_exist = intersection(path0, path1)
            #                 print(common_exist)
            if not common_exist:  # jalur yang tidak ada interseksi berarti valid
                lenPath0 = len(path0)
                lenPath1 = len(path1)
                validEntrancesToJoinPath = [path0, path1, lenPath0 + lenPath1]
                #                     validEntrancesToJoinPaths.append(validEntrancesToJoinPath)
                status = 'Found'
                exit0 = path0[-1]
                exit1 = path1[-1]
                validEntrancesToJoinPaths.append([validEntrancesToJoinPath, status, [exit0, exit1]])
    return validEntrancesToJoinPaths  # , status, exit # [[{'CUSTOMS_DEL': ['JOB_DEL']}, {'VESSEL_ATB': ['DISCHARGE', 'STACK']}]]

def mergePathPairs(listOfPathPairs):  # [ [ [path_pair],status, exit ], [...] ]
    print('In merge listOfPathPairs= ', listOfPathPairs[0])
    exit = listOfPathPairs[2]
    listOfMergedPathPairs = []
    for i in range(len(listOfPathPairs[0])):
        print('Pair ' + str(i) + '= ', listOfPathPairs[0][i])
        mergedPath = []
        for path in listOfPathPairs[0][i]:
            print('path= ', path)
            mergedPath = mergedPath + path
        #         print('MergedPath ' + str(i) + '= ', mergedPath)
        listOfMergedPathPairs.append(mergedPath)
    return listOfMergedPathPairs, exit

# input: semua variasi pasangan entrance, dan pasangan exit
# output: entrance, dan exit yang sudah di merge
# cara merge, jika ada irisan
def mergeEntrance_exit_pairs(joinNodeEnum, nodeJoin, entrance_exit_pairs):
#     nodeJoin = 'JOB_DEL'
#     tuplePairs = [('CUSTOMS_DEL', 'VESSEL_ATB'), ('BAPLIE', 'CUSTOMS_DEL')]
#     tuplePairs = entrance_exit_pairs
    if len(entrance_exit_pairs)>1:
        combPair = generalHelper.combinationPairInList(entrance_exit_pairs)
        for pair in combPair: # tiap entrance_exit di pasangkan
            j_len = len(joinNodeEnum[nodeJoin])
            if j_len>1:

                print('paaiiir= ', pair)
                entrance0 = pair[0][0] #('CUSTOMS_DEL', 'VESSEL_ATB')
                exit0 = pair[0][1]
                dist0 = pair[0][2]
    #             print('paaiiir0= ', pair0)
                entrance1 = pair[1][0]
                exit1 = pair[1][1]
                dist1 = pair[1][2]
                result = intersection(entrance0,entrance1)
                if result:
                    mergedEntrances = tuple(set(entrance0 + entrance1))

                    if pair[0] in (joinNodeEnum[nodeJoin]):
                        joinNodeEnum[nodeJoin].remove(pair[0]) # singkirkan pasangan yg di merge
                    if pair[1] in (joinNodeEnum[nodeJoin]):
                        joinNodeEnum[nodeJoin].remove(pair[1])
                    mergedExits = list(set(exit0+exit1))
                    mergedDistance = dist0+dist1
                    # masukkan hasil merge
                    if [mergedEntrances, mergedExits] not in joinNodeEnum[nodeJoin]:
                        joinNodeEnum[nodeJoin].append([mergedEntrances, mergedExits])
    else:
        pass
    return joinNodeEnum # mergedJoinNodeEnum




