# deteksi split
# fungs-fungsi

def setOutDegreeInNodes(session):
    q_out_degree = '''        
            match (a:RefModel)-[o {rel:'Sequence'}]->(x)
            with a, collect(o) as olist, count(o) as o_degree
            set a.o_degree = o_degree;
            '''
    session.run(q_out_degree)
    return None

def setSplitNodes(session):
    # Split detection
    q_split = '''
            MATCH (n)-[r:DFG]->(a:RefModel)
            WHERE n.o_degree > 1
            set r.split = True, n.split_gate = True;
            '''
    records = session.run(q_split)
    return None

def getAllSplitNodes(session):
    q_getAllSplitGates = '''
            OPTIONAL MATCH (a:RefModel {split_gate:True})
            RETURN collect(a.Name) as splitGates
    '''
    result = session.run(q_getAllSplitGates)

    for rec in result:
        if rec is not None:
            for splitGates in rec:
                return splitGates
        else:
            return None

def calculateLength(session, initial, target):
    if initial == target:
        return 0  # distance = 0
    else:
        q_calculateLength = '''
                MATCH p = shortestPath((a:RefModel {Name:$initial})-[:DFG*..20]->(b:RefModel {Name:$target}))
                RETURN length(p) AS distance
        '''
        result = session.run(q_calculateLength, initial=initial, target=target)

        for rec in result:
            if rec is not None:
                for distance in rec:
                    return distance
            else:
                return None

# Deteksi splitGW terdekat
def closestSplitNode(session, initial, listOfSplitNodes):
    #     splitNodes = getAllSplitNodes()

    theClosestSplitNode = [1000, None]
    for splitNode in listOfSplitNodes:
        length = calculateLength(session, initial, splitNode)
        #         print(length)
        if (length is not None) and (length < theClosestSplitNode[0]):
            theClosestSplitNode = [length, splitNode]
    return theClosestSplitNode

def getAllDirectSuccessors(session, splitNodeName):
    q_getAllDirectSuccessors = '''
            MATCH (a:RefModel {Name: $splitNodeName})-[:DFG]->(d)
            RETURN collect(d.Name) as dSuccessors
    '''
    result = session.run(q_getAllDirectSuccessors, splitNodeName=splitNodeName)

    allDirectSuccessors = []
    for rec in result:
        if rec is not None:
            for dSuccessor in rec:
                allDirectSuccessors = allDirectSuccessors + dSuccessor
                return allDirectSuccessors
        else:
            return None

# petakan Cover
def coverDetection(session, theClosestSplitNode, directSuccessor, t_SuccessorList):
    q_coverDetection = '''
            OPTIONAL MATCH (a:RefModel {Name:$theClosestSplitNode})-->(s {Name:$directSuccessor})-[r:DFG {rel:'Sequence'}]->(c)
            WHERE c.Name in $t_SuccessorList
            RETURN collect(c.Name) as covers
    '''
    records = session.run(q_coverDetection, theClosestSplitNode=theClosestSplitNode, directSuccessor=directSuccessor,
                          t_SuccessorList=t_SuccessorList)

    covers = []
    if directSuccessor in t_SuccessorList:
        t_successor = directSuccessor
    else:
        t_successor = None
    for rec in records:
        if rec is not None:
            for cover in rec:
                covers = covers + cover
                return covers + [t_successor]
        else:
            return None

def futureDetection(session, theClosestSplitNode, directSuccessor, t_SuccessorList):
    q_coverDetection = '''
            OPTIONAL MATCH (a:RefModel {Name:$theClosestSplitNode})-->(s {Name:$directSuccessor})-->(f)
            WHERE f.Name in $t_SuccessorList
            RETURN collect(f.Name) as futures
    '''
    records = session.run(q_coverDetection, theClosestSplitNode=theClosestSplitNode, directSuccessor=directSuccessor,
                          t_SuccessorList=t_SuccessorList)

    futures = []
    if directSuccessor in t_SuccessorList:
        t_successor = directSuccessor
    else:
        t_successor = None
    for rec in records:
        if rec is not None:
            for future in rec:
                futures = futures + future
                return futures
        else:
            return None

def isConcurrent(session, s1, s2):
    q_isConcurrent = '''
            OPTIONAL MATCH (s1:RefModel {Name:$s1})-[r:CONCURRENT]->(s2:RefModel {Name:$s2})
            RETURN s1.Name
    '''
    records = session.run(q_isConcurrent, s1=s1, s2=s2)

    for rec in records:
        if rec is not None:
            for nodeName in rec:
                if nodeName == s1:
                    return True
                else:
                    return False
        else:
            return None

import itertools

def flattenData(splitDictData):
    data = {}
    hasil = []
    for t in splitDictData:  # a,b,c,d,e
        data = {}
        for key in t:
            value = list(itertools.chain.from_iterable(t[key]))
            value.sort()
            data[key] = tuple(value)
            hasil.append(data)
    return hasil

# memasangkan kombinasi antar element dalam list
def combinationPairInList(theList):
    combinations = []
    partial = theList.copy()
    for i in range(len(theList)):
        partial = partial[1:]
        for j in range(len(partial)):
            combinations.append([theList[i], partial[j]])
    return combinations

# membandingkan pasangan value dari pasangan elemen dalam list
def compareValueOfDictPair(ListOfDictPair):
    comparison = []
    for dictPair in ListOfDictPair:
        for key0 in dictPair[0]:
            for key1 in dictPair[1]:
                comparison.append([{key0, key1}, [dictPair[0][key0], dictPair[1][key1]]])
    return comparison

def getTheSameCoverFuture(D_flat):
    combs = combinationPairInList(D_flat)
    #     print('combs = ', combs)
    comparison = compareValueOfDictPair(combs)
    #     print('comparison = ', comparison)
    sameCoverFuturePairs = []
    for e in comparison:
        if e[1][0] == e[1][1]:  # jika value sama
            sameCoverFuturePairs.append([e[1][0], e[0]])  # [ nodes CF nya, nodes entrance nya]
    #     print('sameCoverFuturePair= ', sameCoverFuturePair)
    return sameCoverFuturePairs

# sameCFPairs = getTheSameCoverFuture(test1)
# sameCFPairs

def agregatSameCoverFuture(sameCoverFuturePairs):
    agSameCF = {}
    for pair in sameCoverFuturePairs:
        #         print('agSameCF= ', agSameCF)
        #         print('pair=', pair)
        #         print('pair[0]=', pair[0])
        if pair[0] in list(agSameCF):  # jika CF sudah ada
            for e in pair[1]:
                agSameCF[pair[0]].add(e)  # Set
        else:  # jika CF belum ada
            #             print('pair[0]= ', pair[0])
            #             print('pair[1]= ', pair[1])
            agSameCF[pair[0]] = pair[1]  # buat CF sebagai key, dan nodes entrance nya sebagai value
    return agSameCF

# agregatSameCoverFuture(sameCFPairs)

def clearSplitStatus(session, nodeName):
    q_clearSplitStatus = '''
        OPTIONAL MATCH (a:RefModel {Name:$nodeName})
        SET a.split_gate = False
    '''
    result = session.run(q_clearSplitStatus, nodeName=nodeName)

    return None

def entranceScanner(session, t):
    S = getAllDirectSuccessors(session, t)  # S
    C = {}
    F = {}
    for s1 in S:
        C[s1] = {s1}  # C = {s1:{s1}, ...}
        F[s1] = set()  # F = {s1:{}, ...}
        for s2 in S:
            if (s1 != s2) and (isConcurrent(session, s1, s2)):
                F[s1].add(s2)
    return S, C, F

