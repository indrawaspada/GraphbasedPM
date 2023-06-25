from helper import splitHelper

# memasangkan kombinasi antar element dalam list
def combinationPairInList(theList):
    combinations = []
    partial = theList.copy()
    for i in range(len(theList)):
        partial = partial[1:]
        for j in range(len(partial)):
            combinations.append([theList[i],partial[j]])
    return combinations

# membandingkan pasangan value dari pasangan elemen dalam list
def compareValueOfDictPair(ListOfDictPair):
    comparison = []
    for dictPair in ListOfDictPair:
        for key0 in dictPair[0]:
            for key1 in dictPair[1]:
                comparison.append([{key0,key1}, [dictPair[0][key0], dictPair[1][key1]]])
    return comparison

def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    if len(lst3)>0:
        return True
    else:
        return False

def pairCombBetweenLists(list1, list2):
    pair_comb = []
    for i in list1:
        for j in list2:
            pair_comb.append([i,j])
    return pair_comb

def sequence_detector(session, pair):
    #     nodeA = pair[0]
    #     nodeB = pair[1]
    q_sequence_detector = '''
            MATCH (a)-[:DFG]->(b)
            WHERE a.Name in $pair and b.Name in $pair
            RETURN a.Name,b.Name
            '''
    result = session.run(q_sequence_detector, pair=pair)

    directSequence = []
    for records in result:
        if records is not None:
            print(records)
            directSequence.append(records[0])  # source
            directSequence.append(records[1])  # destination
    return directSequence

def incompleteConcurrentHandler(session, shorcutPair):
    nodeA = shorcutPair[0]
    nodeB = shorcutPair[1]
    q_incompleteConcurrentHandler ='''
        MATCH (a {Name:$nodeA})-[r]->(b {Name:$nodeB})
        MERGE (a)-[s:CONCURRENT]->(b)
        SET s.dff = r.dff
        DELETE r
        '''
    session.run(q_incompleteConcurrentHandler, nodeA=nodeA, nodeB=nodeB)
    return None


def ICRHandlerBetweenRegion(session, regionA, regionB):
    shortcut = 0
    comb_nodes = pairCombBetweenLists(regionA, regionB)
    for pair in comb_nodes:
        shorcutPair = sequence_detector(session, pair)  # deteksi pasangan shorcut
        # print('shorcutPair= ',shorcutPair)
        if len(shorcutPair) > 0:
            print('ubah shortcut jadi concurrent')
            incompleteConcurrentHandler(session, shorcutPair)  # ubah shortcut menjadi concurrent
            shortcut += 1
    return shortcut

# [['BAPLIE', 'DISCHARGE', 'JOB_DEL'], ['BAPLIE', 'DISCHARGE', 'STACK']]
def getEntranceToExitPath(session, entrance, joinNode):
    q_findJoinPath = '''
        //OPTIONAL MATCH (q)-[:DFG]->(y {Name:$joinNode})
        //WITH q
        OPTIONAL MATCH path = (x {Name:$entrance})-[r:DFG*]->(y {Name:$joinNode})
        UNWIND nodes(path) as node
        WITH path, collect(node.Name) as names
        RETURN names
    '''
    results = session.run(q_findJoinPath, entrance=entrance, joinNode=joinNode)
    paths = []
    status = 'Not found'
    for records in results:
        #         print('test1')
        #         print(records)
        for record in records:
            status = 'Exist'
            if record is not None:  # ['BAPLIE', 'DISCHARGE', 'JOB_DEL', 'TRUCK_IN']
                #                 record.remove(entrance)
                record.remove(joinNode)
                paths.append(record)
    return paths, status


def getAllXORPathVariantsFromEntranceToExit(session, t, S, C, F, E, allJoinNodes):
    entrancePairList = combinationPairInList(list(E))
    allEntranceCombPaths = []
    for joinNode in allJoinNodes:
        for entrancePair in entrancePairList:
            # print('entrancePair====: ', entrancePair)
            entrancePairPaths = []
            for entrance in entrancePair:  # tiap entrance dalam entrance pair

                # cek jika entrance = joinNode maka sisipkan invisible task
                if entrance == joinNode:
                    # insert invisible task between splitNode and joinNode
                    entrance = insertInvisibleTask(session,t, joinNode)

                    S, C, F = splitHelper.entranceScanner(session, t)
                    allEntranceCombPaths = []
                    return allEntranceCombPaths, S, C, F

                # if entrance == joinNode:
                #     entrancePairPaths.append([t,[[]],joinNode])
                # else:
                paths = getEntranceToExitPath(session, entrance, joinNode)
                if paths[1] == 'Exist':
                    entrancePairPaths.append([entrance, paths[0], joinNode])
                else:
                    entrancePairPaths = []
                    break
            if len(entrancePairPaths) > 0:
                allEntranceCombPaths.append(entrancePairPaths)

    return allEntranceCombPaths, S, C, F

def getAllANDPathVariantsFromEntranceToExit(session, t, S, C, F, E, allJoinNodes):
    entrancePairList = combinationPairInList(list(E))
    allEntranceCombPaths = []
    for entrancePair in entrancePairList:
        for joinNode in allJoinNodes:
            # print('entrancePair====: ', entrancePair)
            entrancePairPaths = []
            for entrance in entrancePair:  # tiap entrance dalam entrance pair

                # cek jika entrance = joinNode maka sisipkan invisible task
                if entrance == joinNode:

                    skipJoinNode = True
                    entrancePairPaths = [] # isinya dibatalkan
                    break

                # if entrance == joinNode:
                #     entrancePairPaths.append([t,[[]],joinNode])
                # else:
                paths = getEntranceToExitPath(session, entrance, joinNode)
                if paths[1] == 'Exist':
                    entrancePairPaths.append([entrance, paths[0], joinNode])
                else:
                    entrancePairPaths = []
                    break

            if len(entrancePairPaths) > 0:
                allEntranceCombPaths.append(entrancePairPaths)

    return allEntranceCombPaths, S, C, F

def enumJoinNode(valid_blocks):
    # ('CUSTOMS_DEL', 'VESSEL_ATB'): [['JOB_DEL', ['CUSTOMS_DEL', 'DISCHARGE'], 3], ['TRUCK_IN', ['JOB_DEL', 'STACK'], 5]]}
    joinNodeEnum = {}
    for entrancePair in valid_blocks:  # entrancePair = ('CUSTOMS_DEL', 'VESSEL_ATB')
        for pathToJoinInfo in valid_blocks[entrancePair]:  # [['JOB_DEL', ['CUSTOMS_DEL', 'DISCHARGE']], ['TRUCK_IN', ['JOB_DEL', 'STACK']]]
            joinNode = pathToJoinInfo[0]
            exitPair = pathToJoinInfo[1]
            distance = pathToJoinInfo[2]
            if joinNode not in joinNodeEnum:  # 'JOB_DEL'
                joinNodeEnum[pathToJoinInfo[0]] = []
            joinNodeEnum[joinNode].append([entrancePair, exitPair, distance])  # exit = ['CUSTOMS_DEL', 'DISCHARGE']
    return joinNodeEnum

def insertInvisibleTask(session, splitNodeName, joinNodeName):
    # Split detection

    q_insertInvisibleTask = '''
            MATCH (n {Name:$splitNodeName})-[r:DFG]->(a:RefModel)
            WHERE a.Name = $joinNodeName
            MERGE (n)-[s:DFG {rel:r.rel}]->(invTask:Activity:RefModel {Name:"inv"+"_"+n.Name+"_"+a.Name})
            SET s.dff = r.dff
            WITH s, r, invTask, a
            MERGE (invTask)-[t:DFG {rel:r.rel, dff:r.dff}]->(a)
            // hapus r
            DELETE r
            SET 
            s.split = True, 
            t.dff = s.dff, 
            t.join = True, 
            invTask.label= 'Invisible'
            RETURN invTask.Name
            '''
    records = session.run(q_insertInvisibleTask, splitNodeName=splitNodeName, joinNodeName=joinNodeName)

    for rec in records:
        if rec is not None:
            for invTask in rec:
                print(invTask)
                return invTask
        else:
            return None


# deteksi path yang hanya bertipe gw dan invisible
def getTheDirectExitToJoinNode(session, exitNode, joinNodeName):
    q_replaceWithDirectExitToJoinNode = '''
        MATCH p=shortestPath((a {Name:$exitNode})-[:DFG *..100]->(c:RefModel {Name:$joinNodeName }))
        with p, nodes(p) as ns,c 
        WHERE ALL(n IN ns[1..-1] WHERE  (n.label = 'Invisible'))
        WITH c, p, [n in nodes(p)| n.Name] AS nnames
        MATCH (b)-->(c)
        WHERE b.Name in nnames
        RETURN b.Name
        limit 1
        '''
    results = session.run(q_replaceWithDirectExitToJoinNode, exitNode=exitNode, joinNodeName=joinNodeName)

    length = 0
    exitNodeName = None
    for record in results:
            exitNodeName = record[0]
    return exitNodeName


def checkHierarchy(merged_entrance_to_exit_pairs):
    smallestNumOfEntrances = 1000
    smallerBlocks = []
    theJoinNode = ''
    for joinNode in merged_entrance_to_exit_pairs:  # β
        for entrance_to_exit_pairs in merged_entrance_to_exit_pairs[
            joinNode]:  # [[('BAPLIE', 'VESSEL_ATB'), ['VESSEL_ATB', 'BAPLIE']]]
            NumOfEntrances = len(entrance_to_exit_pairs[0])
            if NumOfEntrances < smallestNumOfEntrances:
                smallestNumOfEntrances = NumOfEntrances
                smallerBlocks = [[entrance_to_exit_pairs, joinNode]]
            elif NumOfEntrances == smallestNumOfEntrances:
                smallerBlocks.append([entrance_to_exit_pairs, joinNode])
    print('smallerBlocks= ', smallerBlocks)
    return smallerBlocks


def insertInvisibleTaskBetweenConsecutiveGateway(session):
    q_insertInvisibleTaskBetweenConsecutiveGateway = '''
        MATCH (a:GW)-[r:DFG]->(c:GW)
        MERGE (a)-[s:DFG {rel:r.rel}]->(invTask:Activity:RefModel {Name:"inv"+"_"+a.Name+"_"+c.Name})
        SET s.dff = r.dff
        WITH s, r, invTask, c
        MERGE (invTask)-[t:DFG {rel:r.rel, dff:r.dff}]->(c)
        // hapus r
        DELETE r
        SET t.dff = s.dff, invTask.label= 'Invisible'
        RETURN invTask.Name

        '''
    results = session.run(q_insertInvisibleTaskBetweenConsecutiveGateway)

    length = 0
    invTaskName = None
    for record in results:
        invTaskName = record[0]
    return invTaskName

def entranceValidTest(session, t, a,b):
    q_validTest = '''
        OPTIONAL MATCH path = (t:RefModel {Name:$t})-[r:DFG *..100]->(a:RefModel {Name:$a})
        with t, path, nodes(path) as ns
        WHERE any(node IN ns WHERE (exists(node.Name) and node.Name = $b))
        RETURN t.Name
    
    '''
    result = session.run(q_validTest, t=t, a=a, b=b)
    valid = True
    for record in result:
        for rec in record:
            if rec[0] is not None: # jika ada pathnya berarti tidak valid
                valid = False
    return valid

def isMergedEntranceValid(session, t, mergedEntrances):
    isValid1 = False
    isValid2 = False
    status = False
    all_comb_pairs = combinationPairInList(list(mergedEntrances))
    # check apakah ada dari t ke entrance0 melewati entrance1
    for pair in all_comb_pairs:
        a = pair[0]
        b = pair[1]
        isValid1 = entranceValidTest(session, t, a, b)
        isValid2 = entranceValidTest(session, t, b, a)
        status = isValid1 or isValid2
        if status:
            break
    return status


def exitValidTest(session, a, b, u):
    q_validTest = '''
        OPTIONAL MATCH path = (a:RefModel {Name:$a})-[r:DFG *..100]->(u:RefModel {Name:$u})
        with u, path, nodes(path) as ns
        WHERE any(node IN ns WHERE (exists(node.Name) and node.Name = $b))
        RETURN u.Name

    '''
    result = session.run(q_validTest, a=a, b=b, u=u)
    valid = True
    for record in result:
        for rec in record:
            if rec[0] is not None:  # jika ada pathnya berarti tidak valid
                valid = False
    return valid

def isMergedExitValid(session, mergedExits, u):
    isValid1 = False
    isValid2 = False
    status = False
    all_comb_pairs = combinationPairInList(mergedExits)
    # check apakah ada dari t ke entrance0 melewati entrance1
    for pair in all_comb_pairs:
        a = pair[0]
        b = pair[1]
        isValid1 = exitValidTest(session, a, b, u)
        isValid2 = exitValidTest(session, b, a, u)
        status = isValid1 and isValid2
        if status:
            break
    return status

def mergeSameGwInSequence(session):
    q_mergeSameGwInSequence = '''
        MATCH (g:GW)-[r]->(l:GW)
        WHERE l.i_degree < 2 and g.type = l.type and (NOT (g.o_degree > 1 and l.o_degree > 1))
        DELETE r
        WITH g, l
        call apoc.refactor.mergeNodes([g,l]) YIELD node
        RETURN node
    '''
    results = session.run(q_mergeSameGwInSequence)
    result = False
    for record in results:
        print(record[0])
        print("record[0]['Name']= ", record[0]['Name'])
        if record[0]['Name'] is not None:  # jika ada pathnya berarti tidak valid
            result = True
            break

        # for rec in record:
        #     print(rec)
        #     if rec[0] is None:  # jika ada pathnya berarti tidak valid
        #         print(rec[0])
        #         result = False
    return result

def insertInvisibleTaskBetweenDirectGW(session, exitNodes, joinNodeName):
    while True:
        finish = True
        for exitNode in exitNodes:
            pair = [exitNode, joinNodeName]

            # jika terdeteksi ada exit-joinNode yg konkuren maka exitNodes not valid
            if splitHelper.isConcurrent(session,pair[0], pair[1]):
                exitNodes = []
                return  exitNodes

            if sequence_detector(session, pair):
                pass  # aman, tidak ada node lain
            else:  # ada node lain
                newExit = getTheDirectExitToJoinNode(session, exitNode, joinNodeName)
                oldExit = exitNode
                exitNodes.remove(oldExit)
                exitNodes.append(newExit)
                finish = False
        if finish == True:
            break
    return exitNodes