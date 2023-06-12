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


def shortcutHandlerBetweenRegion(session, regionA, regionB):
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
def getEntranceToJoinPath(session, entrance, joinNode):
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


def getAllPossiblePathsFromEntranceToExit(session, t, S, C, F, A, allJoinNodes):
    entrancePairList = combinationPairInList(list(A))
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

                    # tambahkan ke S
                    # S.remove(joinNode)
                    # S.append(entrance)
                    # C[entrance] = {entrance}
                    # C.pop(joinNode)
                    # F[entrance] = {entrance}
                    # F.pop(joinNode)

                    # print('status insert invisible task = ', status)

                paths = getEntranceToJoinPath(session, entrance, joinNode)
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
            MERGE (n)-[s:DFG {rel:r.rel}]->(invTask:RefModel {Name:"inv"+"_"+n.Name+"_"+a.Name})
            SET s.dff = r.dff
            WITH s, r, invTask, a
            MERGE (invTask)-[t:DFG {rel:r.rel, dff:r.dff}]->(a)
            // hapus r
            DELETE r
            SET s.split = True, t.dff = s.dff, t.join = True, invTask.label= 'INVISIBLE'
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

def getTheDirectExitToJoinNode(session, exitNode, joinNodeName):
    q_replaceWithDirectExitToJoinNode = '''
        MATCH path = allshortestpaths((a {Name:$exitNode})-[r:DFG *..100]->(c:RefModel ))
        WHERE c.Name = $joinNodeName
        WITH c, path, [n in nodes(path)| n.Name] AS nnames
        MATCH (b)-->(c)
        WHERE b.Name in nnames
        RETURN b.Name
        '''
    results = session.run(q_replaceWithDirectExitToJoinNode, exitNode=exitNode, joinNodeName=joinNodeName)

    length = 0
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
        MERGE (a)-[s:DFG {rel:r.rel}]->(invTask:RefModel {Name:"inv"+"_"+a.Name+"_"+c.Name})
        SET s.dff = r.dff
        WITH s, r, invTask, c
        MERGE (invTask)-[t:DFG {rel:r.rel, dff:r.dff}]->(c)
        // hapus r
        DELETE r
        SET t.dff = s.dff, invTask.label= 'INVISIBLE'
        RETURN invTask.Name

        '''
    results = session.run(q_insertInvisibleTaskBetweenConsecutiveGateway)

    length = 0
    for record in results:
        invTaskName = record[0]
    return invTaskName