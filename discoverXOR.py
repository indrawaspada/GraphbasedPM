from helper import generalHelper, joinHelper, blockHelper

# 1. deteksi xor split
# 2. temukan hirarki
# 3. deteksi join nya
# 4. simpan data join dalam tabel
def discoverXOR(session, t, S, C, F, counter):
    GWlist = []
    joinXORgw = []
    # print('t= ', t)
    X = set()  # concurrentPair
    # Check potensi konkurensi
    for s1 in S:
        print('=========== telusuri tiap direct succession ===============')
        print('s1: ', s1)
        Cu = set()
        Cu.update(C[s1])  # Cover+Future 1
        # print('CF1: ', CF1)
        for s2 in S:
            print('=========== telusuri pasangan direct succession nya ===============')
            if F[s1] == F[s2] and (s1 != s2):  # cari pasangan konkuren
                X.add(s2)  # node s2 dengan konkurensi, sekaligus penanda bahwa masih ada konkuren nodes
                Cu.union(C[s2])

        if len(X) > 0:  # jika ada XOR
            X.add(s1)
            break  # dikerjakan satu gateway dulu (yg F nya sama)

    # cari:

    if len(X) > 0:
        print('=========== ditemukan XOR split nodes ===============')
        # periksa kemungkinan ada hierarki dalam tipe gateway yg sama
        allJoinNodes = joinHelper.getAllJoinNodes(session)

        # Get valid block from 2 entrances
        # input: list of entrances
        # output:list of entrance-allPathVariantsTo-exit
        allPathVariantsFromEntranceToExit, S, C, F = generalHelper.getAllPathVariantsFromEntranceToExit(session, t, S, C, F, list(X),
                                                                                                        allJoinNodes)
        if len(allPathVariantsFromEntranceToExit) == 0: # berarti ada insert invisible task
            g = []
            return S, C, F, counter, X, g, joinXORgw

        # input 2 entrance, some paths, 1 join node. Result: valid block only
        valid_blocks = dict()
        for pathVariantsFromEntranceToExit in allPathVariantsFromEntranceToExit:
            entrance0 = pathVariantsFromEntranceToExit[0][0]
            paths0 = pathVariantsFromEntranceToExit[0][1]  # [['VESSEL_ATB', 'DISCHARGE', 'JOB_DEL'], ['VESSEL_ATB', 'DISCHARGE', 'STACK']]
            entrance1 = pathVariantsFromEntranceToExit[1][0]
            paths1 = pathVariantsFromEntranceToExit[1][1]
            joinNode = pathVariantsFromEntranceToExit[0][2]

            # dapat valid kandidat regions
            allValidEntrancePairToJoinBlock = joinHelper.filterValidEntrancePairsToJoinBlocks(paths0,
                                                                                              paths1)  # dapat path yg valid, bs lebih dari 1
            print('validEntranceCombPaths= ', allValidEntrancePairToJoinBlock)

            # jika ada validPaths
            for validEntrancePairToJoinBlock in allValidEntrancePairToJoinBlock:  # [validEntrancesToJoinPath, status, [exit0,exit1]
                regionA = allValidEntrancePairToJoinBlock[0][0][0]
                print('regionA= ', regionA)  # regionA=  ['CUSTOMS_DEL', 'JOB_DEL']
                regionB = allValidEntrancePairToJoinBlock[0][0][1]
                print('regionB= ', regionB)  # regionB=  ['VESSEL_ATB', 'DISCHARGE', 'STACK']

                # tidak ada deteksi shortcut pada blok XOR
                # tetapi perlu menandai semua potensi join-XOR dalam blok XOR
                # delete: generalHelper.shortcutHandlerBetweenRegion(session, regionA, regionB)

                exit = validEntrancePairToJoinBlock[2]  # [exit0,exit1]
                if len(validEntrancePairToJoinBlock[0]) > 0:  # validEntrancesToJoinPath --> ada distance path nya
                    entrancePair = [entrance0, entrance1]
                    entrancePair.sort()  # seragamkan
                    entrancePair = tuple(entrancePair)  # ubah ke tuple
                    distance = validEntrancePairToJoinBlock[0][2]

                    if entrancePair not in valid_blocks:  # cari pasangan entrance yang ada di daftar valid block
                        valid_blocks[entrancePair] = []
                        valid_blocks[entrancePair].append([joinNode, exit, distance])
                    else:
                        valid_blocks[entrancePair].append([joinNode, exit, distance])  # 27 mei 2023, sudah ada distance

        print('valid_blocks==> ', valid_blocks)
        # valid_blocks==>  {
        # ('BAPLIE', 'VESSEL_ATB'): [['DISCHARGE', ['BAPLIE', 'VESSEL_ATB'], 2]],
        # ('BAPLIE', 'CUSTOMS_DEL'): [['JOB_DEL', ['CUSTOMS_DEL', 'DISCHARGE'], 3], ['TRUCK_IN', ['JOB_DEL', 'STACK'], 5]],
        # ('CUSTOMS_DEL', 'VESSEL_ATB'): [['JOB_DEL', ['CUSTOMS_DEL', 'DISCHARGE'], 3], ['TRUCK_IN', ['JOB_DEL', 'STACK'], 5]]}

        # input : valid blocks
        # output: joinNode with its all possible entrances and paths
        joinNodeEnum = generalHelper.enumJoinNode(valid_blocks)

        # kalau ada tuple entrancePair yang beririsan maka gabungkan
        # merged_entrance_to_exit_pairs = []
        for joinNode in joinNodeEnum:
            while True:
                entrance_to_exit_pairs = joinNodeEnum[joinNode]  # [[('BAPLIE', 'VESSEL_ATB'), ['VESSEL_ATB', 'BAPLIE']]]
                finish, joinNodeEnum = joinHelper.mergeEntrance_exit_pairs(joinNodeEnum, joinNode,entrance_to_exit_pairs)
                if finish:
                    break
        mergedJoinNodeEnum = joinNodeEnum
        H = blockHelper.getTheNextHierarchies(mergedJoinNodeEnum)

        for h in H:
            # insert split_AND_gw
            SCP = list(h[0][0])  # SCP = entrances, bisa lebih dari 2
            g = insertXORSplitGW(session, t, SCP, counter)
            print('g= ', g)
            # insert join_AND_gw
            JCP = h[0][1]  # JCP = exits
            joinNode = h[1]
            joinXORgw.append(["xorJoinGW_" + str(counter), JCP, joinNode])

        # for splitArea in mergedJoinNodeEnum[joinNode]:
        #     SCP = list(splitArea[0])
        #     g = insertXORSplitGW(session, t, SCP, counter)


            # # TODO: deteksi XOR-join
            # # insert join_AND_gw
            # JCP = splitArea[1]  # JCP = exits
            # joinXORgw.append(["xorJoinGW_" + str(counter), JCP, joinNode])

            counter = counter + 1  # node number in a block counter

            SCPLen = len(SCP)  # ['BAPLIE', 'VESSEL_ATB']
            if g is not None:
                GWlist.append(g)
                Cu = set()
                Fi = set()
                for i in range(SCPLen):  # jumlah node entrace pair (s1,s2,...)
                    # print('i=', i)
                    # print('Cu= ', Cu)
                    # print('C=', C)
                    print('C[SCP[i]]= ', C[SCP[i]])
                    Cu.update(C[SCP[i]])  # tambahkan cover (dari s1, s2,...) ke set
                    Fi.update(F[SCP[i]])  # tambahkan future ke set
                    S.remove(SCP[i])  # hapus dari S karena digantikan dg g
                    C.pop(SCP[i])
                    F.pop(SCP[i])
                S.append(g)  # tambahkan node gateway ke S
                C[g] = Cu
                # print('C= ', C)
                # print('F= ', F)
                # print('Fi= ', Fi)
                F[g] = Fi
                print('F= ', F)
        print('S: ', S)
    return S, C, F, counter, X, GWlist, joinXORgw

def insertXORSplitGW(session, splitNodeName, splitPairs, counter):
    # Split detection

    q_split = '''
            MATCH (n {Name:$splitNodeName})-[r:DFG]->(a:RefModel)
            WHERE a.Name in $splitPairs
            MERGE (n)-[s:DFG {rel:r.rel}]->(splitGW:GW:RefModel {Name:"XORSplitGW"+"_"+$counter})
            WITH s, r, splitGW, a
            MERGE (splitGW)-[t:DFG {rel:r.rel, dff:r.dff}]->(a)
            // hapus r
            DELETE r
            SET t.split = True, splitGW.type= 'xorSplit', splitGW.split_gate = True
            WITh s, sum(t.dff) as sum_t_dff, splitGW
            SET s.dff = sum_t_dff
            WITH splitGW
            RETURN splitGW.Name
            '''
    records = session.run(q_split, splitNodeName=splitNodeName, splitPairs=splitPairs, counter=counter)

    for rec in records:
        if rec is not None:
            for splitGWName in rec:
                print(splitGWName)
                return splitGWName
        else:
            return None

def insertXORJoinGW(session, exitNodes, xorJoinGW_name, joinNodeName):
    # Split detection

    q_andJoin = '''
            MATCH (a:RefModel)-[r:DFG]->(n {Name:$joinNodeName})
            WHERE a.Name in $exitNodes
            MERGE (joinGW:GW:RefModel {Name:$xorJoinGW_name})-[s:DFG {rel:r.rel}]->(n)
            WITH s, r, joinGW, a
            MERGE (a)-[t:DFG {rel:r.rel, dff:r.dff}]->(joinGW)
            // hapus r
            DELETE r
            SET t.join = True, joinGW.type = 'xorJoin', joinGW.join_gate = True
            WITH s, sum(t.dff) as sum_t_dff, joinGW
            SET s.dff = sum_t_dff
            WITH joinGW
            RETURN joinGW.Name
            '''
    records = session.run(q_andJoin, exitNodes=exitNodes, xorJoinGW_name=xorJoinGW_name, joinNodeName=joinNodeName)

    for rec in records:
        if rec is not None:
            for joinGWName in rec:
                print(joinGWName)
                return joinGWName
        else:
            return None