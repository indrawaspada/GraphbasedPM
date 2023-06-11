from helper import generalHelper, joinHelper

# 1. deteksi xor split
# 2. temukan hirarki
# 3. deteksi join nya
# 4. simpan data join dalam tabel
def discoverXOR(session, t, S, C, F, counter, GWlist, joinXORgw):
    print('t= ', t)
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
        allPathVariantsFromEntranceToExit = generalHelper.getAllPossiblePathsFromEntranceToExit(session, t, list(X),
                                                                                                allJoinNodes)

        # input 2 entrance, some paths, 1 join node. Result: valid block only
        valid_blocks = dict()
        for pathVariantsFromEntranceToExit in allPathVariantsFromEntranceToExit:
            entrance0 = pathVariantsFromEntranceToExit[0][0]
            paths0 = pathVariantsFromEntranceToExit[0][1]  # [['VESSEL_ATB', 'DISCHARGE', 'JOB_DEL'], ['VESSEL_ATB', 'DISCHARGE', 'STACK']]
            entrance1 = pathVariantsFromEntranceToExit[1][0]
            paths1 = pathVariantsFromEntranceToExit[1][1]
            joinNode = pathVariantsFromEntranceToExit[0][2]

            # dapat valid kandidat regions
            allValidEntrancePairToJoinBlock = joinHelper.getValidEntrancesToJoinPaths(paths0,
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
        mergedEntrancePair = []
        for joinNode in joinNodeEnum:
            entrance_to_exit_pairs = joinNodeEnum[joinNode]  # [[('BAPLIE', 'VESSEL_ATB'), ['VESSEL_ATB', 'BAPLIE']]]
            merged_entrance_to_exit_pairs = joinHelper.mergeEntrance_exit_pairs(joinNodeEnum, joinNode,
                                                                                entrance_to_exit_pairs)
        print('mergedEntrance_exit_pairs= ', merged_entrance_to_exit_pairs)

        # pick the minimal number of entrancesPairPaths --> KOREKSI
        # cari block hirarki dengan join node terdekat, ciri2nya punya entrance paling sedikit dan jarak ke join node terdekat

        shortest = 1000
        closestHirarchies = []
        theJoinNode = ''
        for joinNode in merged_entrance_to_exit_pairs:  # β
            for entrance_to_exit_pairs in merged_entrance_to_exit_pairs[joinNode]:  # [[('BAPLIE', 'VESSEL_ATB'), ['VESSEL_ATB', 'BAPLIE']]]
                NumOfEntrances = len(entrance_to_exit_pairs[0])
                if NumOfEntrances < shortest:
                    shortest = NumOfEntrances
                    closestHirarchies = [[entrance_to_exit_pairs, joinNode]]
                elif NumOfEntrances == shortest:
                    closestHirarchies.append([entrance_to_exit_pairs, joinNode])
        print('closestHirarchy= ', closestHirarchies)

        for closestHirarchy in closestHirarchies:
            SCP = list(closestHirarchy[0][0])
            g = insertXORSplitGW(session, t, SCP, counter)

        # TODO: deteksi XOR-join

        # insert join_AND_gw
        exits = [] # temp
        joinNode = [] # temp
        joinXORgw.append(["xorJoinGW_" + str(counter), exits, joinNode])
        # deteksi XOR-join

        # catat XOR-join

    # jika X sudah kosong maka hasil return nilai nya akan menghentikan loop while
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