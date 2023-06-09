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
            break  # dikerjakan satu gateway dulu (yg CF nya sama)

    # cari:

    if len(X) > 0:
        X_list = list(X)
        g = insertXORSplitGW(session, t, X_list, counter)

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