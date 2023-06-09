from sphinx.ext.todo import todo_node

from helper import splitHelper
import discoverAND as disc_and
import discoverXOR as disc_xor

class GraphbasedDiscovery:

    def __init__(self, session, gate):
        self.__session = session
        # self.__gw = gate.GateDiscovery(session)

    def discoverGW(self, session, initialNodeName, counter):
        splitNodes = splitHelper.getAllSplitNodes(session)
        GWlist = []
        joinXORList = []
        joinANDList = []
        while len(splitNodes) > 0: # ada splitNodes yang belum diperiksa
            t = splitHelper.closestSplitNode(session, initialNodeName, splitNodes)[1]  # t
            S = splitHelper.getAllDirectSuccessors(session, t)  # S
            C = {}
            F = {}
            for s1 in S:
                C[s1] = {s1}  # C = {s1:{s1}, ...}
                F[s1] = set()  # F = {s1:{}, ...}
                for s2 in S:
                    if (s1 != s2) and (splitHelper.isConcurrent(session, s1, s2)):
                        F[s1].add(s2)


            gwList = []


            # Discover XOR
            X = set()
            joinXORgw = []
            while True:
                S, C, F, counter, X, gwList, joinXORgw  = disc_xor.discoverXOR(session, t, S, C, F, counter, GWlist, joinXORgw)
                print('x= ', X)
                X = {}
                if len(X) < 1:
                    print('break')
                    break

                if len(gwList) > 0:
                    GWlist.append(gwList)
                if joinXORgw:
                    joinXORList.extend(joinXORgw)

            # Discover AND
            joinANDgw = []
            while True:
                # meski ada penanganan shortcut mjd concurrent tp tdk perlu membuang data splitnode
                S, C, F, counter, A, gwList, joinANDgw = disc_and.discoverAND(session, t, S, C, F, counter, gwList, joinANDgw)
                print("A==> ", A)
                if len(A) < 1:  # kalau sudah tidak ada konkurensi
                    break

                if len(gwList) > 0:
                    GWlist.append(gwList)
                if joinANDgw:
                    joinANDList.extend(joinANDgw)

            splitNodes.remove(t)  # buang split node yang sudah dikerjakan
        return GWlist, joinXORList, joinANDList


