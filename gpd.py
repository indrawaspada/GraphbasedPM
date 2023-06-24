from sphinx.ext.todo import todo_node

from helper import splitHelper
import discoverAND as disc_and
import discoverXOR as disc_xor

class GraphbasedDiscovery:

    def __init__(self, session):
        self.__session = session

    def discoverGW(self, session, initialNodeName, counter):
        T = splitHelper.getAllSplitNodes(session)
        GWlist = []
        gwList = []
        joinXORList = []
        joinANDList = []
        # t = splitHelper.closestSplitNode(session, initialNodeName, T)[1]  # t
        # S, C, F = splitHelper.entranceScanner(session, t)
        while len(T) > 0: # ada splitNodes yang belum diperiksa
            t = splitHelper.closestSplitNode(session, initialNodeName, T)[1]  # t
            S, C, F = splitHelper.entranceScanner(session, t)
            while len(S)>1:

                # discoverANDsplitJoin
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

                # discoverXORsplitJoin
                while True:
                    S, C, F, counter, X, gwList, joinXORgw  = disc_xor.discoverXOR(session, t, S, C, F, counter)
                    if len(X) < 1:
                        print('break')
                        break

                    if len(gwList) > 0:
                        GWlist.append(gwList)
                    if joinXORgw:
                        joinXORList.extend(joinXORgw)

                S, C, F = splitHelper.entranceScanner(session, t)

            T.remove(t)  # buang split node yang sudah dikerjakan
        return GWlist, joinXORList, joinANDList


