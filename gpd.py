from sphinx.ext.todo import todo_node

from helper import splitHelper
import discoverAND as disc_and
import discoverXOR as disc_xor

class GraphbasedDiscovery:

    def __init__(self, session):
        self.__session = session
        # self.__gw = gate.GateDiscovery(session)

    def discoverGW(self, session, initialNodeName, counter):
        splitNodes = splitHelper.getAllSplitNodes(session)
        GWlist = []
        joinXORList = []
        joinANDList = []
        t = splitHelper.closestSplitNode(session, initialNodeName, splitNodes)[1]  # t
        S, C, F = splitHelper.entranceScanner(session, t)
        while len(splitNodes) > 0: # ada splitNodes yang belum diperiksa
            t = splitHelper.closestSplitNode(session, initialNodeName, splitNodes)[1]  # t
            S, C, F = splitHelper.entranceScanner(session, t)
            gwList = []

            # Discover XOR
            X = set()
            joinXORgw = []
            while len(S)>1:
                while True:
                    S, C, F, counter, X, gwList, joinXORgw  = disc_xor.discoverXOR(session, t, S, C, F, counter)
                    if len(X) < 1:
                        xorIteration = False
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
                        andIteration = False
                        break

                    if len(gwList) > 0:
                        GWlist.append(gwList)
                    if joinANDgw:
                        joinANDList.extend(joinANDgw)

                S, C, F = splitHelper.entranceScanner(session, t)

            splitNodes.remove(t)  # buang split node yang sudah dikerjakan
        return GWlist, joinXORList, joinANDList


