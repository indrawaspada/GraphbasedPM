from sphinx.ext.todo import todo_node

from helper import splitHelper
import discoverAND as disc_and
import discoverXOR as disc_xor
import sys
import traceback

class GraphbasedDiscovery:

    def __init__(self, session):
        self.__session = session

    def discoverGW(self, session, initialNodeName, counter):
        T = splitHelper.getAllSplitNodes(session)
        GW_list = []
        gwList = []
        joinXORList = []
        joinANDList = []
        # t = splitHelper.closestSplitNode(session, initialNodeName, T)[1]  # t
        # S, C, F = splitHelper.entranceScanner(session, t)
        while len(T) > 0: # ada splitNodes yang belum diperiksa
            t = splitHelper.closestSplitNode(session, initialNodeName, T)[1]  # t
            if t is None: # kok bisa None?
                try:
                    T.pop(0) # ambil yg paling depan
                except BaseException as ex:
                    ex_type, ex_value, ex_traceback = sys.exc_info()
                    trace_back = traceback.extract_tb(ex_traceback)
                    stack_trace = list()
                    for trace in trace_back:
                        stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))
                    print("Exception type : %s " % ex_type.__name__)
                    print("Exception message : %s" % ex_value)
                    print("Stack trace : %s" % stack_trace)

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
                        GW_list.extend(joinANDgw)
                    if joinANDgw:
                        joinANDList.extend(joinANDgw)

                # discoverXORsplitJoin
                while True:
                    S, C, F, counter, X, gwList, joinXORgw, repeat  = disc_xor.discoverXOR(session, t, S, C, F, counter)
                    if len(X) < 1 and repeat: # ini krn ada insertInvTask
                        break

                    if not repeat:
                        print('NO Join Node')
                        print(t)
                        if len(T)>0:
                            try:
                                T.remove(t) # buang t
                            except:
                                print('error')
                            t = splitHelper.closestSplitNode(session, initialNodeName, T)[1]  # t
                        break

                    if len(gwList) > 0:
                        GW_list.extend(joinXORgw)
                    if joinXORgw:
                        joinXORList.extend(joinXORgw)

                S, C, F = splitHelper.entranceScanner(session, t)

            print(t)
            if len(T) > 0:
                try:
                    T.remove(t)  # buang split node yang sudah dikerjakan
                except:
                    T.pop(0)
                    print(T)
                    print(t)
                    print('ada exception')
        return GW_list, joinXORList, joinANDList


