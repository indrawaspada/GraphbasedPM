# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from neo4j import GraphDatabase
from helper import joinHelper, splitHelper, generalHelper, preprocessing
import gpd
import discoverXOR, discoverAND, saveToPnml
from pm4py.objects.petri.petrinet import PetriNet, Marking
import pm4py



def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

#Delete all nodes with relationship
def clearAll(session):
    query="MATCH (N) detach delete (N)"
    session.run(query)
    return None

def constructDFFRefModel(session, fileName):
    q_trace = '''
                USING PERIODIC COMMIT
                LOAD CSV with headers FROM $fileName AS line
                MERGE (:Trace {LineNo:toInteger(line.index), CaseId: line.case, Name:line.event, Frequency:toInteger(line.case_frequency), idff:0, odff:0});
            '''
    session.run(q_trace, fileName=fileName)

    q_model = '''
                USING PERIODIC COMMIT
                LOAD CSV with headers FROM $fileName AS line
                MERGE (:RefModel:Activity {Name:line.event ,label:line.event, split_gate:False, join_gate:False, entrance:False, exit:False, idff:0, odff:0, i_degree:0, o_degree:0});
            '''
    session.run(q_model, fileName=fileName)

    q_relation = '''
            MATCH (x:Trace), (y:Trace)
            WHERE x.CaseId = y.CaseId 
            AND y.LineNo - x.LineNo = 1
            MERGE (x)-[q:DFG {rel:'SEQUENCE', dff:x.Frequency}]->(y) // observed log
            WITH x,q,y
            MATCH (a:RefModel), (b:RefModel)
            WHERE a.Name = x.Name AND b.Name=y.Name
            // agar tiap edge baru dipastikan beda maka ditambahkan CaseId
            MERGE (a)-[r:DFG {CaseId: x.CaseId, rel:'Sequence', seq:False, xor:False, and:False, or:False, split:False, join:False, dff:q.dff, temp:'None'}]->(b) // modeled behaviour
            '''
    session.run(q_relation)

    q_merge_edge = '''
            match(n:RefModel)-[r:DFG]->(m:RefModel)
            with n,m, sum(r.dff) as total, collect(r) as relationships
            where size(relationships) > 1
            with total, head(relationships) as keep, tail(relationships) as delete
            set keep.dff = total
            REMOVE keep.CaseId
            foreach(r in delete | delete r)
            '''
    session.run(q_merge_edge)

    q_in_dff = '''
            match (a:RefModel)<-[i]-(x)
            with a, collect(i) as ilist, sum(i.dff) as idff
            set a.idff = idff;
            '''
    session.run(q_in_dff)

    q_out_dff = '''        
            match (a:RefModel)-[o]->(x)
            with a, collect(o) as olist, sum(o.dff) as odff
            set a.odff = odff;
            '''
    session.run(q_out_dff)

    q_addEntitiLabel = '''
            match (n:RefModel) WITH n
            CALL apoc.create.addLabels(n, [n.Region]) YIELD node
            RETURN n
            '''
    session.run(q_addEntitiLabel)

    #     q_filter_low_freq ='''
    #             MATCH (a:Model)-[r]->(b:Model)
    #             WHERE r.dff < 100
    #             DELETE r
    #             '''
    #     session.run(q_filter_low_freq)

    # q_concurrent = '''
    #         MATCH (x:Trace)-[p:DFG]->(y:Trace)-[q:DFG]->(z:Trace)
    #         WITH x,y,z
    #         MATCH (a:RefModel)-[r:DFG]->(b:RefModel)-[s:DFG]->(c:RefModel)
    #         WHERE
    #         c.Name = a.Name AND // detect a-b-a pattern in model,
    #         x.Name = a.Name AND // find the matched a-b pattern in traces
    #         y.Name = b.Name AND
    #         x.Name <> z.Name  // if not a-b-a then it's a concurrent else it's a shortloop
    #         //AND r.dff > 100 AND s.dff > 100 // concurrent threshold
    #         MERGE (a)-[:CONCURRENT {rel:'NONE', dff:r.dff}]->(b)
    #         DELETE r;
    #         '''
    # session.run(q_concurrent)
    # return None

    generalHelper.removeSelfLoop(session)


    q_concurrent = '''
            MATCH (p:RefModel)-[v:DFG]->(a:RefModel)-[r:DFG]->(b:RefModel)-[s:DFG]->(c:RefModel)-[w:DFG]->(q:RefModel)
            WITH a,r,b,s,c
            MATCH (i1)-->(a)-->(o1) // shortloop
            WITH a,r,b,s,c, i1, o1
            MATCH (i2)-->(b)-->(o2)
            WHERE 
            c.Name = a.Name // detect a-b-a pattern in model (tp di trace tdk boleh a=c)
            //AND NOT (a)-->(a)
            //AND NOT (b)-->(b)
            AND i1.Name <> b.Name // harus ada input output selain konkuren
            AND NOT (a)-->(i1)-->(a) // bukan shortloop
            AND i2.Name <> a.Name AND NOT (b)-->(i2)-->(b)
            AND o1.Name <> b.Name AND NOT (a)-->(o1)-->(a)
            AND o2.Name <> a.Name AND NOT (b)-->(o2)-->(b)
            WITH a,b,c, r, s
            MATCH (x:Trace)-[p:DFG]->(y:Trace)-[q:DFG]->(z:Trace) // trace 1
            WITH a,b,c, r, s, x, p, y, q, z
            MATCH (l:Trace)-[t:DFG]->(m:Trace)-[u:DFG]->(n:Trace) // trace 2
            WHERE 
            x.Name = m.Name AND y.Name = l.Name AND// trace 1 dan 2 berlawanan arah
            x.Name = a.Name AND // matched a-b pattern in traces with model
            y.Name = b.Name AND
            x.Name <> z.Name  // a-b-a is not a concurrent it's a shortloop
            //AND r.dff > 100 AND s.dff > 100 // concurrent threshold
            MERGE (a)-[:CONCURRENT {rel:'NONE', dff:r.dff}]->(b)
            DELETE r;
            '''
    session.run(q_concurrent)

    # q_shortloop = '''
    #
    #         '''
    # session.run(q_shortloop)

    return None

#Delete all traces model
def deleteTrace(session):
    query="MATCH (N:Trace) detach delete (N)"
    session.run(query)
    return None

def splitJoinInit(session):
    splitHelper.setOutDegreeInNodes(session)
    splitHelper.setSplitNodes(session)
    joinHelper.setInDegreeInNodes(session)
    joinHelper.setJoinNodes(session)
    return None

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # bagian ini diaktifkan ketika belum punya file csv nya
    # filename = "./data/bpic_2012/BPIC12_AO_Start_End.xes.gz"
    # saveName = "bpic12_AO.csv"
    # csv_file_name = preprocessing.convertToCsv(filename, saveName)
    # print(csv_file_name)

    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth = ("neo4j", "faset123"))
    session = driver.session()

    # Model DFG preparation
    clearAll(session)
    # filename = 'df_green_old.csv'
    # filename = 'df_red.csv'
    # filename = 'df_red_ok.csv'
    # filename = 'df_yellow.csv'
    filename = 'df_yellow_f10.csv'
    #
    # filename = "bpic12_A.csv"
    # filename = "bpic12_O.csv"
    # filename = "df_bpic12_AO.csv" # ada AND pendek
    # filename = "df_bpic12_AW.csv" #
    # filename = "df_bpic12_W.csv" #
    # filename = "df_bpic12_AO_all_30.csv"
    # filename = "df_bpic13_cp.csv"
    # filename = "df_bpic17.csv"
    # filename = "df_bpic17_O.csv"
    # filename = "df_bpic17_A.csv"
    # filename = "df_bpic17_W.csv"
    # filename = "df_bpic17_AW.csv"
    # filename = "df_bpic17_W_30.csv"
    # filename = "df_bpic14_30.csv"
    # filename = "df_bpic19_1.csv"

    # filename = csv_file_name

    constructDFFRefModel(session,'file:///'+ filename) # XOR AND

    deleteTrace(session)
    splitJoinInit(session)
    mainCounter = 0

    # sisipkan invisible task ke loop
    # generalHelper.detectLoopInsertInvTask(session)


    # Init variables
    counter = 0
    initialNodeName = 'START' # ganti dengan get initial node name
    gpd = gpd.GraphbasedDiscovery(session)

    join_GWlist, joinXORList, joinANDList = gpd.discoverGW(session, initialNodeName, counter)  # node split terdekat
    print('GWlist= ', join_GWlist, 'joinXORList= ', joinXORList, 'joinANDList= ', joinANDList)
    #

    # urutkan dari joinNode yang terjauhy daulu

    # detect xor atau and join
    for g in join_GWlist:
        joinGW_name = g[0]
        splitNode = g[1]
        exitNodes = g[2]
        joinNodeName = g[3]
        # check jika antara exitNode dan joinNode ada type node lain maka replace exitNode dg nodeLain terdekat
        exitNodes = generalHelper.updateExitNodeWithTheClosestNodeToJoinNode(session, splitNode, exitNodes, joinNodeName)
        if len(exitNodes) == 0 :
            continue # jika exitNodes is not valid maka skip, lanjut gw berikutnya

        if g[0][:3] == 'and':
            discoverAND.insertANDJoinGW(session, exitNodes, joinGW_name, joinNodeName)
        elif g[0][:3] == 'xor':
            discoverXOR.insertXORJoinGW(session, exitNodes, joinGW_name, joinNodeName)
        # setelah insert joinGW maka jika ada joinNodeName di joinGWList di replace dengan JoinNodename
        for joinNode in join_GWlist:
            if joinNode[3] == joinNodeName:
                joinNode[3] = joinGW_name



    splitHelper.setOutDegreeInNodes(session)
    joinHelper.setInDegreeInNodes(session)
    # sekuens gateway dengan tipe sama dilebur
    while True:
        result = True
        result = generalHelper.mergeSameGwInSequence(session) # merge yang bukan hierarki
        if result == False:
            break

    # lengkapi antar split dan antar join dengan invisible task
    # generalHelper.insertInvisibleTaskBetweenConsecutiveGateway(session)

    # pola join yang belum punya gateway diberi gerbang XOR
    result = joinHelper.detectLoopJoin(session)
    for loopjoin in result:
        exits = loopjoin[0]
        joinNodeName = loopjoin[1]
        mainCounter = mainCounter + 1
        discoverXOR.insertXORJoinGW(session,exits,"xorLoopOrJoinGW_" + str(mainCounter), joinNodeName)

    splitHelper.setOutDegreeInNodes(session)
    joinHelper.setInDegreeInNodes(session)
    # lengkapi antar split dan antar join dengan invisible task
    generalHelper.insertInvisibleTaskBetweenConsecutiveGateway(session)

    # # Hapus pola invisible task dengan relatinship cuncurrent
    # generalHelper.removeInvisibleTaskWithConcurrentRelationship(session)

    petri_net = PetriNet("petri_net")
    petri_net, petri_im, petri_fm = saveToPnml.saveToPnml(session, petri_net)

    from pm4py.visualization.petrinet import visualizer as pn_vis_factory

    gviz = pn_vis_factory.apply(petri_net, petri_im, petri_fm)
    pn_vis_factory.view(gviz)

    pm4py.write_pnml(petri_net, petri_im, petri_fm, './pnml/'+filename+'.pnml')














# See PyCharm help at https://www.jetbrains.com/help/pycharm/
