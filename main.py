# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from neo4j import GraphDatabase
from helper import joinHelper, splitHelper
import gpd
from bulk import gateway


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.

#Delete all nodes with relationship
def clearAll():
    query="MATCH (N) detach delete (N)"
    session.run(query)
    return None

def constructDFFRefModel(fileName):
    q_trace = '''
                USING PERIODIC COMMIT
                LOAD CSV with headers FROM $fileName AS line
                MERGE (:Trace {LineNo:toInteger(line.index), CaseId: line.case, Name:line.event, Frequency:toInteger(line.frequency), idff:0, odff:0});
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

    q_concurrent = '''
            MATCH (x:Trace)-[p:DFG]->(y:Trace)-[q:DFG]->(z:Trace)
            WITH x,y,z
            MATCH (a:RefModel)-[r:DFG]->(b:RefModel)-[s:DFG]->(c:RefModel) 
            WHERE 
            c.Name = a.Name AND // detect a-b-a pattern in model,
            x.Name = a.Name AND // find the matched a-b pattern in traces
            y.Name = b.Name AND
            x.Name <> z.Name  // if not a-b-a then it's a concurrent else it's a shortloop
            //AND r.dff > 100 AND s.dff > 100 // concurrent threshold
            MERGE (a)-[:CONCURRENT {rel:'NONE', dff:r.dff}]->(b)
            DELETE r;
            '''
    session.run(q_concurrent)
    return None

#Delete all traces model
def deleteTrace():
    query="MATCH (N:Trace) detach delete (N)"
    session.run(query)
    return None

def splitJoinInit():
    splitHelper.setOutDegreeInNodes(session)
    splitHelper.setSplitNodes(session)
    joinHelper.setInDegreeInNodes(session)
    joinHelper.setJoinNodes(session)
    return None

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth = ("neo4j", "faset123"))
    session = driver.session()

    # Model DFG preparation
    clearAll()
    constructDFFRefModel('file:///df_green.csv')
    # constructDFFRefModel('file:///df_red.csv')
    deleteTrace()
    splitJoinInit()

    # Init variables
    counter = 0
    initialNodeName = 'START' # ganti dengan get initial node name
    gpd = gpd.GraphbasedDiscovery(session, gateway)
    gate = gateway.GateDiscovery(session)

    GWlist, joinXORList, joinANDList = gpd.discoverGW(session, initialNodeName, counter)  # node split terdekat
    print('GWlist= ', GWlist, 'joinXORList= ', joinXORList, 'joinANDList= ', joinANDList)
    #

    # setelah split selesai, maka periksa JOIN untuk disisipkan
    for ANDjoin in joinANDList:
        print('ANDjoin= ', ANDjoin)  # [['join_and_gw_0', ['VESSEL_ATB', 'BAPLIE']]]
        andJoinGW_name = ANDjoin[0]
        exitNodes = ANDjoin[1]
        joinNodeName = ANDjoin[2]
        gate.insertANDJoinGW(session, exitNodes, andJoinGW_name, joinNodeName)
    #
    #
    #
    #









# See PyCharm help at https://www.jetbrains.com/help/pycharm/
