
def andSplit_bpmnToPetriNet(session):
    q_andSplit_bpmnToPetriNet = '''
        OPTIONAL MATCH (a:RefModel)-[r]->(x:GW)-[s]->(b:RefModel)
        WHERE x.type = 'andSplit'
        SET x.type = 'activity', x.label = 'Invisible'
        WITH a,x,b,r,s
        CREATE (p:Place {Name: 'p_'+a.Name+'_'+b.Name, label: 'Invisible', token:0, c:0, p:0, m:0, fm:0, inv_incoming: true})
        MERGE (x)-[t:Arc]->(p)-[u:Arc]->(b)
        SET t.dff = r.dff, u.dff = s.dff 
        DELETE s
        RETURN null
    '''
    session.run(q_andSplit_bpmnToPetriNet)

def andJoin_bpmnToPetriNet(session):
    q_andJoin_bpmnToPetriNet = '''
        OPTIONAL MATCH (a:RefModel)-[r]->(x:GW)-[s]->(b:RefModel)
        WHERE x.type = 'andJoin'
        SET x.type = 'activity', x.label = 'Invisible'
        WITH a,x,b,r,s
        CREATE (p:Place {Name: 'p_'+a.Name+'_'+b.Name, label: 'Invisible', token:0, c:0, p:0, m:0, fm:0, inv_incoming: true})
        MERGE (a)-[t:Arc]->(p)-[u:Arc]->(x)
        SET t.dff = r.dff, u.dff = s.dff 
        DELETE r
        RETURN null
    '''
    session.run(q_andJoin_bpmnToPetriNet)

def xorSplit_bpmnToPetriNet(session):
    q_xorSplit_bpmnToPetriNet = '''
        OPTIONAL MATCH (a:Activity)-[r]->(x:GW)-[s]->(b:Activity)
        WHERE x.type = 'xorSplit'
        WITH a,x,b,r,s
        CREATE (p:Place {Name: 'p_'+a.Name, label: 'Invisible', token:0, c:0, p:0, m:0, fm:0, inv_incoming: true})
        MERGE (a)-[t:Arc]->(p)-[u:Arc]->(b)
        SET t.dff = r.dff, u.dff = s.dff 
        WITH DISTINCT x, a, p
        DETACH DELETE x
        WITH a, collect(p) as nodes
        CALL apoc.refactor.mergeNodes(nodes,{properties:"combine", mergeRels:true}) 
        YIELD node
        RETURN null
    '''
    session.run(q_xorSplit_bpmnToPetriNet)


def xorJoin_bpmnToPetriNet(session):
    q_xorJoin_bpmnToPetriNet = '''
        OPTIONAL MATCH (a:Activity)-[r]->(x:GW)-[s]->(b:Activity)
        WHERE x.type = 'xorJoin'
        WITH a,x,b,r,s
        CREATE (p:Place {Name: 'p_'+b.Name, label: 'Invisible', token:0, c:0, p:0, m:0, fm:0, inv_incoming: true})
        MERGE (a)-[t:Arc]->(p)-[u:Arc]->(b)
        SET t.dff = r.dff, u.dff = s.dff 
        WITH DISTINCT x, p, b
        DETACH DELETE x
        WITH b, collect(p) as nodes
        CALL apoc.refactor.mergeNodes(nodes,{properties:"combine", mergeRels:true}) 
        YIELD node
        RETURN null
    '''
    session.run(q_xorJoin_bpmnToPetriNet)

def sequence_bpmnToPetrinet(session):
    q_sequenceAndANDPetrinet ='''
        MATCH (x:RefModel)-[r:DFG]->(y:RefModel)
        WHERE
        r.rel = 'Sequence' // OR r.rel = 'AND-SPLIT' OR r.rel = 'AND-JOIN'
        WITH x,y
        CREATE (p:Place {Name: 'p_'+x.Name+'_'+y.Name, label: 'Invisible', token:0, c:0, p:0, m:0, fm:0, inv_incoming: false})
        MERGE (x)-[:Arc ]->(p)-[:Arc]->(y)
        RETURN null
            '''
    session.run(q_sequenceAndANDPetrinet)

def remove_DFG(session):
    q_removeDfg = '''
            MATCH (n:RefModel)-[r:DFG]->(m:RefModel)
            DELETE r
    '''
    session.run(q_removeDfg)

def remove_Concurrent(session):
    q_removeConcurrent = '''
            MATCH (n)-[r:CONCURRENT]->()
            DELETE r
    '''
    session.run(q_removeConcurrent)

def renameLabel(session, oldName, newName):
    q_renameLabel = '''
        MATCH (t:RefModel)
        WITH collect(t) AS trans
        CALL apoc.refactor.rename.label($oldName, $newName, trans)
        YIELD batches, total, timeTaken, committedOperations
        RETURN batches, total, timeTaken, committedOperations;
    '''
    session.run(q_renameLabel, oldName=oldName, newName=newName)

def removeLabel_BC_RefModel_TPS(session):
    q_removeLabel = '''
        MATCH (n)
        REMOVE n:BC, n:RefModel, n:TPS
    '''
    session.run(q_removeLabel)

