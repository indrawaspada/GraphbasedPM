
def andSplit_bpmnToPetriNet(session):
    q_andSplit_bpmnToPetriNet = '''
        MATCH (a:RefModel)-[r]->(x:GW)-[s]->(b:RefModel)
        WHERE x.type = 'andSplit'
        WITH a,x,b,r,s
        CREATE (p:Place {Name: 'p_split'+a.Name+'_'+b.Name, label: 'Invisible', token:0, c:0, p:0, m:0, fm:0, inv_incoming: true})
        MERGE (a)-[t:Arc]->(p)-[u:Arc]->(b)
        SET t.dff = r.dff, u.dff = s.dff 
        WITH distinct(r) as single_r, s, x
        DELETE single_r
        WITH s, x
        DELETE s
        WITH x
        DELETE x
        RETURN null
    '''
    session.run(q_andSplit_bpmnToPetriNet)

    # q_andSplit_bpmnToPetriNet = '''
    #     MATCH (a:RefModel)-[r]->(x:GW)-[s]->(b:RefModel)
    #     WHERE x.type = 'andSplit'
    #     SET x.type = 'activity', x.label = 'Invisible'
    #     WITH a,x,b,r,s
    #     CREATE (p:Place {Name: 'p_split'+a.Name+'_'+b.Name, label: 'Invisible', token:0, c:0, p:0, m:0, fm:0, inv_incoming: true})
    #     MERGE (x)-[t:Arc]->(p)-[u:Arc]->(b)
    #     SET t.dff = r.dff, u.dff = s.dff
    #     DELETE s
    #     RETURN null
    # '''
    # session.run(q_andSplit_bpmnToPetriNet)

def andJoin_bpmnToPetriNet(session):
    # q_andJoinDetect = '''
    #     OPTIONAL MATCH (a:RefModel)-[r]->(x:GW)-[s]->(b:RefModel)
    #     WHERE x.type = 'andJoin'
    #     RETURN x.Name
    # '''
    # result = session.run(q_andJoinDetect)

    q_andJoin_bpmnToPetriNet = '''
        MATCH (a:RefModel)-[r]->(x:GW)-[s]->(b:RefModel)
        WHERE x.type = 'andJoin'
        WITH a,x,b,r,s
        CREATE (p:Place {Name: 'p_and_join'+a.Name+'_'+b.Name, label: 'Invisible', token:0, c:0, p:0, m:0, fm:0, inv_incoming: true})
        MERGE (a)-[t:Arc]->(p)-[u:Arc]->(b)
        SET t.dff = r.dff, u.dff = s.dff 
        WITH distinct(s) as single_s, r, x
        DELETE single_s
        WITH r, x
        DELETE r
        WITH x
        DELETE x
        RETURN null
    '''

    session.run(q_andJoin_bpmnToPetriNet)
    # status = ''
    # for record in result:
    #     print(record[0])
    #     if record[0] is not None:
    #         status = 'and_join_exist'
    # if status == 'and_join_exist':
    #     session.run(q_andJoin_bpmnToPetriNet)


def xorSplit_bpmnToPetriNet(session):
    # q_xorSplitDetect = '''
    #     OPTIONAL MATCH (a:Activity)-[r]->(x:GW)-[s]->(b:Activity)
    #     WHERE x.type = 'xorSplit'
    #     RETURN x.Name
    # '''
    # result = session.run(q_xorSplitDetect)

    q_xorSplit_bpmnToPetriNet = '''
        MATCH (a:Activity)-[r]->(x:GW)-[s]->(b:Activity)
        WHERE x.type = 'xorSplit'
        WITH a,x,b,r,s
        CREATE (p:Place {Name: 'p_xor_split'+a.Name, label: 'Invisible', token:0, c:0, p:0, m:0, fm:0, inv_incoming: true})
        MERGE (a)-[t:Arc]->(p)-[u:Arc]->(b)
        SET t.dff = r.dff, u.dff = s.dff 
        WITH DISTINCT x, a, p
        DETACH DELETE x
        WITH a, collect(p) as nodes
        CALL apoc.refactor.mergeNodes(nodes,{properties:"combine", mergeRels:true}) 
        YIELD node
        RETURN null
    '''
    # status = ''
    # for record in result:
    #     print(record[0])
    #     if record[0] is not None:
    #         status = 'xor_split_exist'
    # if status == 'xor_split_exist':
    #     session.run(q_xorSplit_bpmnToPetriNet)

    session.run(q_xorSplit_bpmnToPetriNet)

def xorJoin_bpmnToPetriNet(session):
    # q_xorJoinDetect = '''
    #     OPTIONAL MATCH (a:Activity)-[r]->(x:GW)-[s]->(b:Activity)
    #     WHERE x.type = 'xorJoin'
    #     RETURN x.Name
    # '''
    # result = session.run(q_xorJoinDetect)

    q_xorJoin_bpmnToPetriNet = '''
        MATCH (a:Activity)-[r]->(x:GW)-[s]->(b:Activity)
        WHERE x.type = 'xorJoin'
        WITH a,x,b,r,s
        CREATE (p:Place {Name: 'p_xor_join'+b.Name, label: 'Invisible', token:0, c:0, p:0, m:0, fm:0, inv_incoming: true})
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
    # status = ''
    # for record in result:
    #     print(record[0])
    #     if record[0] is not None:
    #         status = 'xor_join_exist'
    # if status == 'xor_join_exist':
    #     session.run(q_xorJoin_bpmnToPetriNet)

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

