from pm4py.objects.petri import utils
from pm4py.objects.petri.petrinet import PetriNet, Marking

def gdb_setMasterInitialMarking(session):
    session.run('''
    MATCH (node:Transition)
    WHERE NOT ()-->(node)
    MERGE (im:Place {Name: 'im', label: 'Invisible', token:0, c:0, p:0, m:0, fm:0, inv_incoming: false})-[:Arc]->(node)
    ''')
    return None

def gdb_setMasterFinalMarking(session):
    session.run('''
    MATCH (node:Transition)
    WHERE NOT ()<--(node)
    MERGE (fm:Place {Name: 'fm', label: 'Invisible', token:0, c:0, p:0, m:0, fm:0, inv_incoming: false})<-[:Arc]-(node)
    ''')
    return None

def savePetrinetAsList(session):
    q_model_only = '''
            match (a)-[r]->(b)
            return a,r,b
            '''
    records = session.run(q_model_only)
    # Ambil data-data dari node dan relationship untuk di simpan
    model = []
    for rec in records:
        dfg = []  # menampung data (1) node source, (2) relationship, (3) node destination
        # 1 Source node
        print("===Source Node=============")
        sources = []  # menampund data2 node source
        sources.append("source")
        sources.append(dict(rec)['a'].id)  # tambahkan id
        x = dict(rec)['a'].labels
        for i in x:
            sources.append(i)
        sources.append(dict(rec)['a']['idff'])
        sources.append(dict(rec)['a']['odff'])
        sources.append(dict(rec)['a']['Name'])
        sources.append(dict(rec)['a']['label'])
        dfg.append(sources)
        print('dfg = ', dfg)
        print("node : ", dfg[0][0])
        print("id   : ", dfg[0][1])
        print("type : ", dfg[0][2])
        print("idff : ", dfg[0][3])
        print("odff : ", dfg[0][4])
        print("name : ", dfg[0][5])
        print("label : ", dfg[0][6])

        # 2 relationship
        print("===Relationship==============")
        rels = []  # menampund data2 relationship
        rels.append(dict(rec)['r'].type)
        rels.append(dict(rec)['r']['rel'])
        rels.append(dict(rec)['r']['dff'])
        dfg.append(rels)
        print("type : ", dfg[1][0])
        print("rel  : ", dfg[1][1])
        print("dff  : ", dfg[1][2])

        # 3 Destination node
        print("===Destination Node ==============")
        dests = []  # menampund data2 node destination
        dests.append("destination")
        dests.append(dict(rec)['b'].id)
        x = dict(rec)['b'].labels
        print('x=', x)
        for i in x:
            dests.append(i)
        dests.append(dict(rec)['b']['idff'])
        dests.append(dict(rec)['b']['odff'])
        print("dict(rec)['b']['Name']=", dict(rec)['b']['Name'])
        dests.append(dict(rec)['b']['Name'])
        print("dict(rec)['b']['label']=", dict(rec)['b']['label'])
        dests.append(dict(rec)['b']['label'])
        dfg.append(dests)
        print('dfg = ', dfg)
        print("node : ", dfg[2][0])
        print("id   : ", dfg[2][1])
        print("type : ", dfg[2][2])
        print("idff : ", dfg[2][3])
        print("odff : ", dfg[2][4])
        print("name : ", dfg[2][5])
        print("label : ", dfg[2][6])
        print("#########################")

        model.append(dfg)
    return model # model as list


def createPlaceToTransitionPair(placeName, placeLabel, transitionName, transitionLabel, petri_net):
    placeNames = []
    transNames = []
    for place in petri_net.places:
        placeNames.append(place.name)
    for trans in petri_net.transitions:
        transNames.append(trans.name)

    sourcePlace = PetriNet.Place(placeName)
    sourcePlace.properties['label'] = placeLabel
    # cek kalau source place sudah ada maka tidak perlu buat lagi
    if placeName not in placeNames:
        petri_net.places.add(sourcePlace)  # tambahkan place
    else:
        for place in petri_net.places:
            if place.name == placeName:
                sourcePlace = place

    destTrans = PetriNet.Transition(transitionName, transitionLabel)  # object t_1 with name, label
    if transitionName not in transNames:
        petri_net.transitions.add(destTrans)
    else:
        for trans in petri_net.transitions:
            if trans.name == transitionName:
                destTrans = trans

    utils.add_arc_from_to(sourcePlace, destTrans, petri_net)
    return None


def createTransitionToPlacePair(transitionName, transitionLabel, placeName, placeLabel, petri_net):
    placeNames = []
    transNames = []
    for place in petri_net.places:
        placeNames.append(place.name)
    for trans in petri_net.transitions:
        transNames.append(trans.name)

    sourceTrans = PetriNet.Transition(transitionName, transitionLabel)  # object t_1 with name, label
    if transitionName not in transNames:
        petri_net.transitions.add(sourceTrans)
    else:
        for trans in petri_net.transitions:
            if trans.name == transitionName:
                sourceTrans = trans

    destPlace = PetriNet.Place(placeName)
    destPlace.properties['label'] = placeLabel
    # cek kalau source place sudah ada maka tidak perlu buat lagi
    if placeName not in placeNames:
        petri_net.places.add(destPlace)  # tambahkan place
    else:
        for place in petri_net.places:
            if place.name == placeName:
                destPlace = place

    utils.add_arc_from_to(sourceTrans, destPlace, petri_net)
    return None

def buildPnml(model, petri_net):
    petri_im = Marking()
    petri_fm = Marking()
    for nodePair in model:
        placeNames = []
        transNames = []
        for place in petri_net.places:
            placeNames.append(place.name)
        for trans in petri_net.transitions:
            transNames.append(trans.name)

        # place --> transition
        if nodePair[0][2] == 'Place':  # node source berupa place dari suatu pairNode
            print('nodePair= ', nodePair)
            # source
            placeName = nodePair[0][5]
            placeLabel = nodePair[0][6]
            # destination
            transitionName = nodePair[2][5]
            transitionLabel = nodePair[2][6]
            createPlaceToTransitionPair(placeName, placeLabel, transitionName, transitionLabel, petri_net)

            if nodePair[0][5] == 'im':

                for place in petri_net.places:
                    if place.name == 'im':
                        print('place.name=', place.name)
                        source = place
                petri_im[source] = 1

        # transition --> place
        if nodePair[0][2] == 'Transition':  # node source berupa transition suatu pairNode
            # source
            transitionName = nodePair[0][5]
            transitionLabel = nodePair[0][6]
            # destination
            placeName = nodePair[2][5]
            placeLabel = nodePair[2][6]

            createTransitionToPlacePair(transitionName, transitionLabel, placeName, placeLabel, petri_net)

            if nodePair[2][5] == 'fm':
                if 'sink' not in placeNames:
                    for place in petri_net.places:
                        if place.name == 'fm':
                            sink = place

                    petri_fm[sink] = 1
    return petri_net, petri_im, petri_fm


