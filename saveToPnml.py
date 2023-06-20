from helper import bpmnToPetrinetHelper, petrinetToPnmlHelper

def saveToPnml(session, petri_net):
    bpmnToPetrinetHelper.xorSplit_bpmnToPetriNet(session)
    bpmnToPetrinetHelper.xorJoin_bpmnToPetriNet(session)
    bpmnToPetrinetHelper.andSplit_bpmnToPetriNet(session)
    bpmnToPetrinetHelper.andJoin_bpmnToPetriNet(session)
    bpmnToPetrinetHelper.sequence_bpmnToPetrinet(session)
    bpmnToPetrinetHelper.remove_DFG(session)
    bpmnToPetrinetHelper.remove_Concurrent(session)
    bpmnToPetrinetHelper.renameLabel(session, 'GW', 'Activity')
    bpmnToPetrinetHelper.renameLabel(session, 'Activity', 'Transition')
    bpmnToPetrinetHelper.removeLabel_BC_RefModel_TPS(session)

    petrinetToPnmlHelper.gdb_setMasterInitialMarking(session)
    petrinetToPnmlHelper.gdb_setMasterFinalMarking(session)

    modelInList = petrinetToPnmlHelper.savePetrinetAsList(session)

    petri_net, petri_im, petri_fm = petrinetToPnmlHelper.buildPnml(modelInList, petri_net)
    return petri_net, petri_im, petri_fm
