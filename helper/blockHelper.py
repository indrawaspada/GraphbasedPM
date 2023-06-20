def getTheNextHierarchies(mergedJoinNodeEnum):
    shortest = 1000
    closestHirarchies = []
    theJoinNode = ''
    for joinNode in mergedJoinNodeEnum:  # β
        for entrance_to_exit_pairs in mergedJoinNodeEnum[
            joinNode]:  # [[('BAPLIE', 'VESSEL_ATB'), ['VESSEL_ATB', 'BAPLIE']]]
            NumOfEntrances = len(entrance_to_exit_pairs[0])
            if NumOfEntrances < shortest:
                shortest = NumOfEntrances
                closestHirarchies = [[entrance_to_exit_pairs, joinNode]]
            elif NumOfEntrances == shortest:
                closestHirarchies.append([entrance_to_exit_pairs, joinNode])
    return closestHirarchies