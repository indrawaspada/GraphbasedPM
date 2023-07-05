import pm4py
import pandas as pd

def convertToCsv(filename, saveFileName):
    # path = ".data/red/"
    # filename = "red_no_flag_custDel_jobDel_from_prom.xes.gz"
    eventlog_awal = pm4py.read_xes(filename)
    # deteksi daftar variant
    variants = pm4py.get_variants_as_tuples(eventlog_awal)

    # tiap event ditambah atribut bernama frequency, yang merupakan frequency dari variant
    variants_keys = variants.keys()
    highfreq_variants = pm4py.objects.log.obj.EventLog()
    lowfreq_variants = pm4py.objects.log.obj.EventLog()
    for key in variants_keys:
        frequency = len(variants[key])
        threshold = 100

        if frequency >= threshold:  # high freq variants
            trace_variant = variants[key][0]
            for event in trace_variant:  # tambahkan frekuensi pada setiap event
                event['frequency'] = frequency
            highfreq_variants.append(trace_variant)
        else:  # low freq variants
            trace_variant = variants[key][0]
            for event in trace_variant:  # tambahkan frekuensi pada setiap event
                event['frequency'] = frequency
            lowfreq_variants.append(trace_variant)

    df_highfreq_variants = pm4py.convert_to_dataframe(highfreq_variants)

    # update column name
    df_highfreq_variants.rename(
        columns={'concept:name': 'event', 'case:concept:name': 'case', 'frequency': 'case_frequency'}, inplace=True)
    # df_highfreq_variants = df_highfreq_variants.drop('index', axis=1)
    df_highfreq_variants.index.name = 'index'

    # EXPORT file
    df_highfreq_variants.to_csv('df_bpic_2012_A.csv', index=True)

    #################################### lanjut ke bagian 3
    df_event_log = pd.read_csv('df_bpic_2012_A.csv')
    df_event_log = df_event_log.drop('index', axis=1)
    df_event_log.index.name = 'index'

    # Simpan ke Neo4j

    import os.path
    folder = 'C:\\Users\\User\\AppData\\Local\\Neo4j\\Relate\\Data\\dbmss\\dbms-6688132d-b071-4263-8d05-cc20cd5fd182\\import'
    csv_file_name = saveFileName
    file_path = os.path.join(folder, csv_file_name)
    df_event_log.to_csv(file_path)




    return csv_file_name