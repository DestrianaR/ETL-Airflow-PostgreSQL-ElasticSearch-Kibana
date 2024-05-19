def loadData():
    from elasticsearch import Elasticsearch
    import pandas as pd

    es = Elasticsearch('http://elasticsearch:9200') 
    df=pd.read_csv('/opt/airflow/data/text_processing.csv')
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="data_product", doc_type="doc", body=doc)
        print(res)