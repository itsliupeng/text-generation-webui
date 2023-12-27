from elasticsearch import Elasticsearch, exceptions
import time
import logging

logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(name)s -   %(message)s",
        datefmt="%m/%d/%Y %H:%M:%S",
        level=logging.INFO)

es_host = "http://172.24.8.108:9200"
RETRY_TIMES = 10
WAIT_TIME = 10  # 等待10秒后重试

es = Elasticsearch(
    hosts=[es_host],
    retry_on_timeout=True,
    max_retries=RETRY_TIMES,
    timeout=30  # 设置连接超时时间为30秒
)


INDEX_KIND = {
    "CC": "common_crawl",
    "Wechat": "wechat",
    "Falcon": "falcon",
    "cc_d0822": "cc_d0822",
    "chunjie_en_cc": "chunjie_en_cc",
    "chunjie_zh_cc": "chunjie_zh_cc",
}


def es_get(doc_ids, data_kind):
    doc_ids = list(map(str, doc_ids))
    for _ in range(RETRY_TIMES):
        try:
            response = es.mget(index=INDEX_KIND[data_kind], ids=doc_ids)
            return [x['_source'] for x in response['docs']]

        except exceptions.ConnectionError:
            print(f"Connection failed, retrying in {WAIT_TIME} seconds...")
            time.sleep(WAIT_TIME)

    print("Failed to connect to Elasticsearch after several retries.")
    return []
    

def es_search(keyword: str, data_kind: str, count: int = 10):
    for _ in range(RETRY_TIMES):
        try:
            # 定义查询
            query = {
                "size": count,
                "query": {
                    "match": {
                        "content": keyword
                    }
                }
            }
            response = es.search(index=INDEX_KIND[data_kind], body=query, timeout="120s")
            doc_id_list = [hit["_id"] for hit in response["hits"]["hits"]]
            score_list = [hit["_score"] for hit in response["hits"]["hits"]]
            hit_source = [hit["_source"] for hit in response["hits"]["hits"]]
            content_list = [x['content'] for x in hit_source]
            if data_kind in ['cc_d0822', 'chunjie_en_cc']:
                # no cluster
                cluster_id = [-1] * len(content_list)
            else:
                cluster_id = [x['prediction'] for x in hit_source]


            return  doc_id_list, score_list, content_list, cluster_id

        except exceptions.ConnectionError:
            print(f"Connection failed, retrying in {WAIT_TIME} seconds...")
            time.sleep(WAIT_TIME)

    print("Failed to connect to Elasticsearch after several retries.")
    return [], [], [], []
    
    
    








