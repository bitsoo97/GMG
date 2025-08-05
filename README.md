# GMG
#실험적으로 갖다 붙인 코드 입니당

from google.cloud import bigquery
import pandas as pd

# BigQuery 클라이언트 생성
client = bigquery.Client(project="intrepid-snow-467703-i7")

# 쿼리 정의 (별칭 cu 지정)
query = """
SELECT visitNumber, visitId, visitStartTime, date, totals, trafficSource, device, geoNetwork,hits,channelGrouping
FROM `team3-467708.team3.ga_view`
limit 1000
"""

# 쿼리 실행 및 결과를 DataFrame으로 저장
query_job = client.query(query)
df = query_job.result().to_dataframe()



# 예시용으로 df 생성했다고 가정 (실제론 BigQuery에서 read_gbq 등으로 불러옴)
# df = pd.read_gbq('SELECT ...')

# 하위 필드를 안전하게 꺼내는 함수
def get_nested_value(d, keys):
    try:
        for k in keys:
            if isinstance(d, list):  # hits 같은 경우
                d = d[0] if d else {}
            d = d.get(k, {})
        return d if not isinstance(d, dict) else None
    except Exception:
        return None

# totals 컬럼에서 꺼내기
df["pageviews"] = df["totals"].apply(lambda x: x.get("pageviews") if isinstance(x, dict) else None)
df["timeOnSite"] = df["totals"].apply(lambda x: x.get("timeOnSite") if isinstance(x, dict) else None)

# trafficSource 컬럼에서 꺼내기
df["source"] = df["trafficSource"].apply(lambda x: x.get("source") if isinstance(x, dict) else None)
df["medium"] = df["trafficSource"].apply(lambda x: x.get("medium") if isinstance(x, dict) else None)
df["keyword"] = df["trafficSource"].apply(lambda x: x.get("keyword") if isinstance(x, dict) else None)

# device 컬럼에서 꺼내기
df["browser"] = df["device"].apply(lambda x: x.get("browser") if isinstance(x, dict) else None)
df["operatingSystem"] = df["device"].apply(lambda x: x.get("operatingSystem") if isinstance(x, dict) else None)
df["deviceCategory"] = df["device"].apply(lambda x: x.get("deviceCategory") if isinstance(x, dict) else None)

# geoNetwork 컬럼에서 꺼내기
df["subcontinent"] = df["geoNetwork"].apply(lambda x: x.get("subContinent") if isinstance(x, dict) else None)
df["country"] = df["geoNetwork"].apply(lambda x: x.get("country") if isinstance(x, dict) else None)
df["city"] = df["geoNetwork"].apply(lambda x: x.get("city") if isinstance(x, dict) else None)
df["networkDomain"] = df["geoNetwork"].apply(lambda x: x.get("networkDomain") if isinstance(x, dict) else None)

# hits[0] 에서 꺼내기
df["hit_hour"] = df["hits"].apply(lambda x: x[0].get("hour") if isinstance(x, list) and x else None)
df["isInteraction"] = df["hits"].apply(lambda x: x[0].get("isInteraction") if isinstance(x, list) and x else None)

# latencyTracking.pageLoadTime
df["pageLoadTime"] = df["hits"].apply(
    lambda x: x[0].get("latencyTracking", {}).get("pageLoadTime") 
    if isinstance(x, list) and x else None
)
