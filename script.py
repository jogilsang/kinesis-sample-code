# 필요한 라이브러리들을 가져옵니다.
import json  # JSON 형식의 데이터를 다루기 위한 라이브_러리
import boto3  # AWS 서비스(이 코드에서는 Kinesis)와 상호작용하기 위한 AWS SDK for Python
import csv  # CSV 파일을 다루기 위한 라이브러리 (이 코드에서는 사용되지 않음)
import datetime  # 날짜와 시간 관련 작업을 위한 라이브러리
import random  # 무작위 숫자를 생성하기 위한 라이브러리
import time
import sys
from datetime import timedelta  # 날짜와 시간의 차이를 계산하기 위한 라이브러리

# 스크립트를 사용자 종료가 없을경우, 최대 20분 동안 실행합니다. (20분 * 60초 = 1200초)
DURATION_SECONDS = 20 * 60

# AWS Kinesis Data Stream의 이름과 리전(Region)을 설정합니다.
kdsname = None  # 데이터를 보낼 Kinesis 스트림의 이름
region = None  # 해당 Kinesis 스트림이 위치한 AWS 리전
print(f"이 스크립트는 별도의 종료요청이 없다면, {int(DURATION_SECONDS / 60)}분 후 자동으로 종료됩니다.")

# 명령줄에서 2개의 인수를 제공했는지 확인합니다.
if len(sys.argv) > 2:
    # 첫 번째 인수를 kdsname으로, 두 번째 인수를 region으로 할당합니다.
    kdsname = sys.argv[1]
    region = sys.argv[2]
else:
    # 인수가 올바르지 않으면 사용법을 안내하고 스크립트를 종료합니다.
    print("오류: Kinesis 스트림 이름과 리전, 두 개의 인수가 필요합니다.")
    print("사용법: python script.py [스트림_이름] [리전]")
    sys.exit() # 프로그램 종료

# 할당된 변수들을 출력하여 확인합니다.
print(f"Kinesis 스트림의 이름: {kdsname}")
print(f"해당 Kinesis 스트림이 위치한 AWS 리전: {region}")

# 전송된 데이터 레코드의 수를 세기 위한 카운터 변수를 초기화합니다.
i = 0

# boto3를 사용하여 Kinesis 클라이언트를 생성합니다. 이 클라이언트를 통해 Kinesis와 통신합니다.
# 'kinesis' 서비스를 사용하고, 지정된 'region_name'으로 설정합니다.
clientkinesis = boto3.client('kinesis', region_name=region)


# 무작위 위도와 경도 좌표를 반환하는 함수입니다.
def getlatlon():
    # 미리 정의된 위도, 경도 좌표 문자열의 리스트입니다. (뉴욕시의 좌표로 보임)
    a = ["-73.98174286,40.71915817", "-73.98508453, 40.74716568", "-73.97333527,40.76407242",
         "-73.99310303,40.75263214",
         "-73.98229218,40.75133133", "-73.96527863,40.80104065", "-73.97010803,40.75979996",
         "-73.99373627,40.74176025", "-73.98544312,	40.73571014",
         "-73.97686005,40.68337631", "-73.9697876,40.75758362", "-73.99397278,40.74086761",
         "-74.00531769,40.72866058", "-73.99013519, 40.74885178",
         "-73.9595108, 40.76280975", "-73.99025726,	40.73703384", "-73.99495697,40.745121",
         "-73.93579865,40.70730972", "-73.99046326,40.75100708",
         "-73.9536438,40.77526093", "-73.98226166,40.75159073", "-73.98831177,40.72318649",
         "-73.97222137,40.67683029", "-73.98626709,40.73276901",
         "-73.97852325,	40.78910065", "-73.97612, 40.74908066", "-73.98240662,	40.73148727",
         "-73.98776245,40.75037384", "-73.97187042,40.75840378",
         "-73.87303925,	40.77410507", "-73.9921875,	40.73451996", "-73.98435974,40.74898529",
         "-73.98092651,40.74196243", "-74.00701904,40.72573853",
         "-74.00798798,	40.74022675", "-73.99419403,40.74555969", "-73.97737885,40.75883865",
         "-73.97051239,40.79664993", "-73.97693634,40.7599144",
         "-73.99306488,	40.73812866", "-74.00775146,40.74528885", "-73.98532867,40.74198914",
         "-73.99037933,40.76152802", "-73.98442078,40.74978638",
         "-73.99173737,	40.75437927", "-73.96742249,40.78820801", "-73.97813416,40.72935867",
         "-73.97171021,40.75943375", "-74.00737,40.7431221",
         "-73.99498749,	40.75517654", "-73.91600037,40.74634933", "-73.99924469,40.72764587",
         "-73.98488617,40.73621368", "-73.98627472,40.74737167"
         ]
    # 0부터 53까지의 정수 중에서 무작위로 하나를 선택합니다 (리스트의 인덱스).
    randomnum = random.randint(0, 53)
    # 무작위로 선택된 인덱스에 해당하는 좌표 문자열을 가져옵니다.
    b = a[randomnum]
    # 선택된 좌표 문자열을 반환합니다.
    return b


# 'store and forward' 플래그 값을 무작위로 반환하는 함수입니다.
def getstore():
    # 'Y'와 'N' 값을 가진 리스트 (이 코드에서는 직접 사용되지 않음)
    taxi = ['Y', 'N']
    # 0 또는 1을 무작위로 선택합니다.
    randomnum = random.randint(0, 1)
    # 선택된 숫자(0 또는 1)를 반환합니다.
    return randomnum

# @JsonPropertyOrder(...)는 주석 처리되어 있습니다.
# 이것은 주로 Java의 Jackson 라이브러리에서 JSON을 생성할 때 필드 순서를 지정하기 위해 사용되는 어노테이션입니다.
# 이 파이썬 스크립트에서는 아무런 기능도 하지 않으며, 데이터 구조에 대한 메모로 남겨둔 것으로 보입니다.


# 시작 시간 기록
start_time = time.time()

# 무한 루프를 시작하여 지속적으로 데이터를 생성하고 Kinesis로 전송합니다.
while (time.time() - start_time) < DURATION_SECONDS:
    # 카운터 변수 i를 1 증가시킵니다.
    i = int(i) + 1
    
    # --- Kinesis로 보낼 가상의 택시 운행 데이터를 생성합니다. ---
    
    # 'id' + 1665586~8888888 사이의 무작위 숫자로 고유 ID를 생성합니다.
    id = 'id' + str(random.randint(1665586, 8888888))
    # 택시 회사 ID를 1 또는 2 중에서 무작위로 선택합니다.
    vendorId = random.randint(1, 2)
    # 승차 시각을 현재 시각으로 설정하고, ISO 8601 형식의 문자열로 변환합니다.
    pickupDate = datetime.datetime.now().isoformat()
    # 하차 시각을 현재 시각으로부터 30분에서 100분 사이의 무작위 시간을 더하여 설정합니다.
    dropoffDate = datetime.datetime.now() + timedelta(minutes=random.randint(30, 100))
    # 하차 시각을 ISO 8601 형식의 문자열로 변환합니다.
    dropoffDate = dropoffDate.isoformat()
    # 승객 수를 1에서 9 사이의 무작위 숫자로 설정합니다.
    passengerCount = random.randint(1, 9)
    # getlatlon() 함수를 호출하여 승차 지점의 위도, 경도 좌표를 가져옵니다.
    location = getlatlon()
    # 가져온 좌표 문자열을 ','를 기준으로 분리하여 리스트로 만듭니다.
    location = location.split(",")
    # 분리된 리스트에서 경도와 위도를 각각 변수에 저장합니다.
    pickupLongitude = location[0]
    pickupLatitude = location[1]
    # getlatlon() 함수를 다시 호출하여 하차 지점의 위도, 경도 좌표를 가져옵니다.
    location = getlatlon()
    location = location.split(",")
    dropoffLongitude = location[0]
    dropoffLatitude = location[1]
    # getstore() 함수를 호출하여 'store and forward' 플래그 값을 가져옵니다 (0 또는 1).
    storeAndFwdFlag = getstore()
    # 직선 거리(Great-circle distance)를 1에서 7 사이의 무작위 정수로 설정합니다.
    gcDistance = random.randint(1, 7)
    # 총 운행 시간을 8초에서 10000초 사이의 무작위 정수로 설정합니다.
    tripDuration = random.randint(8, 10000)
    # 구글 지도 기반 거리를 위에서 생성한 직선 거리와 동일하게 설정합니다.
    googleDistance = gcDistance
    # 구글 지도 기반 소요 시간을 위에서 생성한 총 운행 시간과 동일하게 설정합니다.
    googleDuration = tripDuration

    # 생성된 데이터들을 담을 새로운 딕셔너리(dictionary)를 생성합니다.
    new_dict = {}
    new_dict["id"] = id
    new_dict["vendorId"] = vendorId
    new_dict["pickupDate"] = pickupDate
    new_dict["dropoffDate"] = dropoffDate
    new_dict["passengerCount"] = passengerCount
    new_dict["pickupLongitude"] = pickupLongitude
    new_dict["pickupLatitude"] = pickupLatitude
    new_dict["dropoffLongitude"] = dropoffLongitude
    new_dict["dropoffLatitude"] = dropoffLatitude
    new_dict["storeAndFwdFlag"] = storeAndFwdFlag
    new_dict["gcDistance"] = gcDistance
    new_dict["tripDuration"] = tripDuration
    new_dict["googleDistance"] = googleDistance
    new_dict["googleDuration"] = googleDuration

    # Kinesis 클라이언트를 사용하여 데이터를 스트림으로 전송합니다.
    # put_record: 단일 데이터 레코드를 Kinesis 스트림에 넣는 함수입니다.
    # - StreamName: 데이터를 보낼 스트림의 이름 (kdsname).
    # - Data: 전송할 데이터. 딕셔너리를 JSON 형식의 문자열로 변환하여 전달합니다.
    # - PartitionKey: 데이터를 스트림 내의 특정 샤드(shard)에 매핑하기 위한 키. 여기서는 운행 ID를 사용합니다.
    response = clientkinesis.put_record(StreamName=kdsname, Data=json.dumps(new_dict), PartitionKey=id)
    
    # 데이터 전송 후, 성공 여부와 관련 정보를 콘솔에 출력합니다.
    # - "Total ingested": 현재까지 전송한 총 레코드 수.
    # - "ReqID": AWS로부터 받은 요청 ID.
    # - "HTTPStatusCode": HTTP 응답 상태 코드 (200이면 성공).
    print("Total ingested:" + str(i) + ",ReqID:" + response['ResponseMetadata']['RequestId'] + ",HTTPStatusCode:" + str(
        response['ResponseMetadata']['HTTPStatusCode']))
    
    time.sleep(1)

# 루프가 끝나면 종료 메시지를 출력합니다.
print(f"\n지정된 {int(DURATION_SECONDS / 60)}분이 경과하여 스크립트를 자동 종료합니다.")