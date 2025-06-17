import json
import base64
from datetime import datetime

# input
# {
#   "id": "id1234567",
#   "pickupDate": "2025-06-17T14:00:00",
#   "dropoffDate": "2025-06-17T15:20:00",
#   ...
# }

# output
# {
#   "id": "id1234567",
#   "pickupTimestamp": 1750178400,
#   "dropoffTimestamp": 1750184400,
#   ...
# }

def lambda_handler(event, context):
    output = []

    for record in event['records']:
        # Kinesis Firehose는 Base64로 인코딩된 JSON 데이터를 전달
        payload = base64.b64decode(record['data'])
        
        try:
            data = json.loads(payload)

            # 날짜 포맷 변환: ISO8601 → Unix timestamp
            data['pickupTimestamp'] = int(datetime.fromisoformat(data['pickupDate']).timestamp())
            data['dropoffTimestamp'] = int(datetime.fromisoformat(data['dropoffDate']).timestamp())

            # 원래 날짜 필드는 제거 (원한다면 주석처리 가능)
            del data['pickupDate']
            del data['dropoffDate']

            # Firehose가 요구하는 반환 포맷
            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': base64.b64encode((json.dumps(data) + '\n').encode('utf-8')).decode('utf-8')
            }

        except Exception as e:
            # 실패한 경우에도 데이터를 그대로 넘기되, 상태는 실패로 표기
            output_record = {
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data']
            }

        output.append(output_record)

    return {'records': output}
