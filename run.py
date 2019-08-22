import boto3
import requests
import io
import time
import json

PDF_LOCATION = "https://d32ze2gidvkk54.cloudfront.net/Amazon_Route_53_Domain_Registration_Pricing_20140731.pdf"
BUCKET_NAME = "ianmckay-us-west-2"
KEY_NAME = "r53pricing.pdf"

textract_client = boto3.client('textract', region_name='us-west-2')

r = requests.get(PDF_LOCATION, allow_redirects=True)

s3_client = boto3.client('s3', region_name='us-west-2')
s3_client.put_object(Body=r.content, Bucket=BUCKET_NAME, Key=KEY_NAME, ACL='public-read')

analysis_job = textract_client.start_document_analysis(
    DocumentLocation={
        'S3Object': {
            'Bucket': BUCKET_NAME,
            'Name': KEY_NAME
        }
    },
    FeatureTypes=[
        'TABLES'
    ]
)

analysis_result = textract_client.get_document_analysis(
    JobId=analysis_job['JobId']
)

while analysis_result['JobStatus'] == "IN_PROGRESS":
    time.sleep(10)
    analysis_result = textract_client.get_document_analysis(
        JobId=analysis_job['JobId']
    )

while 'NextToken' in analysis_result:
    tmp_result = textract_client.get_document_analysis(
        JobId=analysis_job['JobId'],
        NextToken=analysis_result['NextToken']
    )
    tmp_result['Blocks'].extend(analysis_result['Blocks'])
    analysis_result = tmp_result

rows = {}

for block in analysis_result['Blocks']: # document tables
    if block['BlockType'] == 'TABLE':
        print("Table")
        for relationship in block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for rid in relationship['Ids']:
                    for block2 in analysis_result['Blocks']: # table cells
                        if block2['Id'] == rid:
                            if block2['RowIndex'] > 3:
                                if block2['ColumnIndex'] == 1:
                                    rows[block2['Page']*1000 + block2['RowIndex']] = {'tld': ''}
                                if 'Relationships' in block2:
                                    for relationship2 in block2['Relationships']:
                                        if relationship2['Type'] == 'CHILD':
                                            for rid2 in relationship2['Ids']:
                                                for block3 in analysis_result['Blocks']: # cell words
                                                    if block3['Id'] == rid2:
                                                        if block2['ColumnIndex'] == 1:
                                                            rows[block2['Page']*1000 + block2['RowIndex']]['tld'] = block3['Text'].lower()
                                                        elif block2['ColumnIndex'] == 2:
                                                            if 'registration_price' in rows[block2['Page']*1000 + block2['RowIndex']]:
                                                                rows[block2['Page']*1000 + block2['RowIndex']]['registration_price'] += " " + block3['Text']
                                                            else:
                                                                rows[block2['Page']*1000 + block2['RowIndex']]['registration_price'] = block3['Text']
                                                        elif block2['ColumnIndex'] == 3:
                                                            if 'change_ownership_price' in rows[block2['Page']*1000 + block2['RowIndex']]:
                                                                rows[block2['Page']*1000 + block2['RowIndex']]['change_ownership_price'] += " " + block3['Text']
                                                            else:
                                                                rows[block2['Page']*1000 + block2['RowIndex']]['change_ownership_price'] = block3['Text']
                                                        elif block2['ColumnIndex'] == 4:
                                                            if 'restoration_price' in rows[block2['Page']*1000 + block2['RowIndex']]:
                                                                rows[block2['Page']*1000 + block2['RowIndex']]['restoration_price'] += " " + block3['Text']
                                                            else:
                                                                rows[block2['Page']*1000 + block2['RowIndex']]['restoration_price'] = block3['Text']
                                                        elif block2['ColumnIndex'] == 5:
                                                            if 'transfer_price' in rows[block2['Page']*1000 + block2['RowIndex']]:
                                                                rows[block2['Page']*1000 + block2['RowIndex']]['transfer_price'] += " " + block3['Text']
                                                            else:
                                                                rows[block2['Page']*1000 + block2['RowIndex']]['transfer_price'] = block3['Text']
                                                        elif block2['ColumnIndex'] == 6:
                                                            if 'transfer_term' in rows[block2['Page']*1000 + block2['RowIndex']]:
                                                                rows[block2['Page']*1000 + block2['RowIndex']]['transfer_term'] += " " + block3['Text']
                                                            else:
                                                                rows[block2['Page']*1000 + block2['RowIndex']]['transfer_term'] = block3['Text']

obj = {'prices': {}}
for item in rows.values():
    obj['prices'][item['tld']] = item

with open('domains.json', 'w') as f:
    f.write(json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': ')))

