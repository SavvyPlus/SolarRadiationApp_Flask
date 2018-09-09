from flask import Flask
from flask import render_template
from flask import request
import boto3
import json


from data_process import process

app = Flask(__name__)


# app.config.from_object(Config)
# db = SQLAlchemy(app)

@app.route('/test1')
def hello_world():
    # conn_str = 'awsathena+jdbc://{access_key}:{secret_key}@athena.{region_name}.amazonaws.com:443/' \
    #            '{schema_name}?s3_staging_dir={s3_staging_dir}'
    # engine = create_engine(conn_str.format(
    #     access_key=quote_plus('AKIAIEZK2WKUI276JE2A'),
    #     secret_key=quote_plus('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'),
    #     region_name='ap-southeast-2',
    #     schema_name='solar_radiation',
    #     s3_staging_dir=quote_plus('s3://solar-radiation/solar_test')))
    # try:
    #     with contextlib.closing(engine.connect()) as conn:
    #         many_rows = Table('bom_prod_2_output', MetaData(bind=engine), autoload=True)
    #         print(select([func.count('*')], from_obj=many_rows).scalar())
    # finally:
    #     engine.dispose()

    # conn = engine.connect()
    # try:
    #     with conn.cursor() as cursor:
    #         cursor.execute("SELECT * FROM bom_prod_2_output LIMIT 20")
    #         print(cursor.description)
    #         print(cursor.fetchall())
    # finally:
    #     conn.close()
    # conn = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
    #                region_name='us-west-2')
    # try:
    #     with conn.cursor() as cursor:
    #         cursor.execute("""
    #         SELECT * FROM one_row
    #         """)
    #         print(cursor.description)
    #         print(cursor.fetchall())
    # finally:
    #     conn.close()
    return 'Hello World!'


@app.route('/test')
def hello_world1():
    client = boto3.client(
        'athena',
        aws_access_key_id='AKIAIEZK2WKUI276JE2A',
        aws_secret_access_key='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'
    )
    query_string = "SELECT * FROM bom_prod_2_output LIMIT 20"
    query_id = client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={
            'Database': 'solar_radiation'
        },
        ResultConfiguration={
            'OutputLocation': 's3://solar-radiation/solar_test'
        }
    )['QueryExecutionId']
    query_status = None
    while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        query_status = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
        if query_status == 'FAILED' or query_status == 'CANCELLED':
            raise Exception('Athena query with the string "{}" failed or was cancelled'.format(query_string))
    results_paginator = client.get_paginator('get_query_results')
    results_iter = results_paginator.paginate(
        QueryExecutionId=query_id,
        PaginationConfig={
            'PageSize': 1000
        }
    )
    results = []
    data_list = []
    for results_page in results_iter:
        for row in results_page['ResultSet']['Rows']:
            data_list.append(row['Data'])
    for datum in data_list[1:]:
        results.append([x['VarCharValue'] for x in datum])
    return results


@app.route('/')
def index():
    posts = [{'a': 'aaaa'}, {'a': 'bb'}]
    return render_template('map.html', title='map', posts=posts)


@app.route('/location', methods=['GET', 'POST'])
def getLocation():
    data_json = json.loads(request.form.get('place'))
    lat = data_json['geometry']['location']['lat']
    lng = data_json['geometry']['location']['lng']
    print(lat)
    print(lng)

    client = boto3.client(
        'athena',
        aws_access_key_id='AKIAIEZK2WKUI276JE2A',
        aws_secret_access_key='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'
    )
    query_string = """
     SELECT bom_prod_2_output.*,
        (ACOS( SIN(RADIANS(""" + str(lat) + """)) * SIN(RADIANS(bom_prod_2_output.latitude)) + COS(RADIANS(""" + str(
        lat) + """)) * COS(RADIANS(bom_prod_2_output.latitude)) *COS(RADIANS(bom_prod_2_output.longitude) - RADIANS(""" + str(
        lng) + """)) ) * 6384.0999) AS distance
FROM bom_prod_2_output
WHERE (((ACOS( SIN(RADIANS(""" + str(lat) + """)) * SIN(RADIANS(bom_prod_2_output.latitude)) + COS(RADIANS(""" + str(
        lat) + """)) * COS(RADIANS(bom_prod_2_output.latitude)) * COS(RADIANS(bom_prod_2_output.longitude) - RADIANS(""" + str(
        lng) + """)) ) * 6384.0999) <= 2 )
    OR (bom_prod_2_output.latitude = """ + str(lat) + """
    AND bom_prod_2_output.longitude = """ + str(lng) + """))
    AND cast(year as integer) >= 2018
    AND radiationtype='dni'
    AND cast(month as integer) >= 1
    """
    query_id = client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={
            'Database': 'solar_radiation'
        },
        ResultConfiguration={
            'OutputLocation': 's3://solar-radiation/solar_test'
        }
    )['QueryExecutionId']
    query_status = None
    while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        query_status = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
        if query_status == 'FAILED' or query_status == 'CANCELLED':
            raise Exception('Athena query with the string "{}" failed or was cancelled'.format(query_string))
    results_paginator = client.get_paginator('get_query_results')
    results_iter = results_paginator.paginate(
        QueryExecutionId=query_id,
        PaginationConfig={
            'PageSize': 1000
        }
    )
    results = []
    data_list = []
    for results_page in results_iter:
        for row in results_page['ResultSet']['Rows']:
            data_list.append(row['Data'])
    for datum in data_list[1:]:
        results.append([x['VarCharValue'] for x in datum])
    # results = [['2018-02-01 00:00', 'dni', '-37.62558510000013', '144.77199200000015', '0', '2018', '02', '01', '00',
    #             '8.374844870441626'],
    #            ['2018-02-01 00:00', 'dni', '-37.62558510000013', '144.82198740000015', '0', '2018', '02', '01', '00',
    #             '5.4593579043583995'],
    #            ['2018-02-01 00:00', 'dni', '-37.62558510000013', '144.87198280000015', '0', '2018', '02', '01', '00',
    #             '5.32734503047231'],
    #            ['2018-02-01 00:00', 'dni', '-37.62558510000013', '144.92197820000015', '0', '2018', '02', '01', '00',
    #             '8.115791770167663'],
    #            ['2018-02-01 00:00', 'dni', '-37.67558050000013', '144.77199200000015', '0', '2018', '02', '01', '00',
    #             '6.806473764646096'],
    #            ['2018-02-01 00:00', 'dni', '-37.67558050000013', '144.82198740000015', '0', '2018', '02', '01', '00',
    #             '2.4539345598004814'],
    #            ['2018-02-01 00:00', 'dni', '-37.67558050000013', '144.87198280000015', '0', '2018', '02', '01', '00',
    #             '2.1444742080373578'],
    #            ['2018-02-01 00:00', 'dni', '-37.67558050000013', '144.92197820000015', '0', '2018', '02', '01', '00',
    #             '6.485291814745095'],
    #            ['2018-02-01 00:00', 'dni', '-37.72557590000013', '144.82198740000015', '0', '2018', '02', '01', '00',
    #             '6.656097100143916'],
    #            ['2018-02-01 00:00', 'dni', '-37.72557590000013', '144.87198280000015', '0', '2018', '02', '01', '00',
    #             '6.548401529140875'],
    #            ['2018-02-01 00:00', 'dni', '-37.72557590000013', '144.92197820000015', '0', '2018', '02', '01', '00',
    #             '8.961950299555237'],
    #            ['2018-02-01 01:00', 'dni', '-37.62558510000013', '144.77199200000015', '0', '2018', '02', '01', '01',
    #             '8.374844870441626'],
    #            ['2018-02-01 01:00', 'dni', '-37.62558510000013', '144.82198740000015', '0', '2018', '02', '01', '01',
    #             '5.4593579043583995'],
    #            ['2018-02-01 01:00', 'dni', '-37.62558510000013', '144.87198280000015', '0', '2018', '02', '01', '01',
    #             '5.32734503047231'],
    #            ['2018-02-01 01:00', 'dni', '-37.62558510000013', '144.92197820000015', '0', '2018', '02', '01', '01',
    #             '8.115791770167663'],
    #            ['2018-02-01 01:00', 'dni', '-37.67558050000013', '144.77199200000015', '0', '2018', '02', '01', '01',
    #             '6.806473764646096'],
    #            ['2018-02-01 01:00', 'dni', '-37.67558050000013', '144.82198740000015', '0', '2018', '02', '01', '01',
    #             '2.4539345598004814'],
    #            ['2018-02-01 01:00', 'dni', '-37.67558050000013', '144.87198280000015', '0', '2018', '02', '01', '01',
    #             '2.1444742080373578'],
    #            ['2018-02-01 01:00', 'dni', '-37.67558050000013', '144.92197820000015', '0', '2018', '02', '01', '01',
    #             '6.485291814745095'],
    #            ['2018-02-01 01:00', 'dni', '-37.72557590000013', '144.82198740000015', '0', '2018', '02', '01', '01',
    #             '6.656097100143916'],
    #            ['2018-02-01 01:00', 'dni', '-37.72557590000013', '144.87198280000015', '0', '2018', '02', '01', '01',
    #             '6.548401529140875'],
    #            ['2018-02-01 01:00', 'dni', '-37.72557590000013', '144.92197820000015', '0', '2018', '02', '01', '01',
    #             '8.961950299555237']]

    response_data = {'results': results, 'processed': process(results)}
    return json.dumps(response_data)


if __name__ == '__main__':
    app.run()
