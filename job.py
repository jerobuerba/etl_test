from flask import Flask, request, jsonify
import pandas as pd
#from io import StringIO
from base_google import GoogleJobs
from parameters import params
from tempfile import NamedTemporaryFile

app = Flask(__name__)

goog = GoogleJobs()

def read_file(file_name):
    blob = goog.retrieve_gcs_file(f"{params[file_name]['folder_prefix']}{file_name}.csv")
    with NamedTemporaryFile() as temp_file:
        blob.download_to_filename(temp_file.name)
        df = pd.read_csv(temp_file, **params[file_name]['pandas_read_csv_options'])
        temp_file.close()
    return df


@app.route('/upload', methods=['POST'])
def upload_file():
    file_name = request.form.get('file_name')
    if file_name is None:
        return jsonify({'error': 'File name not sent'}), 400
    
    df = read_file(file_name)

    num_batches = df.shape[0]//1000 +1
    for i in range(num_batches):
        i_s = 1000 * i
        i_f = min(1000*i + 999, df.shape[0])
        goog.write_data_to_bq(df.loc[i_s:i_f], params[file_name]['schema'],destination_table=params[file_name]['table_name'])

        print(f'Batch {i+1} uploaded!')

    goog.move_gcs_file(f"{params[file_name]['folder_prefix']}{file_name}.csv",params[file_name]['processed_folder'])
    return jsonify({'message': 'File uploaded successfully and moved to processed folder.'}), 201

@app.route('/quarterly_report', methods=['GET'])
def quarterly_report():
    year = request.form.get('year')
    if year is None:
        return jsonify({'error': 'File name not sent'}), 400
    query = f"""
        select department, job, coalesce(Q1,0) as Q1,coalesce(Q2,0) as Q2,coalesce(Q3,0) as Q3,coalesce(Q4,0) as Q4
        from (
        select department, job,'Q'||cast(extract(quarter from datetime) as STRING) as quarter,count(*) as cant
        from jero-test.etl_test.hired_employees h
        left join jero-test.etl_test.departments d
            on h.department_id = d.id
        left join jero-test.etl_test.jobs j
            on h.job_id = j.id
        where extract(year from datetime) = {year}
        group by 1,2,3)
        PIVOT(sum(cant) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
        order by department, job
    """
    result = goog.read_data_from_bq(query)
    return jsonify(result), 200

@app.route('/departments_report', methods=['GET'])
def departments_report():
    year = request.form.get('year')
    if year is None:
        return jsonify({'error': 'File name not sent'}), 400
    query = f"""        
        with departments as
        ( select department_id as id, department,count(*) as hired
        from jero-test.etl_test.hired_employees h
        left join jero-test.etl_test.departments d
            on h.department_id = d.id
        left join jero-test.etl_test.jobs j
            on h.job_id = j.id
        where extract(year from datetime) = {year}
        group by 1,2
        )
        select *
        from departments
        where hired > (select avg(hired) from departments)
        order by hired desc
    """
    result = goog.read_data_from_bq(query)
    return jsonify(result), 200


if __name__ == '__main__':
    app.run(debug=True)