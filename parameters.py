from google.cloud import bigquery

params = {
    'hired_employees': {
        'pandas_read_csv_options': {
            'sep':',',
            'names': ['id','name','datetime','department_id','job_id'],
            'dtype': {
                'id': 'Int64',
                'name':'object',
                'datetime': 'object',
                'department_id':'Int64',
                'job_id':'Int64'
            },
            'parse_dates': ['datetime']
        },
        'folder_prefix' : 'hired_employees/pending/',
        'processed_folder':'hired_employees/processed/',
        'table_name': 'etl_test.hired_employees',
        'schema': [
            bigquery.SchemaField('id',"INTEGER",mode='NULLABLE'),
            bigquery.SchemaField('name',"STRING",mode='NULLABLE'),
            bigquery.SchemaField('datetime',"DATETIME",mode='NULLABLE'),
            bigquery.SchemaField('department_id',"INTEGER",mode='NULLABLE'),
            bigquery.SchemaField('job_id',"INTEGER",mode='NULLABLE')
        ]
    },
    'departments': {
        'pandas_read_csv_options': {
            'sep':',',
            'names': ['id','department'],
            'dtype': {
                'id': 'Int64',
                'department':'object',
            }
        },
        'folder_prefix' : 'departments/pending/',
        'processed_folder':'departments/processed/',
        'table_name': 'etl_test.departments',
        'schema': [
            bigquery.SchemaField('id',"INTEGER",mode='NULLABLE'),
            bigquery.SchemaField('department',"STRING",mode='NULLABLE')
        ]
    },
    'jobs': {
        'pandas_read_csv_options': {
            'sep':',',
            'names': ['id','job'],
            'dtype': {
                'id': 'Int64',
                'job':'object',
            }
        },
        'folder_prefix' : 'jobs/pending/',
        'processed_folder':'jobs/processed/',
        'table_name': 'etl_test.jobs',
        'schema': [
            bigquery.SchemaField('id',"INTEGER",mode='NULLABLE'),
            bigquery.SchemaField('job',"STRING",mode='NULLABLE')
        ]
    },   
}