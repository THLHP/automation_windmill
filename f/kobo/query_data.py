import wmill
import requests
import psycopg2
import json
from psycopg2.extras import execute_values


def main(
    name: str,
    endpoint: str
):

    kobo_token = json.loads(wmill.get_variable("f/kobo/kobo_token"))
    settings = json.loads(wmill.get_variable("f/kobo/vultr_db"))

    headers = {"Authorization": "Token " + kobo_token['token']}

    conn = psycopg2.connect(
        host=settings['db_settings']['host'],
        database=settings['db_settings']['dbname'],
        user=settings['db_settings']['username'],
        password=settings['db_settings']['password'],
        port=settings['db_settings']['port']
    )

    cursor = conn.cursor()

    response = requests.get(endpoint, headers=headers)

    results = ""

    if response.status_code == 200:
        data = response.json()
        results = data['results']

    if results:
        rows = [(row['_uuid'], json.dumps(row), name) for row in results]
        query = """
            INSERT INTO kobo.submissions (uuid, content, form_name) 
            VALUES %s 
            ON CONFLICT (uuid) DO NOTHING
        """
        execute_values(cursor, query, rows)
        conn.commit()

        return (f'Added {cursor.rowcount} rows to {name}')
    else: 
        return f'Incorrect API response for {name}'
