import json
import datetime
from . import table_definitions
import psycopg2

class table:
    def __init__(self, table_name):
        self.table_name = (table_name)
        self.create_table(self.table_name)
        self.create_indexes(self.table_name)

    def create_table():
        cur.execute(table_definitions[table_name]["create_table"])
        if table_definitions[table_name]["seed"]:
            cur.execute(table_definitions[table_name]["seed"])

    def create_index():
        for index_sql in table_definitions[table_name]["create_index"]:
            cur.execute(index_sql)

    def simple_insert(data_hash, *args):
        # Supports inserting one row of strings/datetime objects to the table
        # args are options after the insert statement, such as "on conflict do nothing". These need to be input in the correct order
        columns = data_hash.keys()
        values = "("

        for column in columns:
            data = data_hash[column]
            if isinstance(data, str):
                values+= ("$$" + data + "$$,")
            else:
                values+= "'{data},'".format(data = data)
        values = values[:-1] + ")"

        insert_sql = """
        INSERT INTO {table_name}
        ({columns})
        VALUES
        ({values})
        """.format(
            table_name = self.table_name
            columns = ", ".join(data_hash.keys())
            values = values
            )

        for arg in args:
        insert_sql += arg

        cur.execute(insert_sql)

    def custom_action(query_name, *args):
        # Anything that is not a simple insert, including more complicated inserts, delete, aggregates, etc.
        # Supply query_name, define query_name in table definitions, and have the function run itself
        # In case multiple args need to be passed, they must be passed in the correct order

        sql = table_definitions[self.table_name][query_name].format(arg for arg in args)
        cur.execute(query)