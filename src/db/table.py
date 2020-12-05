from . import table_definitions
table_definitions = table_definitions.table_definitions

class table:
    def __init__(self, table_name, db_cursor):
        self.table_name = (table_name)
        self.cur=db_cursor
        self.create_table()
        self.create_index()

    def create_table(self):
        self.cur.execute(table_definitions[self.table_name]["create_table"])
        if "seed" in table_definitions[self.table_name].keys():
            self.cur.execute(table_definitions[self.table_name]["seed"])

    def create_index(self):
        for index_sql in table_definitions[self.table_name]["create_index"]:
            self.cur.execute(index_sql)

    def simple_insert(self, data_hash, *args):
        # Supports inserting one row of strings/datetime objects to the table
        # args are options after the insert statement, such as "on conflict do nothing". These need to be input in the correct order
        columns = data_hash.keys()
        values = ""

        for column in columns:
            data = data_hash[column]
            if isinstance(data, str):
                values+= ("$$" + data + "$$,")
            else:
                values+= "'{data}',".format(data = data)
        values = values[:-1]

        insert_sql = """
        INSERT INTO {table_name}
        ({columns})
        VALUES
        ({values})
        """.format(
            table_name = self.table_name,
            columns = ", ".join(data_hash.keys()),
            values = values,
            )

        for arg in args:
            insert_sql += arg
        self.cur.execute(insert_sql)

    def custom_action(self, query_name, *args):
        # Anything that is not a simple insert, including more complicated inserts, delete, aggregates, etc.
        # Supply query_name, define query_name in table definitions, and have the function run itself
        # In case multiple args need to be passed, they must be passed in the correct order

        sql = table_definitions[self.table_name][query_name].format(*args)
        self.cur.execute(sql)