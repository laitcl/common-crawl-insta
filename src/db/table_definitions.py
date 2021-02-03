import datetime

table_definitions = {
    "instagram_links" : {
        "create_table" : """
            CREATE TABLE IF NOT EXISTS instagram_links
                (
                id              serial PRIMARY KEY,
                instagram_link  text UNIQUE,
                linked_count    int DEFAULT 0,
                created_at      timestamptz,
                updated_at      timestamptz
                );
            """,
        "create_index" : ["CREATE UNIQUE INDEX IF NOT EXISTS instagram_link_idx ON instagram_links (instagram_link);"],
        "count_links" : """
            with 
            last_run as(
            SELECT time FROM last_run_time
            ),
            recent_references as(
            SELECT instagram_link_id, reference_link_id
            FROM address_linked_by alb
            CROSS JOIN last_run_time lrt
            WHERE alb.updated_at > lrt.time
            ),
            counts as (
            SELECT instagram_link_id, count(reference_link_id) as reference_count
            FROM recent_references
            GROUP BY instagram_link_id
            ),
            summed as(
            SELECT 
                il.id as id, 
                il.linked_count + c.reference_count as total_count
            FROM instagram_links il
            JOIN counts c on il.id = c.instagram_link_id
            )
            UPDATE instagram_links il2
            SET linked_count = total_count
            FROM summed
            WHERE summed.id = il2.id
            """,
    },
    "reference_links" : {
        "create_table" : """
            CREATE TABLE IF NOT EXISTS reference_links
            (
            id              serial PRIMARY KEY,
            reference_link  text UNIQUE,
            created_at      timestamptz,
            updated_at      timestamptz,
            warc_date       timestamptz
            );
            """,
        "create_index" : ["CREATE UNIQUE INDEX IF NOT EXISTS reference_link_idx ON reference_links (reference_link);",
                          "CREATE INDEX IF NOT EXISTS warc_date_idx ON reference_links (warc_date);"],
        "select_warc_date" : "SELECT warc_date FROM reference_links WHERE reference_link = $${0}$$",
    },
    "address_linked_by" : {
        "create_table" : """
            CREATE TABLE IF NOT EXISTS address_linked_by 
            (
            id                  serial PRIMARY KEY,
            instagram_link      text,
            reference_link      text,
            created_at          timestamptz,
            updated_at          timestamptz
            );
            """,
        "create_index" : ["CREATE INDEX IF NOT EXISTS alb_instagram_link_idx ON address_linked_by (instagram_link);",
                          "CREATE INDEX IF NOT EXISTS alb_reference_link_idx ON address_linked_by (reference_link);"],
        "remove_old_linked_by": """
            DELETE FROM address_linked_by
            WHERE reference_link = $${0}$$
            """,
        "insert_linked_by" : """
            INSERT INTO address_linked_by
            (instagram_link, reference_link, created_at, updated_at)
            VALUES
            (
                $${0}$$,
                $${1}$$,
                '{2}',
                '{3}'
            )
            """,
    },
    "last_run_time" : {
        "create_table" : """
            CREATE TABLE IF NOT EXISTS last_run_time 
            (
            time       timestamptz DEFAULT CURRENT_TIMESTAMP(0)
            );
            """,
        "create_index" : ["CREATE INDEX IF NOT EXISTS instagram_link_idx on address_linked_by (instagram_link, instagram_link_id)",
                          "CREATE INDEX IF NOT EXISTS instagram_link_idx on address_linked_by (reference_link, reference_link_id)"],
        "seed" : "INSERT INTO last_run_time (time) SELECT '{time}' WHERE NOT EXISTS (SELECT time from last_run_time)".format(time = datetime.datetime.now()),
        "update_last_run_time" : """
            UPDATE last_run_time
            SET time = '{0}'
            """
    },
}
