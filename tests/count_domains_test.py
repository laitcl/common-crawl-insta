import pytest
from src import count_domains
import getpass
import psycopg2
import os
from pathlib import Path
import datetime
from dateutil.tz import tzutc
import dateutil.parser

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
test_archive = str(TEST_DIR) + '/assets/example.warc.gz'
test_db_string="dbname=localhost user={user} dbname=cc_insta_test".format(user = getpass.getuser())
test_counter = count_domains.etl_worker(test_archive, test_db_string)

conn=psycopg2.connect(test_db_string)
cur=conn.cursor()

def cleanup_active_db(conn, cur):
    cur.execute("""drop table instagram_links, reference_links, address_linked_by, last_run_time;""")
    conn.commit()

def test_create_database():
    test_counter.create_tables()
    test_counter.conn.commit()
    cur.execute("""
        select 
            table_name, 
            column_name, 
            data_type 
        from information_schema.columns where 
        table_schema = 'public';
        """)
    result = cur.fetchall()
    tables = [
        ('address_linked_by', 'reference_link_id', 'integer'), 
        ('address_linked_by', 'id', 'integer'), 
        ('instagram_links', 'instagram_link', 'text'), 
        ('address_linked_by', 'instagram_link', 'text'), 
        ('address_linked_by', 'updated_at', 'timestamp with time zone'), 
        ('last_run_time', 'time', 'timestamp with time zone'), 
        ('reference_links', 'id', 'integer'), 
        ('instagram_links', 'updated_at', 'timestamp with time zone'), 
        ('reference_links', 'updated_at', 'timestamp with time zone'), 
        ('reference_links', 'created_at', 'timestamp with time zone'), 
        ('address_linked_by', 'created_at', 'timestamp with time zone'), 
        ('address_linked_by', 'reference_link', 'text'), 
        ('reference_links', 'reference_link', 'text'), 
        ('instagram_links', 'id', 'integer'), 
        ('reference_links', 'warc_date', 'timestamp with time zone'), 
        ('instagram_links', 'created_at', 'timestamp with time zone'), 
        ('address_linked_by', 'instagram_link_id', 'integer'), 
        ('instagram_links', 'linked_count', 'integer')
        ]
    for table in tables:
        assert table in result
    cleanup_active_db(conn, cur)

def test_read_warc_archive():
    test_counter.read_warc_archive(test_archive)
    assert test_counter.data==[
        {'instagram_link': 'https://www.instagram.com/lai.tcl/', 
        'reference_link': 'https://laitcl.github.io/laitcl-Website/', 
        'warc_date': datetime.datetime(2020, 12, 5, 7, 13, 29, tzinfo=tzutc())}, 
        {'instagram_link': 'https://www.instagram.com/mcdonalds/', 
        'reference_link': 'https://www.mcdonalds.com/us/en-us.html', 
        'warc_date': datetime.datetime(2020, 12, 5, 7, 13, 30, tzinfo=tzutc())}
        ]


@pytest.mark.parametrize("data, expected",
    [
        (
            [{'instagram_link': "https://www.instagram.com/test_insert", 
            "reference_link": "https://www.test_insert.com/", 
            "warc_date": datetime.datetime.now(datetime.timezone.utc),
            }],
            ["https://www.instagram.com/test_insert", "https://www.test_insert.com/", datetime.date.today()]
        ),
        (
            [{'instagram_link': "https://www.instagram.com/test_insert", 
            "reference_link": "https://www.test_insert.com/", 
            "warc_date": datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1),},
            {'instagram_link': "https://www.instagram.com/test_insert", 
            "reference_link": "https://www.test_insert.com/", 
            "warc_date": datetime.datetime.now(datetime.timezone.utc),
            }],
            ["https://www.instagram.com/test_insert", "https://www.test_insert.com/", datetime.date.today()]
        ),
        (
            [{'instagram_link': "https://www.instagram.com/test_insert", 
            "reference_link": "https://www.test_insert.com/", 
            "warc_date": datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1),},
            {'instagram_link': "https://www.instagram.com/test_insert", 
            "reference_link": "https://www.test_insert.com/", 
            "warc_date": datetime.datetime.now(datetime.timezone.utc),
            }],
            ["https://www.instagram.com/test_insert", "https://www.test_insert.com/", datetime.date.today()]
        ),
    ])
def test_insert_data_to_db(data, expected):
    test_counter.create_tables()
    test_counter.conn.commit()
    test_counter.data = data
    test_counter.insert_data_to_db()
    test_counter.conn.commit()

    cur.execute("""
        select instagram_link, linked_count from instagram_links
        """)
    result=cur.fetchall()
    assert result == [("{0}".format(expected[0]), 1)]

    cur.execute("""
        select reference_link, warc_date::date from reference_links
        """)
    result=cur.fetchall()
    assert result == [("{0}".format(expected[1]), expected[2])]

    cur.execute("""
        select instagram_link, reference_link from address_linked_by
        """)
    result=cur.fetchall()
    assert result == [("{0}".format(expected[0]), "{0}".format(expected[1]))]

    # import code; code.interact(local=dict(globals(), **locals()))
    cleanup_active_db(conn, cur)