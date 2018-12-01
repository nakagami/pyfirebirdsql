
timezone_name_by_id = {}

def load_timezone_ids(connection):
    global id_to_name
    cur = connection.cursor()
    cur.execute("select count(*) from rdb$relations where rdb$relation_name='RDB$TIME_ZONES' and rdb$system_flag=1")
    if cur.fetchone()[0]:
        cur.execute("select rdb$time_zone_id, rdb$time_zone_name from rdb$time_zones")
        for r in cur.fetchall():
            timezone_name_by_id[r[0]] = r[1]
    cur.close()


def get_tzinfo(timezone_id):
    import pytz
    return pytz.timezone(timezone_name_by_id[timezone_id])
