try_load_timezone_ids = False
timezone_name_by_id = {}
timezone_id_by_name = {}

def load_timezone_ids(connection):
    global try_load_timezone_ids
    if try_load_timezone_ids:
        return
    try_load_timezone_ids = True
    cur = connection.cursor()
    cur.execute("select count(*) from rdb$relations where rdb$relation_name='RDB$TIME_ZONES' and rdb$system_flag=1")
    if cur.fetchone()[0]:
        cur.execute("select rdb$time_zone_id, rdb$time_zone_name from rdb$time_zones")
        for r in cur.fetchall():
            timezone_name_by_id[r[0]] = r[1]
            timezone_id_by_name[r[1]] = r[0]

    cur.close()


def get_tzinfo(timezone_id):
    import pytz
    return pytz.timezone(timezone_name_by_id[timezone_id])


def get_timezone_id(tz_name):
    return timezone_id_by_name[tz_name]
