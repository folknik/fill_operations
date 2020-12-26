import os
import json
import psycopg2
import numpy as np
from statistics import median
from datetime import datetime, timedelta
from clickhouse_driver import connect
from add_functions import distance_between, calculate_angle, most_common


execution_date = os.environ['execution_date']
update_status = os.environ['update_status']
MAX_HEIGHT_FOR_OPERATION_HEADING = int(os.environ['max_height_for_operation_heading'])
MAX_HEIGHT_FOR_OPERATION_TYPE = int(os.environ['max_height_for_operation_type'])
MAX_DISTANCE_FROM_AIRPORT = int(os.environ['max_distance_from_airport'])
GAP = int(os.environ['gap'])


def get_aircraft_geos(params, id):
    ch_conn = connect('clickhouse://{}:{}@{}:9000/{}'.format(params["clickhouse_user"],
                                                             params["clickhouse_pw"],
                                                             params["clickhouse_host"],
                                                             params["clickhouse_db"])
                      )
    ch_cursor = ch_conn.cursor()

    ch_cursor.execute(""" SELECT time_stamp, longitude_aircraft, latitude_aircraft,
                                altitude_aircraft, serial_number_equip 
                          FROM eco_monitoring.adsb_raw_data 
                          WHERE id_track = '{}' """.format(id))
    arr = ch_cursor.fetchall()
    if len(arr) > 0:
        arr = np.array(arr)
        return arr[arr[:, 0].argsort()][::-1]
    return arr


def order_check(arr):
    status = [1 if arr[i] > arr[i+1] else -1 for i in range(len(arr)-1)]
    S = sum(status)
    if S > 0:
        return 'takeoff'
    return 'landing'


def get_operation_type(arr):
    """
    arr include columns - [time_stamp, longitude_aircraft, latitude_aircraft, altitude_aircraft]
    """
    arr = arr[arr[:, 3] <= MAX_HEIGHT_FOR_OPERATION_TYPE]
    altitudes = arr[:, 3]
    operation_type = order_check(altitudes)
    return operation_type


def find_true_heading(tracks, calculated_value):
    for track in tracks:
        if track - GAP / 2 <= calculated_value <= track + GAP / 2:
            return track
    return calculated_value


def get_operatioin_heading(sn_, conn, arr):
    """
    arr include columns - [time_stamp, longitude_aircraft, latitude_aircraft, altitude_aircraft]
    """
    if len(arr) > 1:
        cursor = conn.cursor()
        query = """ SELECT E.serial_number_equip, A.heading
                    FROM public.equipment AS E INNER JOIN public.airports_true_heading AS A ON E.id_airport = A.id_airport
                    WHERE E.serial_number_equip = '{}'""".format(sn_)
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        records = [v[1] for v in records]
        arr = [line for line in arr if line[3] * 0.3048 < MAX_HEIGHT_FOR_OPERATION_HEADING]

        angles = [calculate_angle(arr[i][2], arr[i][1], arr[i - 1][2], arr[i - 1][1]) for i in range(1, len(arr))]
        angles = [a for a in angles if a != 0]
        if len(angles) > 0:
            calculated_angle = int(round(median(angles)))
            return find_true_heading(records, calculated_angle)
        return 0
    return 0


def get_airport_destination(conn, arr):
    """
    arr include columns - [time_stamp, longitude_aircraft, latitude_aircraft, altitude_aircraft]
    """
    cursor = conn.cursor()
    query = """ SELECT longitude_center, latitude_center, name FROM public.airports"""
    cursor.execute(query)
    airports_geo = cursor.fetchall()
    cursor.close()

    airports_string = []
    for geo in arr:
        for airport in airports_geo:
            dist = distance_between(geo[2], geo[1], airport[1], airport[0])
            if dist <= MAX_DISTANCE_FROM_AIRPORT:
                airport_name = airport[2]
                airports_string.append(airport_name)
                break

    if len(airports_string) > 0:
        return most_common(airports_string)
    return ''


def main(d):

    with open("./credentials.json", "r+") as credJson:
        refs = json.loads(credJson.read())

    ps_conn = psycopg2.connect(dbname=refs["postgres_db"],
                               user=refs["postgres_user"],
                               password=refs["postgres_pw"],
                               host=refs["postgres_host"])
    ps_cursor = ps_conn.cursor()

    d = datetime.strptime(d, '%Y-%m-%d')
    start_date = (d - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    stop_date = d.strftime("%Y-%m-%d %H:%M:%S")

    print(f"EXECUTION PERIOD {start_date} AND {stop_date}")

    query = """ SELECT uid, id_track, first_time_data, last_time_data, operation_type,
                        operation_heading, from_airport, to_airport 
                FROM public.aircraft_tracks
                WHERE last_time_data between '{}' and '{}' AND operation_heading = 0
                ORDER BY last_time_data DESC """.format(start_date, stop_date)
    ps_cursor.execute(query)
    arr = ps_cursor.fetchall()

    for line in arr:
        uid, id_track, start, stop, _, _, _, _ = line
        start = start.strftime("%Y-%m-%d %H:%M:%S")
        stop = stop.strftime("%Y-%m-%d %H:%M:%S")
        aircraft_geos = get_aircraft_geos(params=refs, id=id_track)
        sn = aircraft_geos[0][4]
        operation_type = get_operation_type(aircraft_geos)
        operation_heading = get_operatioin_heading(sn, ps_conn, aircraft_geos)
        airport_destination = get_airport_destination(ps_conn, aircraft_geos)
        if operation_type == 'takeoff':
            from_airport, to_airport = airport_destination, ''
        else:
            from_airport, to_airport = '', airport_destination

        print("uid: {}, id_track: {}, first_time_data: {}, last_time_data: {}, operation_type: {}, "
              "operation_heading: {}, from_airport: {}, to_airport: {}".format(uid, id_track, start, stop,
                                                                               operation_type, operation_heading,
                                                                               from_airport, to_airport))
        if update_status == 'true':
            postgres_update_query = """ UPDATE public.aircraft_tracks 
                                        SET operation_type = %s, operation_heading = %s,
                                            from_airport = %s, to_airport = %s
                                        WHERE uid = %s"""
            ps_cursor.execute(postgres_update_query, (operation_type, operation_heading,
                                                      from_airport, to_airport, uid))

    ps_cursor.close()
    ps_conn.close()


if __name__ == '__main__':
    main(execution_date)