import sqlite3
import numpy
import csv


def query_clan_top(database):
    clan_detail_dict = {}
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    COLstr = "clan_id,clan_name,current_period_ranking"
    sql = """SELECT %s FROM `clan_detail`""" % COLstr
    try:
        cursor.execute(sql)
        for clan in cursor:
            if clan[2] < 151 and clan[2] > 0:
                clan_detail_dict[clan[0]] = clan
        return clan_detail_dict
    except Exception as e:
        print('Error', e)
        conn.rollback()
    conn.close()


def get_clan_power(database, clan_id):
    clan_power = []
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    # COLstr = "viewer_id,name,level,role,total_power,join_clan_id,join_clan_name,last_login_time,join_clan_history,name_history,last_refresh_time"
    sql = """SELECT total_power FROM `clan_members` WHERE join_clan_id = %s""" % clan_id
    try:
        cursor.execute(sql)
        for user in cursor:
            clan_power.append(user[0])
        return int(numpy.mean(clan_power))
    except Exception as e:
        conn.rollback()
    conn.close()


def query_avg_power(database):
    clan_dict = query_clan_top(database)
    clan_dict1 = {}
    for clan_id in clan_dict.keys():
        mean_power = get_clan_power(database, clan_id)
        # clan_dict[clan_id] = clan_dict[clan_id] + tuple([mean_power])
        clan_dict1[mean_power] = clan_dict[clan_id] + tuple([mean_power])
        # print(mean_power)
    list1= sorted(clan_dict1.items(), key=lambda x:x[0], reverse=True)
    # clan_dict1= sorted(clan_dict.values()[3])
    with open ('clan4.csv', 'w', encoding='gbk', newline="") as fp:
        for clan in list1:
            write = csv.writer(fp)
            write.writerow(clan[1])



if __name__ == "__main__":
    query_avg_power('pcr_qd2204.db')
