import sqlite3
import numpy
import csv
import json
import sql


def query_clan_top(database):
    clan_detail_dict = {}
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    COLstr = "clan_id,clan_name,current_period_ranking"
    sql = """SELECT %s FROM `clan`""" % COLstr
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
    sql = """SELECT total_power FROM `members` WHERE join_clan_id = %s""" % clan_id
    try:
        cursor.execute(sql)
        for user in cursor:
            clan_power.append(user[0])
        return int(numpy.mean(clan_power))
    except Exception:
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
    list1 = sorted(clan_dict1.items(), key=lambda x: x[0], reverse=True)
    # clan_dict1= sorted(clan_dict.values()[3])
    with open('clan4.csv', 'w', encoding='gbk', newline="") as fp:
        for clan in list1:
            write = csv.writer(fp)
            write.writerow(clan[1])


def get_avatar():
    avatar_dict = {}
    total = 0
    db = sql.get_sql_members('total.db', 2)
    for member in db.keys():
        # if db[member][5] < 51 and db[member][7] < 51:
        if True:
            total += 1
            avatar = db[member][10]
            if avatar in avatar_dict.keys():
                avatar_dict[avatar] += 1
            else:
                avatar_dict[avatar] = 1
    with open('id.json', encoding='utf-8') as fp:
        id_dict = json.load(fp)
    for id in id_dict:
        if int(id) not in avatar_dict.keys():
            print(id_dict[id])
    avatar_dict_trans = {}
    # a = sorted(avatar_dict.items(), key=lambda x: x[0], reverse=True)
    for avatar in sorted(avatar_dict.items(), key=lambda x: x[1], reverse=True):
        avatar_dict_trans[id_dict[str(avatar[0])]] = '{:.2%}'.format(avatar_dict[avatar[0]]/total)
    with open('fmz.json', 'w', encoding='utf-8') as fp:
        json.dump(avatar_dict_trans, fp, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    query_avg_power('pcr_qd2206.db')
    # get_avatar()
