import sqlite3
import time
import re
import numpy


def validate(name):
    rstr = r"[\/\\\:\*\?\'\"\<\>\|\-\+\%]"
    new_name = re.sub(rstr, "''", name)
    return new_name


def sql_create(db_name, db_type_list):
    if isinstance(db_type_list, str):
        db_type_list = [db_type_list]
    for db_type in db_type_list:
        try:
            if db_type == 'clan':
                create_sql_clan(db_name)
            elif db_type == 'members':
                create_sql_members(db_name)
            elif db_type == 'status':
                create_sql_status(db_name)
            elif db_type == 'members_arena':
                create_sql_members_arena(db_name)
            elif db_type == 'grand':
                create_sql_grand(db_name)
        except sqlite3.OperationalError:
            pass


def sql_insert(db_name, db_type, db_data):
    if db_type == 'clan':
        insert_new_clan_detail(db_name, db_data)
    elif db_type == 'arena':
        insert_members_arena(db_name, db_data)
    elif db_type == 'status':
        insert_status_detail(db_name, db_data)


def sql_get(db_name, db_type):
    if db_type == 'status':
        return get_clan_status(db_name)
    elif db_type == 'rank':
        return get_clan_rank(db_name)
    elif db_type == 'members':
        return get_sql_members(db_name)
    elif db_type == 'arena':
        return get_sql_members_arena(db_name)

# 创建 TABLE clan
def create_sql_clan(database):
    conn = sqlite3.connect(database)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = conn.cursor()
    # 使用 execute() 方法执行 SQL，如果表存在则删除
    # cursor.execute("DROP TABLE IF EXISTS `clan`")
    # 使用预处理语句创建表
    sql = """CREATE TABLE `clan` (
            "clan_id"	INTEGER NOT NULL PRIMARY KEY,
            "clan_name"	TEXT NOT NULL,
            "leader_viewer_id"	INTEGER NOT NULL,
            "leader_name"	TEXT NOT NULL,
            "join_condition"	INTEGER NOT NULL,
            "activity"	INTEGER NOT NULL,
            "clan_battle_mode"	INTEGER NOT NULL,
            "member_num"	INTEGER NOT NULL,
            "current_period_ranking"	INTEGER NOT NULL,
            "grade_rank"	INTEGER NOT NULL,
            "description"	TEXT NOT NULL,
            "refresh_time"	TEXT NOT NULL
            )"""
    cursor.execute(sql)
    print("CREATE TABLE clan OK")
    cursor.close()
    conn.close()


# 创建 TABLE members
def create_sql_members(database):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    # cursor.execute("DROP TABLE IF EXISTS `members`")
    # 使用预处理语句创建表
    sql = """CREATE TABLE `members` (
            "viewer_id"	INTEGER NOT NULL PRIMARY KEY,
            "name"	TEXT NOT NULL,
            "level"	INTEGER NOT NULL,
            "role"	INTEGER NOT NULL,
            "total_power"	INTEGER NOT NULL,
            "join_clan_id"	INTEGER,
            "join_clan_name"	TEXT,
            "last_login_time"	TEXT NOT NULL,
            "last_refresh_time"	TEXT NOT NULL
            )"""
    cursor.execute(sql)
    print("CREATE TABLE members OK")
    cursor.close()
    conn.close()


# 创建 TABLE members(带jjc组别与排名)
def create_sql_members_arena(database):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    # 使用预处理语句创建表
    sql = """CREATE TABLE `members_arena` (
            "viewer_id"	INTEGER NOT NULL PRIMARY KEY,
            "user_name"	TEXT NOT NULL,
            "team_level"	INTEGER NOT NULL,
            "unit_num"	INTEGER NOT NULL,
            "total_power"	INTEGER NOT NULL,
            "arena_rank"	INTEGER,
            "arena_group"	INTEGER,
            "grand_arena_rank"	INTEGER,
            "grand_arena_group"	INTEGER,
            "user_comment"	TEXT,
            "favorite_unit"	INTEGER NOT NULL,
            "join_clan_id"	INTEGER,
            "join_clan_name"	TEXT,
            "last_login_time"	TEXT NOT NULL,
            "last_refresh_time"	TEXT NOT NULL
            )"""
    cursor.execute(sql)
    print("CREATE TABLE members_arena OK")
    cursor.close()
    conn.close()


# 创建 TABLE clan_status
def create_sql_status(database):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    # 使用预处理语句创建表
    sql = """CREATE TABLE `clan_status` (
            "clan_id"	INTEGER NOT NULL PRIMARY KEY,
            "exist"	INTEGER NOT NULL,
            "active"	INTEGER NOT NULL,
            "last_refresh_time"	TEXT NOT NULL
            )"""
    cursor.execute(sql)
    print("CREATE TABLE clan_status OK")
    cursor.close()
    conn.close()


# 创建 database grand_arena
def create_sql_grand(database):
    conn = sqlite3.connect(database)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = conn.cursor()
    # 使用 execute() 方法执行 SQL，如果表存在则删除
    # cursor.execute("DROP TABLE IF EXISTS `clan`")
    # 使用预处理语句创建表
    sql = """CREATE TABLE `grand_arena` (
            "viewer_id"	INTEGER NOT NULL PRIMARY KEY,
            "user_name"	TEXT NOT NULL,
            "team_level"	INTEGER NOT NULL,
            "grand_arena_rank"	INTEGER,
            "grand_arena_group"	INTEGER,
            "winning_number"	INTEGER,
            "favorite_unit"	INTEGER,
            "last_refresh_time"	TEXT NOT NULL
            )"""
    cursor.execute(sql)
    print("CREATE TABLE grand OK")
    cursor.close()
    conn.close()


# 将 clan 信息添加到TABLE中
def insert_new_clan_detail(database, clan_data_list):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    if isinstance(clan_data_list, dict):
        clan_data_list = [clan_data_list]
    for clan_data in clan_data_list:
        clan_dict = clan_data['clan']['detail']
        insert_keys = ["clan_id", "clan_name", "leader_viewer_id", "leader_name",
                       "join_condition", "activity", "member_num", "clan_battle_mode", "current_period_ranking",
                       "grade_rank", "description"]
        row_str = ''
        col_str = ''
        for key in insert_keys:
            col_str = (col_str + '%s' + ',') % key
            if key == "clan_name" or key == "leader_name" or key == "description":
                row_str = (row_str + "'%s'" + ',') % (validate(clan_dict[key]))
            else:
                row_str = (row_str + "'%s'" + ',') % (clan_dict[key])
        sql_row = """REPLACE INTO `clan` (%s) VALUES (%s)""" % (
        col_str[:-1] + ",refresh_time", row_str[:-1] + "," + time.strftime("'%Y-%m-%d %H:%M:%S'", time.localtime()))
        cursor.execute(sql_row)
        # 添加 members 信息
        members_list = clan_data['clan']['members']
        insert_keys = ['viewer_id', 'name', 'level', 'role', 'total_power']
        for member in members_list:
            row_str = ''
            col_str = ''
            for key in insert_keys:
                col_str = (col_str + '%s' + ',') % key
                if key == "name":
                    row_str = (row_str + "'%s'" + ',') % (validate(member[key]))
                else:
                    row_str = (row_str + "'%s'" + ',') % (member[key])
            col_str = col_str + 'join_clan_id,join_clan_name,last_login_time,last_refresh_time'
            row_str = row_str + str(clan_dict['clan_id']) + ",'" + validate(clan_dict["clan_name"]) + "'," + time.strftime(
                "'%Y-%m-%d %H:%M:%S'", time.localtime(member['last_login_time']))
            sql_row = """REPLACE INTO `members` (%s) VALUES (%s)""" % (
                col_str, row_str + ',' + time.strftime("'%Y-%m-%d %H:%M:%S'", time.localtime()))
            cursor.execute(sql_row)
    conn.commit()
    cursor.close()
    conn.close()


# 更新 members_arena 信息
def insert_members_arena(database, mix_data_list):
    if isinstance(mix_data_list[0], dict):
        mix_data_list = [mix_data_list]
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    for mix_data in mix_data_list:
        member_data = mix_data[0]
        clan_data = mix_data[1]
        member_dict = member_data['user_info']
        insert_keys = ["viewer_id", "user_name", "team_level", "unit_num",
                       "total_power", "arena_rank", "arena_group", "grand_arena_rank", "grand_arena_group", "user_comment"]
        row_str = ''
        col_str = ''
        for key in insert_keys:
            col_str = (col_str + '%s' + ',') % key
            if key == "user_name" or key == "user_comment":
                row_str = (row_str + "'%s'" + ',') % (validate(member_dict[key]))
            else:
                row_str = (row_str + "'%s'" + ',') % (member_dict[key])

        col_str = col_str + 'favorite_unit,join_clan_id,join_clan_name,last_login_time,last_refresh_time'
        if 'favorite_unit' not in member_data:
            member_data['favorite_unit']= {'id': 0}
        row_str = row_str + str(member_data['favorite_unit']['id']) + "," + str(clan_data[0]) + ",'" + validate(
            clan_data[1]) + "'," + time.strftime(
            "'%Y-%m-%d %H:%M:%S'", time.localtime(member_dict['last_login_time']))
        sql_row = """REPLACE INTO `members_arena` (%s) VALUES (%s);""" % (
            col_str, row_str + ',' + time.strftime("'%Y-%m-%d %H:%M:%S'", time.localtime()))
        cursor.execute(sql_row)
    conn.commit()
    cursor.close()
    conn.close()


# 更新 clan_status 信息
def insert_status_detail(database, status_dict):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    for clan_id in status_dict.keys():
        insert_keys = ["clan_id", "exist", "active"]
        row_str = ''
        col_str = ''
        for key in insert_keys:
            col_str = (col_str + '%s' + ',') % key
            if key == "clan_id":
                row_str = (row_str + "'%s'" + ',') % clan_id
            else:
                row_str = (row_str + "'%s'" + ',') % (status_dict[clan_id][key])
        sql_row = """REPLACE INTO `clan_status` (%s) VALUES (%s)""" % (
            col_str[:-1] + ",last_refresh_time", row_str[:-1] + "," + time.strftime("'%Y-%m-%d %H:%M:%S'", time.localtime()))
        cursor.execute(sql_row)
    conn.commit()
    cursor.close()
    conn.close()


# 更新 grand_arena 信息
def insert_members_grand(database, res, group):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    for member_dict in res['ranking']:
        insert_keys = ["viewer_id", "user_name", "team_level", "grand_arena_rank", "grand_arena_group", "winning_number"]
        row_str = ''
        col_str = ''
        for key in insert_keys:
            col_str = (col_str+'%s'+',') % key
            if key == "user_name":
                row_str = (row_str+"'%s'"+',') % (validate(member_dict[key]))
            elif key == "grand_arena_rank":
                row_str = (row_str+"'%s'"+',') % (member_dict["rank"])
            elif key == "grand_arena_group":
                row_str = (row_str+"'%s'"+',') % (group)
            else:
                row_str = (row_str+"'%s'"+',') % (member_dict[key])

        col_str = col_str + 'favorite_unit,last_refresh_time'
        row_str = row_str + str(member_dict['favorite_unit']['id'])

        sql_row = """REPLACE INTO `grand_arena` (%s) VALUES (%s)""" % (
                col_str, row_str+','+time.strftime("'%Y-%m-%d %H:%M:%S'", time.localtime()))
        cursor.execute(sql_row)
    conn.commit()
    cursor.close()
    conn.close()


# 查询 clan_status 信息
def get_clan_status(database):
    clan_status = {}
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    sql = """SELECT clan_id,exist,active FROM `clan_status`"""
    try:
        cursor.execute(sql)
        for clan in cursor:
            clan_status[clan[0]] = [clan[1], clan[2]]
        return clan_status
    except Exception as e:
        print('Error', e)
        conn.rollback()
    conn.close()


# 从数据库中获取公会id与上期排名
def get_clan_rank(database):
    clan_dict = {}
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    sql = """SELECT clan_id,current_period_ranking FROM `clan`"""
    try:
        cursor.execute(sql)
        for clan in cursor:
            clan_dict[clan[0]] = clan[1]
        return clan_dict
    except Exception as e:
        print('Error', e)
        conn.rollback()
    conn.close()


# 读取members信息, 返回dict
def get_sql_members(database):
    clan_members_dict = {}
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    sql = """SELECT * FROM `members`"""
    try:
        cursor.execute(sql)
        for user in cursor:
            clan_members_dict[user[0]] = user
        return clan_members_dict
    except Exception as e:
        print('Error', e)
        conn.rollback()
    conn.close()


# 读取members_arena信息, 返回dict
def get_sql_members_arena(database):
    clan_members_dict = {}
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    sql = """SELECT * FROM `members_arena`"""
    try:
        cursor.execute(sql)
        for user in cursor:
            clan_members_dict[user[0]] = user
        return clan_members_dict
    except Exception as e:
        print('Error', e)
        conn.rollback()
    conn.close()


# 获取 clan 平均战力
def get_clan_power(database):
    clan_power_dict = {}
    clan_id_dict = {}
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    sql = """SELECT clan_id,clan_name,current_period_ranking FROM `clan`"""
    try:
        cursor.execute(sql)
        for clan in cursor:
            if 0 < clan[2] < 151:
                clan_id_dict[clan[0]] = clan[1]
    except Exception:
        conn.rollback()
    for clan_id in clan_id_dict.keys():
        sql1 = """SELECT total_power FROM `members` WHERE join_clan_id = %s""" % clan_id
        try:
            cursor.execute(sql1)
            clan_power = []
            for user in cursor:
                clan_power.append(user[0])
            clan_power_dict[clan_id_dict[clan_id]] = int(numpy.mean(clan_power))
        except Exception:
            conn.rollback()
    return clan_power_dict
    conn.close()


# 更新单条目
def insert_single(database, table, data):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    row_str = ''
    for i, value in enumerate(data):
        if i in [1, 9]:
            row_str = (row_str + "'%s'" + ',') % (validate(value))
        else:
            row_str = (row_str + "'%s'" + ',') % value

    sql_row = """REPLACE INTO `%s` VALUES (%s)""" % (table, row_str[:-1])
    try:
        cursor.execute(sql_row)
    except Exception as e:
        print('Error', e)
        conn.rollback()
    conn.commit()
    cursor.close()
    conn.close()
