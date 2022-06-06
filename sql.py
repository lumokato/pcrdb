import sqlite3
import time
import re


def validate(name):
    rstr = r"[\/\\\:\*\?\'\"\<\>\|\-\+\%]"
    new_name = re.sub(rstr, "''", name)
    return new_name


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


# 将 clan 信息添加到TABLE中
def insert_new_clan_detail(clan_data, database):
    clan_dict = clan_data['clan']['detail']
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    insert_keys = ["clan_id", "clan_name", "leader_viewer_id", "leader_name",
                   "join_condition", "activity", "member_num", "clan_battle_mode", "current_period_ranking", "grade_rank", "description"]
    ROWstr = ''
    COLstr = ''
    for key in insert_keys:
        COLstr = (COLstr+'%s'+',') % key
        if key == "clan_name" or key == "leader_name" or key == "description":
            ROWstr = (ROWstr+"'%s'"+',') % (validate(clan_dict[key]))
        else:
            ROWstr = (ROWstr+"'%s'"+',') % (clan_dict[key])
    sqlrow = """REPLACE INTO `clan` (%s) VALUES (%s)""" % (COLstr[:-1]+",refresh_time", ROWstr[:-1]+","+time.strftime("'%Y-%m-%d %H:%M:%S'", time.localtime()))
    cursor.execute(sqlrow)
    # 添加 members 信息
    members_list = clan_data['clan']['members']
    insert_keys = ['viewer_id', 'name', 'level', 'role', 'total_power']
    for member in members_list:
        ROWstr = ''
        COLstr = ''
        for key in insert_keys:
            COLstr = (COLstr+'%s'+',') % key
            if key == "name":
                ROWstr = (ROWstr+"'%s'"+',') % (validate(member[key]))
            else:
                ROWstr = (ROWstr+"'%s'"+',') % (member[key])
        COLstr = COLstr + 'join_clan_id,join_clan_name,last_login_time,last_refresh_time'
        ROWstr = ROWstr + str(clan_dict['clan_id']) + ",'" + validate(clan_dict["clan_name"]) + "'," + time.strftime(
            "'%Y-%m-%d %H:%M:%S'", time.localtime(member['last_login_time']))
        sqlrow = """REPLACE INTO `members` (%s) VALUES (%s)""" % (
            COLstr, ROWstr+','+time.strftime("'%Y-%m-%d %H:%M:%S'", time.localtime()))
        cursor.execute(sqlrow)
    conn.commit()
    cursor.close()
    conn.close()


# 从数据库中获取公会id与上期排名
def get_clan_id(last_db):
    clan_dict = {}
    conn = sqlite3.connect(last_db)
    cursor = conn.cursor()
    sql = """SELECT clan_id,current_period_ranking FROM `clan_detail`"""
    try:
        cursor.execute(sql)
        for clan in cursor:
            clan_dict[clan[0]] = clan[1]
        return clan_dict
    except Exception as e:
        print('Error', e)
        conn.rollback()
    conn.close()


# 创建 TABLE members_detail
def create_sql_members_detail(database):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    # 使用预处理语句创建表
    sql = """CREATE TABLE `members_detail` (
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
    print("CREATE TABLE members_detail OK")
    cursor.close()
    conn.close()


# 创建 TABLE members_abandon
def create_sql_members_abandon(database):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    # 使用预处理语句创建表
    sql = """CREATE TABLE `members_abandon` (
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
    print("CREATE TABLE members_abandon OK")
    cursor.close()
    conn.close()


# 更新 members_detail 信息
def insert_members_detail(member_data, clan_data, database):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    member_dict = member_data['user_info']
    insert_keys = ["viewer_id", "user_name", "team_level", "unit_num",
                   "total_power", "arena_rank", "arena_group", "grand_arena_rank", "grand_arena_group", "user_comment"]
    ROWstr = ''
    COLstr = ''
    for key in insert_keys:
        COLstr = (COLstr+'%s'+',') % key
        if key == "user_name" or key == "user_comment":
            ROWstr = (ROWstr+"'%s'"+',') % (validate(member_dict[key]))
        else:
            ROWstr = (ROWstr+"'%s'"+',') % (member_dict[key])

    COLstr = COLstr + 'favorite_unit,join_clan_id,join_clan_name,last_login_time,last_refresh_time'
    ROWstr = ROWstr + str(member_data['favorite_unit']['id']) + "," + str(clan_data[0]) + ",'" + validate(clan_data[1]) + "'," + time.strftime(
            "'%Y-%m-%d %H:%M:%S'", time.localtime(member_dict['last_login_time']))
    sqlrow = """REPLACE INTO `members_detail` (%s) VALUES (%s)""" % (
            COLstr, ROWstr+','+time.strftime("'%Y-%m-%d %H:%M:%S'", time.localtime()))
    cursor.execute(sqlrow)
    conn.commit()
    cursor.close()
    conn.close()


# 读取members信息, 返回dict
def get_sql_members(database, ver=0):
    clan_members_dict = {}
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    if ver == 1:
        sql = """SELECT * FROM `clan_members`"""
    elif ver == 2:
        sql = """SELECT * FROM `members_detail`"""
    else:
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


# 更新单条目
def insert_single(database, table, data):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    ROWstr = ''
    for i, value in enumerate(data):
        if i in [1, 9]:
            ROWstr = (ROWstr+"'%s'"+',') % (validate(value))
        else:
            ROWstr = (ROWstr+"'%s'"+',') % (value)

    sqlrow = """REPLACE INTO `%s` VALUES (%s)""" % (table, ROWstr[:-1])
    try:
        cursor.execute(sqlrow)
    except Exception as e:
        print('Error', e)
        conn.rollback()
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    # create_sql_clan('test.db')
    # create_sql_members('test.db')
    get_clan_id('data/pcr_qdall.db')
