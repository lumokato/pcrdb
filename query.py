from pcrapi import PCRApi
import json
import sql
import time

last_db = 'data/pcr_qd2205.db'
this_db = 'data/pcr_qd2206.db'


# 获取每月新db
def new_db(last_db, this_db, filter_rank=0, start_clan=0):
    # last_db = 'data/' + last_db
    # this_db = 'data/' + this_db
    try:
        sql.create_sql_clan(this_db)
        sql.create_sql_members(this_db)
    except Exception:
        pass

    with open('account.json') as f:
        account_data = json.load(f)
    account = account_data["new2"]
    App = PCRApi(account['viewer_id'], account['uid'], account['access_key'])
    # 读取上期id
    clan_dict = sql.get_clan_id(last_db)
    for clan_id in clan_dict.keys():
        file = open('log.txt', 'a', encoding='utf-8')
        if filter_rank:
            if clan_dict[clan_id] == 0 or clan_dict[clan_id] > filter_rank:
                continue
        if clan_id < start_clan:
            continue
        clan_data = App.query_clan(clan_id)
        if 'clan' in clan_data:
            sql.insert_new_clan_detail(clan_data, this_db)
            file.write('已更新公会'+str(clan_id)+'\n')
        else:
            file.write('无此公会'+str(clan_id)+'\n')
        file.close()
    # 读取新id
    clan_new = list(clan_dict.keys())[-1]
    empty_count = 0
    while True:
        file = open('log.txt', 'a', encoding='utf-8')
        clan_new += 1
        clan_data = App.query_clan(clan_new)
        if 'clan' in clan_data:
            sql.insert_new_clan_detail(clan_data, this_db)
            file.write('已更新公会'+str(clan_new)+'\n')
            empty_count = 0
        else:
            file.write('无此公会'+str(clan_id)+'\n')
            empty_count += 1
        if empty_count > 9:
            break
        file.close()


# 获取活跃用户信息
def members_active(start=0):
    try:
        sql.create_sql_members_plus(this_db, 'members_detail')
    except Exception:
        pass

    member_dict = sql.get_sql_members(this_db)
    active_dict = {}
    for vid in member_dict.keys():
        member_data = member_dict[vid]
        login_time = time.strptime(member_data[7], "%Y-%m-%d %H:%M:%S")
        refresh_time = time.strptime(member_data[8], "%Y-%m-%d %H:%M:%S")
        if int(time.mktime(refresh_time)) - int(time.mktime(login_time)) < 7 * 86400 and int(member_data[4]) > 600000:
            active_dict[vid] = [member_data[5], member_data[6]]

    with open('account.json') as f:
        account_data = json.load(f)
    account = account_data["new2"]
    App = PCRApi(account['viewer_id'], account['uid'], account['access_key'])
    count = 0
    for active_id in active_dict.keys():
        count += 1
        active_data = App.query_id(active_id)
        sql.insert_members_detail(active_data, active_dict[active_id], this_db)
        if count % 100 == 0:
            file = open('log2.txt', 'a', encoding='utf-8')
            file.write('已完成更新'+str(count)+'\n')
            file.close()


# 筛选工会外成员
def members_outside():
    outside_list = []
    member_last = sql.get_sql_members(last_db, 1)
    member_this = list(sql.get_sql_members(this_db).keys())
    member_plus = list(sql.get_sql_members('data/plus.db', 2).keys())
    for vid in member_last.keys():
        member_data = member_last[vid]
        login_time = time.strptime(member_data[7], "%Y-%m-%d %H:%M:%S")
        refresh_time = time.strptime(member_data[10], "%Y-%m-%d %H:%M:%S")
        if int(time.mktime(refresh_time)) - int(time.mktime(login_time)) < 7 * 86400 and int(member_data[4]) > 400000:
            if vid not in member_this and vid not in member_plus:
                outside_list.append(vid)
    print(len(outside_list))
    try:
        sql.create_sql_members_plus('data/plus.db', 'members_detail')
    except Exception:
        pass
    with open('account.json') as f:
        account_data = json.load(f)
    account = account_data["new2"]
    App = PCRApi(account['viewer_id'], account['uid'], account['access_key'])
    count = 0
    for outside_id in outside_list:
        count += 1
        outside_data = App.query_id(outside_id)
        sql.insert_members_detail(outside_data, ["''", "''"], 'data/plus.db')
        if count % 100 == 0:
            file = open('log2.txt', 'a', encoding='utf-8')
            file.write('已完成更新'+str(count)+'\n')
            file.close()


# 合并db(单次)
def merge_outside():
    try:
        sql.create_sql_members_plus(this_db, 'members_abandon')
    except Exception:
        pass
    member_plus = sql.get_sql_members('data/plus.db', 2)
    for vid in member_plus.keys():
        member_data = member_plus[vid]
        login_time = time.strptime(member_data[13], "%Y-%m-%d %H:%M:%S")
        refresh_time = time.strptime(member_data[14], "%Y-%m-%d %H:%M:%S")
        if int(time.mktime(refresh_time)) - int(time.mktime(login_time)) < 7 * 86400:
            sql.insert_single(this_db, 'members_detail', member_data)
        else:
            sql.insert_single(this_db, 'members_abandon', member_data)


# 查询单场下防信息
def find_jjc_down():
    with open('account.json') as f:
        account_data = json.load(f)
    account = account_data["jjc"]
    App = PCRApi(account['viewer_id'], account['uid'], account['access_key'])
    down_list = []
    for i in range(1, 11):
        down_list += App.query_jjc(i)
    print(down_list)


# 查询农场号组别
def find_farm_group():
    with open('account.json') as f:
        account_data = json.load(f)
    account = account_data["new2"]
    App = PCRApi(account['viewer_id'], account['uid'], account['access_key'])
    with open('account-farm.json') as f:
        account_farm = json.load(f)
    # try:
    #     sql.create_sql_members_plus('farm.db', 'members_detail')
    # except Exception:
    #     pass

    for user in account_farm['accounts']:
        farm_data = App.query_id(user['vid'])
        try:
            sql.insert_members_detail(farm_data, ["''", "''"], 'data/farm.db')
        except Exception:
            print(str(user))


# 查询top下防信息
def find_top_down():
    down_dict = {}
    member_dict = sql.get_sql_members(last_db, 2)
    member_dict2 = sql.get_sql_members('data/new_group.db', 2)
    for mem in member_dict2:
        jjc_down = member_dict2[mem][5] - member_dict[mem][5]
        down_dict[member_dict2[mem][1]] = jjc_down
    a = sorted(down_dict.items(), key=lambda x: x[1], reverse=False)
    with open('down.json', 'w', encoding='utf-8') as fp:
        json.dump(a, fp, indent=4, ensure_ascii=False)


# 新分场信息
def new_group():
    try:
        sql.create_sql_members_plus('data/new_group.db', 'members_detail')
    except Exception:
        pass

    member_dict = sql.get_sql_members(this_db, 2)
    active_dict = {}
    for vid in member_dict.keys():
        member_data = member_dict[vid]
        if member_data[5] and member_data[7]:
            if member_data[5] < 51 or member_data[7] < 51:
                active_dict[vid] = [member_data[11] if member_data[11] else 0, member_data[12]]
    print(len(active_dict))
    with open('account.json') as f:
        account_data = json.load(f)
    account = account_data["new2"]
    App = PCRApi(account['viewer_id'], account['uid'], account['access_key'])
    count = 0
    for active_id in active_dict.keys():
        count += 1
        active_data = App.query_id(active_id)
        sql.insert_members_detail(active_data, active_dict[active_id], 'data/new_group.db')
        if count % 100 == 0:
            file = open('log2.txt', 'a', encoding='utf-8')
            file.write('已完成更新'+str(count)+'\n')
            file.close()


# 新分场信息
def new_group_clan(clan_id):
    try:
        sql.create_sql_members_plus('data/new_group_clan.db', 'members_detail')
    except Exception:
        pass

    member_dict = sql.get_sql_members(last_db, 1)
    active_dict = {}
    for vid in member_dict.keys():
        member_data = member_dict[vid]
        if member_data[5] == clan_id:
            active_dict[vid] = [member_data[5] if member_data[5] else 0, member_data[6]]
    # print(len(active_dict))
    with open('account.json') as f:
        account_data = json.load(f)
    account = account_data["new2"]
    App = PCRApi(account['viewer_id'], account['uid'], account['access_key'])
    count = 0
    for active_id in active_dict.keys():
        count += 1
        active_data = App.query_id(active_id)
        sql.insert_members_detail(active_data, active_dict[active_id], 'data/new_group_clan.db')
        if count % 100 == 0:
            file = open('log2.txt', 'a', encoding='utf-8')
            file.write('已完成更新'+str(count)+'\n')
            file.close()


# 原场信息
def pre_data():
    member_dict = sql.get_sql_members(this_db, 2)
    member_dict1 = sql.get_sql_members('data/new_group.db', 2)
    pre_group = {}
    active_dict = []
    for vid in member_dict1.keys():
        member_data = member_dict1[vid]
        if member_data[6] == 49:
            pre_data = member_dict[vid]
            if pre_data[6] in pre_group.keys():
                pre_group[pre_data[6]] += 1
            else:
                pre_group[pre_data[6]] = 1
            if pre_data[5] < 21:
                active_dict.append(pre_data)

    # print(len(active_dict))
    # with open('account.json') as f:
    #     account_data = json.load(f)
    # account = account_data["new2"]
    # App = PCRApi(account['viewer_id'], account['uid'], account['access_key'])
    # count = 0
    # for active_id in active_dict.keys():
    #     count += 1
    #     active_data = App.query_id(active_id)
    #     sql.insert_members_detail(active_data, active_dict[active_id], 'data/new_group_clan.db')
    #     if count % 100 == 0:
    #         file = open('log2.txt', 'a', encoding='utf-8')
    #         file.write('已完成更新'+str(count)+'\n')
    #         file.close()


if __name__ == '__main__':
    # find_jjc_down()
    new_db(last_db, this_db, 1500, 0)
    # find_farm_group()
