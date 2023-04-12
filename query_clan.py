import os
import pcrsql
import json


# 查询成员加入时间
def members_alive(clan_id):
    db_folder = 'data/month/'
    db_all = os.listdir(db_folder)
    db_month = []
    for db in db_all:
        db_month.append(db)
    member_last = pcrsql.get_sql_members(db_folder + db_month[-1])
    mem_last_list = []
    for mem in member_last.keys():
        if member_last[mem][5] == clan_id:
            mem_last_list.append(mem)
    mem_join_list = []
    for db in db_month:
        member_month = pcrsql.get_sql_members(db_folder + db)
        mem_month_list = []
        name_month_list = []
        for mem in member_month.keys():
            if member_month[mem][5] == clan_id and mem in mem_last_list:
                mem_month_list.append(mem)
                if mem not in mem_join_list:
                    mem_join_list.append(mem)
                    name_month_list.append(member_last[mem][1])
        # print(db[6:10] + '现存人数: ' + str(len(mem_month_list)) + str(name_month_list))
        print(db[6:10] + '加入: ' + str(name_month_list)[1:-1])
    return 0


# 查询成员当前所在工会
def members_now(clan_id, month):
    db_folder = 'data/month/'
    db_all = os.listdir(db_folder)
    member_last = pcrsql.get_sql_members(db_folder + db_all[-1])
    mem_join_dict = {}
    for db in db_all:
        if db[6:10] == month:
            member_month = pcrsql.get_sql_members(db_folder + db)
            for mem in member_month.keys():
                if member_month[mem][5] == clan_id and mem in member_last.keys():
                    if member_last[mem][6] not in mem_join_dict.keys():
                        mem_join_dict[member_last[mem][6]] = [member_last[mem][1]]
                    else:
                        mem_join_dict[member_last[mem][6]].append(member_last[mem][1])

    for clan in mem_join_dict.keys():
        print(clan+str(mem_join_dict[clan]))


# 查询平均战力
def query_avg_power(month):
    db_folder = 'data/month/'
    db_all = os.listdir(db_folder)
    for db in db_all:
        if db[6:10] == month:
            clan_dict = pcrsql.get_clan_power(db_folder + db)
            write_list = sorted(clan_dict.items(), key=lambda x: x[1], reverse=True)
            with open('data/'+month+'.txt', 'w', encoding='utf-8', newline="") as fp:
                for clan in write_list:
                    fp.write(clan[0]+': '+str(clan[1]) + '\n')


# 查询头像使用率
def get_avatar():
    avatar_dict = {}
    total = 0
    db = pcrsql.get_sql_members_arena('data/arena/alive2304.db')
    for member in db.keys():
        # if db[member][5] < 51 and db[member][7] < 51:
        if True:
            total += 1
            avatar = db[member][10]
            if avatar in avatar_dict.keys():
                avatar_dict[avatar] += 1
            else:
                avatar_dict[avatar] = 1
    with open('json/unit_id.json', encoding='utf-8') as fp:
        id_dict = json.load(fp)
    for unit_id in id_dict:
        if int(unit_id) not in avatar_dict.keys():
            print(id_dict[unit_id])
    avatar_dict_trans = {}
    for avatar in sorted(avatar_dict.items(), key=lambda x: x[1], reverse=True):
        avatar_dict_trans[id_dict[str(avatar[0])]] = '{:.2%}'.format(avatar_dict[avatar[0]]/total)
    with open('json/fmz.json', 'w', encoding='utf-8') as fp:
        json.dump(avatar_dict_trans, fp, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    get_avatar()
