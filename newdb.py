from pcrapi import PCRApi
import json
import pcrsql
import time
import asyncio
import os
from queuedb import QueueDb


# 获取所有clan
def new_clan(db_new=None, db_last=None, query_type='month', sync_num=10, new_clan_add=20):
    if db_new is None:
        db_new = 'data/month/pcr_qd' + time.strftime("%y%m", time.localtime()) + '.db'
        db_last = 'data/month/' + os.listdir('data/month')[-1]
    quest_clan_list = []
    clan_total = pcrsql.sql_get(db_last, 'status')
    for clan_id in clan_total.keys():
        if clan_total[clan_id][0 if query_type == 'total' else 1] == 1:
            quest_clan_list.append(clan_id)
    for clan_id in range(quest_clan_list[-1], quest_clan_list[-1] + new_clan_add):
        quest_clan_list.append(clan_id)

    def data_process_clan(clan_data):
        if 'clan' in clan_data:
            print('已查询公会' + str(clan_data['clan']['detail']['clan_id']) + '\n')
            last_login_time = 0
            for member in clan_data['clan']['members']:
                if member['last_login_time'] > last_login_time:
                    last_login_time = member['last_login_time']
            # 60 days
            if time.time() - last_login_time < 2592000*2:
                return {"data": clan_data, "exist": 1, "active": 1}
            else:
                return {"data": clan_data, "exist": 1, "active": 0}
        elif 'server_error' in clan_data:
            if 'message' in clan_data['server_error']:
                if '此行会已解散' in clan_data['server_error']['message']:
                    print('已解散工会\n')
                    return {"data": None, "exist": 0, "active": 0}
                elif '连接中断' in clan_data['server_error']['message']:
                    return 0

    def db_insert(frag_db, data_return):
        clan_data_list = []
        for clan in data_return.values():
            if clan['data']:
                clan_data_list.append(clan['data'])
        pcrsql.sql_insert(frag_db, 'clan', clan_data_list)
        pcrsql.sql_insert(frag_db, 'status', data_return)
        print('完成'+str(list(data_return.keys())))
        time.sleep(3)

    clan_queue = QueueDb(db_new, ['clan', 'status', 'members'], quest_clan_list, data_process_clan, db_insert, sync_num)
    clan_queue.queue_start()


# 新分场信息
def new_group():
    db_new = 'data/arena/alive/' + time.strftime("%y%m", time.localtime()) + '.db'
    db_members = 'data/month/' + os.listdir('data/month')[-1]
    member_dict = pcrsql.sql_get(db_members, 'members')
    quest_members_list = []
    for member_id in member_dict.keys():
        if member_dict[member_id][4] > 1500000:
            login_time = time.mktime(time.strptime(member_dict[member_id][7],"%Y-%m-%d %H:%M:%S"))
            if time.time() - login_time < 2592000:
                quest_members_list.append(member_id)

    def data_process_members(members_data):
        if 'user_info' in members_data:
            viewer_id = members_data['user_info']['viewer_id']
            if viewer_id in member_dict.keys():
                clan_info = [member_dict[viewer_id][5], member_dict[viewer_id][6]]
            return [members_data, clan_info]
        else:
            return 0

    def db_insert(frag_db, multi_data):
        multi_data_list = []
        for member_data_list in multi_data.values():
            multi_data_list.append(member_data_list)
        pcrsql.sql_insert(frag_db, 'arena', multi_data_list)
        time.sleep(3)

    member_queue = QueueDb(db_new, 'members_arena', quest_members_list, data_process_members, db_insert, 10)
    member_queue.queue_start()


# 查询农场号组别
def find_farm_group():
    db_name = 'data/arena/farm/farm' + time.strftime("%y%m", time.localtime()) + '.db'
    with open('json/account-farm.json') as f:
        account_farm = json.load(f)
    farm_account = []
    for account in account_farm["accounts"]:
        farm_account.append(account["vid"])

    def data_process_farm(farm_data):
        if 'user_info' in farm_data:
            return [farm_data, ["''", "''"]]
        else:
            return 0

    def db_insert(frag_db, farm_multi_data):
        multi_data_list = []
        for member_data_list in farm_multi_data.values():
            multi_data_list.append(member_data_list)
        pcrsql.sql_insert(frag_db, 'arena', multi_data_list)

    farm_queue = QueueDb(db_name, 'members_arena', farm_account, data_process_farm, db_insert)
    farm_queue.queue_start()


# 查询pjjc
def find_pjjc_top():
    with open('json/grand_account.json') as f:
        account_grand = json.load(f)
    db_name = 'data/arena/grand/' + time.strftime("%y%m%d", time.localtime()) + '.db'
    pcrsql.sql_create(db_name, 'grand')

    async def query_top(db, client, group):
        for i in range(1, 6):
            res = await client.query_grand(i)
            pcrsql.insert_members_grand(db, res, group)
            print("完成第"+str(group)+"组第"+str(i)+"页")
        return 0

    async def queue_main():
        tasks = []
        for client_id in account_grand.keys():
            if client_id != "access_key":
                client = PCRApi(account_grand[client_id]["vid"], account_grand[client_id]["uid"], account_grand["access_key"])
                await client.login()
            else:
                continue
            task = asyncio.create_task(query_top(db_name, client, int(client_id)))
            tasks.append(task)
        await asyncio.gather(*tasks)

    start = time.time()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(queue_main())
    end = time.time()
    print(end-start)


if __name__ == '__main__':
    new_clan()
    # new_group()
    # find_pjjc_top()
