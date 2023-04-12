import os

from pcrapi import create_client
import pcrsql
import time
import asyncio
import math
import sqlite3


class QueueDb:
    def __init__(self, db_name, db_type, query_list, db_func, db_func2=None, sync_num=10):
        self.db_name = db_name.split('/')[-1]
        self.db_folder = db_name[0:len(db_name)-len(self.db_name)]
        self.db_type = db_type
        self.sync_num = sync_num
        self.query_list = query_list
        if len(self.query_list) < 500:
            self.sync_num = 1
        if self.query_list[0] > 1000000000000:
            self.query_type = 'id'
        else:
            self.query_type = 'clan'
        self.db_func = db_func
        self.db_func2 = db_func2
        self.create_fragment()

    def create_fragment(self):
        pcrsql.sql_create(self.db_folder + self.db_name, self.db_type)
        for nums in range(self.sync_num):
            pcrsql.sql_create(self.db_folder + str(nums) + '-' + self.db_name, self.db_type)

    async def client_list(self, client, client_index, frag_list):
        for loop_time in range(math.ceil(len(self.query_list)/(50 * self.sync_num))):
            query_index = [j + 50 * self.sync_num * loop_time for j in frag_list]
            data_return = {}
            for index in query_index:
                if index < len(self.query_list):
                    query_id_client = self.query_list[index]
                    print('查询'+str(query_id_client))
                for retry in range(4):
                    if self.query_type == 'clan':
                        query_data = await client.query_clan(query_id_client)
                    else:
                        query_data = await client.query_id(query_id_client)
                    data_status = self.db_func(self.db_folder+str(client_index)+'-'+self.db_name, query_data)
                    if data_status:
                        data_return[query_id_client] = data_status
                        break
                    else:
                        time.sleep(5)
                        await client.login()
            if self.db_func2 and data_return:
                self.db_func2(self.db_folder+str(client_index)+'-'+self.db_name, data_return)

    async def queue_main(self):
        tasks = []
        for client_id in range(self.sync_num):
            client = await create_client(client_id)
            frag_list = list(range(client_id*50, 50+client_id*50))
            task = asyncio.create_task(self.client_list(client, client_id, frag_list))
            tasks.append(task)
        await asyncio.gather(*tasks)

    def merge_db(self):
        for nums in range(self.sync_num):
            con3 = sqlite3.connect(self.db_folder + self.db_name)
            con3.execute("ATTACH '"+self.db_folder+str(nums)+'-'+self.db_name + "' as dba")
            con3.execute("BEGIN")
            for row in con3.execute("SELECT * FROM dba.sqlite_master WHERE type='table'"):
                combine = "REPLACE INTO " + row[1] + " SELECT * FROM dba." + row[1]
                print(combine)
                con3.execute(combine)
            con3.commit()
            con3.execute("detach database dba")
            os.remove(self.db_folder+str(nums)+'-'+self.db_name)

    def queue_start(self):
        start = time.time()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.queue_main())
        self.merge_db()
        end = time.time()
        print(end-start)
