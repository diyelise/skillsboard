#!/home/enviroments/skb_env/bin/python3.7
# -*- coding: utf-8 -*-
import asyncpg
import asyncio
import scrubber
import env
from itertools import product
from aiohttp import ClientSession
from random import randint as rnd

class UploadData:

    def __init__(self):
        self.loop = asyncio.get_event_loop()

    def run_process(self):
        self.loop.run_until_complete(self.start())
        self.loop.close()

    async def get_info(self, db, lang: str, associate: str = None):
        """
        Поиск вакансий по выбранному языку
        :param db: сессия бд
        :param lang: строка поиска для языка
        :param associate: ассоциация с языком
        :return:

        associate нужно т.к вакансия может содержать
        Python, python, Go, golang. По умолчанию равна искомой строке
        """
        await asyncio.sleep(delay=rnd(5, 15))
        try:
            list_to_add = []
            async with ClientSession() as client:
                vacs = await scrubber.Scrub.list_vacancies(session=client, lang=lang)
                if vacs:
                    tasks = [asyncio.create_task(scrubber.Scrub.get_vacancy(
                        session=client, vac_id=i
                    )) for i in vacs]
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
                    list_to_add.extend([obj.result() for obj in done if obj.result() is not None])

                lang = associate if associate else lang
                async with db.acquire() as conn:
                    for res in list_to_add:
                        async with conn.transaction():
                            await conn.execute("""
                                insert into sb.public.vacancies
                                (vac_id, city_id, city, experience, schedule, employment, salary_from, salary_to, lang_type)
                                values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                                on conflict (vac_id) do nothing;
                            """, res.get('id'), res.get('city_id'), res.get('city'),
                                             res.get('exp'), res.get('sch'), res.get('emp'),
                                             res.get('s_from'), res.get('s_to'), lang)
            return f'OK {lang}', None
        except Exception as err:
            return f'BAD {lang}', str(err)

    async def start(self):
        db = await self.init_db()  # pool
        tasks = [
            asyncio.create_task(self.get_info(db, lang='python')),
            asyncio.create_task(self.get_info(db, lang='Python', associate='python')),
            asyncio.create_task(self.get_info(db, lang='golang')),
            asyncio.create_task(self.get_info(db, lang='Golang', associate='golang')),
            asyncio.create_task(self.get_info(db, lang='Java', associate='java')),
            asyncio.create_task(self.get_info(db, lang='java', associate='java')),
            asyncio.create_task(self.get_info(db, lang='C/С++', associate='c/c++')),
            asyncio.create_task(self.get_info(db, lang='javascript')),
        ]
        done, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        for obj in done:
            status, err = obj.result()
        async with db.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                    update sb.public.vacancies vac
                    set reg = rg.reg_name
                    from (select city_name, reg_name from regions) as rg
                    where vac.city = rg.city_name
                """)
                await conn.execute("""
                    delete from sb.public.vacancies
                    where reg is null
                """)
        await db.close()

    async def init_db(self):
        return await asyncpg.create_pool(dsn=env.admin_db)

    async def update_regions(self, db):
        async with ClientSession() as client:
            regions = await scrubber.Scrub.get_regions(session=client, country_id=113)  # Russia
            if regions:
                update_str = []
                for k,v in regions.items():
                    data = [(x,y) for x,y in product([k], v)]
                    update_str.extend(data)
                if update_str:
                    for reg, city in update_str:
                        res = await db.execute("""
                            insert into sb.public.regions
                            (reg_name, city_name)
                            values
                            ($1, $2)
                            on conflict (city_name) do nothing;
                        """, reg, city)
        print('update process has been completed')


upd = UploadData()
upd.run_process()