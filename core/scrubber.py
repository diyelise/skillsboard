import asyncio
import asyncpg
from collections import defaultdict
from itertools import product
from datetime import datetime, timedelta
from aiohttp import ClientSession


class Scrub:

    @staticmethod
    async def list_vacancies(session: ClientSession, lang: str):
        """
        :param session:
        :param lang:
        :return:
        """
        time_marker = (datetime.now() - timedelta(minutes=30)).isoformat()
        pages = 0
        active_vacancies = []
        url = 'https://api.hh.ru/vacancies?type=open&text={0}&date_from={1}&page={2}&per_page=100'
        data = await Scrub.fetch(session, url.format(lang, time_marker, 0))
        if data.get('status') == 200:
            body = data.get('body')
            pages = body.get('pages')
            active_vacancies = [i.get('id') for i in body.get('items')]
            if pages > 0 and body.get('found') > 100:
                tasks = []
                urls = [url.format(lang, time_marker, str(i)) for i in range(1, pages + 1)]
                tasks.extend([asyncio.create_task(Scrub.fetch(session, url)) for url in urls])
                done, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
                for item in done:
                    res = item.result()
                    items = res.get('items')
                    if items:
                        active_vacancies.extend([i.get('id') for i in items])
        return list(set(active_vacancies))

    @staticmethod
    async def fetch(session: ClientSession, url: str) -> dict:
        async with session.get(url) as resp:
            return {
                'status': resp.status,
                'body': await resp.json()
            }

    @staticmethod
    async def get_vacancy(session: ClientSession, vac_id: int) -> dict or None:
        """
        Получение тела вакансии
        :param session: объект сессии
        :param vac_id: идентификатор вакансии
        :return: dict
        """
        url = f'https://api.hh.ru/vacancies/{vac_id}'
        data = await Scrub.fetch(session, url)
        if data.get('status') == 200:
            salary = Scrub.get_salary(data.get('body'))
            if salary:
                area = Scrub.get_area(data.get('body'))
                experience = Scrub.get_experience(data.get('body'))
                schedule = Scrub.get_schedule(data.get('body'))
                employment = Scrub.get_employment(data.get('body'))
                return {
                    'id': int(data['body']['id']),
                    'city_id': int(area.get('city_id')),
                    'city': area.get('city'),
                    'exp': experience,
                    'sch': schedule,
                    'emp': employment,
                    's_from': salary.get('from'),
                    's_to': salary.get('to'),
                }
            else:
                return None

    @staticmethod
    def get_employment(value: dict):
        employment = value.get('employment')
        return employment.get('name') if employment else None

    @staticmethod
    def get_schedule(value: dict) ->str or None:
        schedule = value.get('schedule')
        return schedule.get('name') if schedule else None

    @staticmethod
    def get_area(value: dict) -> dict:
        """
        Получить город и идентификатор
        :param value: response
        :return: dict
        """
        area = value.get('area')
        if area:
            return {
                'city': area.get('name'),
                'city_id': area.get('id'),
            }

    @staticmethod
    def get_experience(value: dict) -> str or None:
        """
        Получить опыт работы
        :param value: response
        :return: str or None
        """
        experience = value.get('experience')
        return experience.get('name') if experience else None

    @staticmethod
    def get_salary(value: dict) -> dict:
        """
        Получить информацию о зарплате
        :param value: response
        :return: dict
        """
        salary = value.get('salary')
        if salary:
            _from = salary.get('from')
            _to = salary.get('to')
            return {
                'from': int(_from) if _from else 0,
                'to': int(_to) if _to else 0,
                'curr': salary.get('currency'),
                'gross': salary.get('gross'),
            }

    @staticmethod
    async def get_regions(session: ClientSession, country_id: int) -> defaultdict or None:
        """
        Получение
        :param country_id: id страны (113 - Россия)
        :param session: сессия
        :return:
        """
        url = f'https://api.hh.ru/areas/{country_id}'
        data = await Scrub.fetch(session, url)
        if data.get('status') == 200:
            regions = defaultdict(list)
            body = data.get('body')
            areas = body.get('areas')
            for i in areas:
                for j in i.get('areas'):
                    regions[i.get('name')].append(j.get('name'))
            return regions
        else:
            return None