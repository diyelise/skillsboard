from aiohttp import ClientSession


class Scrub:

    @staticmethod
    async def get_vacancy(session: ClientSession, vac_id: int) -> dict:
        """
        Получение тела вакансии
        :param session: объект сессии
        :param vac_id: идентификатор вакансии
        :return: dict
        """
        url = f'https://api.hh.ru/vacancies/{vac_id}'
        async with session.get(url) as resp:
            return {
                'status': resp.status,
                'body': await resp.json()
            }

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
            return {
                'from': salary.get('from'),
                'to': salary.get('to'),
                'curr': salary.get('currency'),
                'gross': salary.get('gross'),
            }