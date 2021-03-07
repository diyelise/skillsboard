from typing import Union, List, Dict, Any
import query_calculation
import psycopg2
import env

class Calculation:

    """
    Выполнение рассчетов для недельной,месячной,годовой статистики
    """
    def __init__(self):
        self.db = psycopg2.connect(dsn=env.sk_board_high)

    def update_stats(self, period: str = 'week'):
        sql = None
        if period == 'week':
            sql = query_calculation.update_regions_last_week
        with self.db.cursor() as cur:
            try:
                cur.execute(sql)
                cur.commit()
                return 'ok', None
            except psycopg2.DatabaseError as err:
                return 'fail', str(err)

    def close_db(self):
        self.db.close()


calc = Calculation()
calc.close_db()