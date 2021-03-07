
update_regions_last_week = """
    insert into regions_last_week
    (region, employment_type, lang_type, schedule_type, experience_type, vac_count, avg_salary_from, avg_salary_to, calc_week)
    select * from sb.public.regions_calc_stats_last_week
"""