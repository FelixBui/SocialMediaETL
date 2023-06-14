from datetime import datetime

def datetime_now():
    now = datetime.now()
    day = now.day
    month = now.month
    year = now.year
    return f"{year}-{month}-{day}"