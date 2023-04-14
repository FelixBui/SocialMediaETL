
# if __name__ == '__main__':
#     from datetime import datetime, date, timedelta
#     import pytz
#     import pendulum
#     from dwh.report.revenue_accounting import Revenue_accounting
  
#     vn_zone = pytz.timezone("Asia/Saigon")

#     bq_conf = {
#         "type": "service_account",
#         "project_id": "data-warehouse-sv",
#         "private_key_id": "dbb955380ccd0c7d36a34768942bd1d6d13b40a5",
#         "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDbWNM1icUHu38B\nKnrOeTcHdRgsVxqL3g85S8GqSGrmoJpxp579GuxhQx298enUG+25GYF4BeRb+D8j\nDeBin3BECZSfWoCHlkdraQgOTi9OVXalnNozgQKj4x2MoJSvNSKjRs1fWtgeZNsX\nDcUjEr7vH+b15c032iYnDHCjRrYkkgX9qJJcbQIeY1ch/crMln8VCzyVB9lw7g4O\nPkcPaiMA7t3QHTeidaFkDrQ3Ni9tq+37mGCHhEkbrjmyKcw9cKGRJSwXA/rgNCAo\nsw+SVXp0gko9WuEacqyS/oSuM+0VhdEiRpsj07tbEcIMuDLP2ZLLYIgpNgcN+l7a\nEguWz6nLAgMBAAECggEAaeIS8sEyj2z+/f0XkxwIaF0p8h2J/wKOrsXXEvAqSnI0\nm7CimuOoP7s6XDqu+WkR9ExPpzSQ66SGNdG5A2Kbuvw09kely83LZBorfVxYkaPH\nxmJVIo+gSqST2xKZM4qe0dqXwPQFZ4agDBzlkOy3po7r2r+3tMJlsGvc47R6sGO3\nmHUt7/fy7ICRgj3RX4RtyRryDrHD6Ysa8SiwrFRw2SjdVcgRkqXCHVckbyAJxtX1\nqPBBuWZ4g8kzCi910KVYEZOf5bevtiZo4wKh6d+zcJpbKaBWz0b5kcdrjaS1Winu\nhVc1sgouBi4vJ4cugbCbS+CBbuT+7AIO0H36aqnlwQKBgQD9tdg5Rm/RWycpwh5e\nn2o027AeTNhGKYHtglhBUoXmP0814of8EmSNxP4R1w9lg/mMPWFPO4k4snwB7oHp\nGFGMYpjo9L9iAROItQpcDFYx+3ZZHg+WaMQyYMa6qu4bus01UdHamVVoFUjr747b\nLMsU3P80WXJWbHzpEOTQtW6bwwKBgQDdU5bwl5Nz27WjhyxVHpe95UHpBeWwpH0m\nHJZZd1LD4W1/WWwhWtvbb/3QYoK1Axujnwdusq7zdJu3Gw/i4pyRhEswtJufp2/W\nM8Sgpg+eT5GG/LJQ8blIDT7VqB8tpSx7MWrKXu1IbSR+BrYgoubly/lygGDCEbPh\na/t04QVBWQKBgQDAyNf3lZ1MAS6+HWJEVnA2oCSwsYW5srqmxl/XqyRR9BN1h5/7\nG+LKk0DYP4nUgrcyKEX0FfJMYdu3AAw5GiCFQzqb8bYne1fjjmXD0iABNOoWB7Ci\ngxNqkH0RxObPrV9XP8ftiowKXfoeRddQljXusYs+tSUbpK45z3t/WA4nTwKBgQDV\noG4rcZKgnKwY2EshKbM7VoKkwc1vD4XAeI1ic3sOmJMQ/aYSF9noV5N8ROl2gDZ4\nWvJYld2qHZ6DQXq2+xY2mqPcsicYFgwri7I5ga/HlXOZAGW5HWhCNI62uVzGuQxT\nKGK0TKXpZ3d2sVGv9Ky7l4MBUwqRp3ahmZGPwfZJmQKBgQCwNNSvX3ROqof7KSr4\n/d1tqWxcZD35AbFdxht5gxj7vjpB8BzAL8Mhmd0hCjyRp9eysPdLR96sm5+YU6wC\nKFdtrTNpqaQvDYw8rP6SvGF7QLBrha9QR2l8Qa6hSOc1QgnOKnuA8iXwx8vo628d\nRp+uxih8Vtbh4ijwiBx/2Dux/A==\n-----END PRIVATE KEY-----\n",
#         "client_email": "dp-dev@data-warehouse-sv.iam.gserviceaccount.com",
#         "client_id": "106647749644688116354",
#         "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#         "token_uri": "https://oauth2.googleapis.com/token",
#         "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#         "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/dp-dev%40data-warehouse-sv.iam.gserviceaccount.com"
#     }
#     def parse_dt_to_week(dt):
#         dt = pendulum.parse(dt)
#         week = dt.week_of_year
#         week = week + 1
#         return week
#     def parse_dt_to_month(dt):
#         datee = datetime.datetime.strptime(dt, "%Y-%m-%d")
#         month = datee.month
#         return month
#     def parse_dt_to_year(dt):
#         datee = datetime.datetime.strptime(dt, "%Y-%m-%d")
#         year = datee.year
#         return year
#     today = date.today()
#     yesterday = today - timedelta(days=1)
#     yesterday = yesterday.strftime("%Y-%m-%d")
#     current_woy = parse_dt_to_week(yesterday)
#     current_moy = parse_dt_to_month(yesterday)
#     current_y = parse_dt_to_year(yesterday)
#     yesterday_date = 
#     current_week = 
#     current_month = 
#     curren_year = 
#     rev_accounting_config = {
#         "process_name": "revenue_accounting",
#         "process_type": "dwh",
#         "process_group": "report",
#         "execution_date": datetime.now(),
#         "from_date": datetime(2022, 1, 1, 0, 15, tzinfo=vn_zone),
#         "to_date": datetime(2022, 5, 1, 0, 15, tzinfo=vn_zone),

#         "params":{
#             "telegram":{
#                 "token": "1457683584:AAELr3jin7tkAJn35mDeuF-N7KvhFQmSRgg",
#                 "owner": {
#                     "id": -1001655012345,
#                     "name": "Data"
#                 }
#             },
#             "dp-ops": bq_conf,
#             "bq-ops": bq_conf,
#             "gsheet_id":"1rf2pSaROkDexabR_oEVMOQVVwPH9GA-0LtaD46Y20VQ",
#             "sheets":[
#                 "to_day"
#             ],
#             "report_queries":{
#                 "to_day": yesterday_date,
#                 "current_week": current_week,
#                 "current_month": current_month,
#                 "current_year": curren_year,
#             },
#         }
#     }
  
   
    
#     task = Revenue_accounting(rev_accounting_config)

#     task.execute()
#     # task.backfill()