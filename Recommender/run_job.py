import os
# date_list=["2022-01-18","2022-01-19","2022-01-22","2022-01-23","2022-01-24","2022-01-25"]
env='prod'
date_list=["2022-01-22","2022-01-23"]
for running_date in date_list:
    print(f"running date is {running_date}")
    cmd = f"python ./local_main.py --running_date {running_date}  --env {env} "
    os.system(cmd)
