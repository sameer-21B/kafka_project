import configuration
import dbutils
import importlib
importlib.reload(configuration)
importlib.reload(dbutils)
from configuration import GlobalCounter as gc
from dbutils import Dbutils as db
import pandas as pd

car_data=pd.read_csv('./cardekho_dataset.csv',index_col=0)
car_data['Idx']=range(1,len(car_data)+1)
car_data=car_data[['Idx']+list(car_data.columns)[:-1]]

gc_obj=gc()
db_obj=db()

while (int(gc_obj.current_global_counter_val)+int(gc_obj.records_in_a_batch)>len(car_data)):
    print("Current value of Global Counter is {}".format(gc_obj.current_global_counter_val))
    batch_of_data = gc_obj.fetch_records(car_data)
    print("Incrementing Global counter value by {}".format(gc_obj.records_in_a_batch))
    gc_obj.set_global_counter_val()
    print("Commiting config data changes.")
    gc_obj.write_config_data()



gc_obj.reset_global_counter_val()