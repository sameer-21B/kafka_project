from configparser import ConfigParser 

configure = ConfigParser()
configure.read('config.ini')

class GlobalCounter:
    def __init__(self):
        self.current_global_counter_val = configure['Counter_Variable']['processed_row_count']
        self.records_in_a_batch = configure['Batch_Upload_to_DB']['records_in_a_batch']

    def fetch_records(self,car_data):
        return car_data.iloc[list(range(int(self.current_global_counter_val),int(self.current_global_counter_val)+int(self.records_in_a_batch)))]
    
    def set_global_counter_val(self):
        self.current_global_counter_val = str(int(self.current_global_counter_val)+int(self.records_in_a_batch))
        configure['Counter_Variable']['processed_row_count'] = str(int(self.current_global_counter_val))
    
    def write_config_data(self):
        with open('config.ini', 'w') as configfile:
            configure.write(configfile)
    
    def reset_global_counter_val(self):
        self.current_global_counter_val = str(0)
        configure['Counter_Variable']['processed_row_count'] = self.current_global_counter_val
        #need to figure this out
        configure['Batch_Upload_to_DB']['records_in_a_batch'] = self.records_in_a_batch
        self.write_config_data()

