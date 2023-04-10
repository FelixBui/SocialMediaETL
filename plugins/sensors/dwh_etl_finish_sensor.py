from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator
from datetime import datetime
 
class DwhEtlFinishSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, 
                
                *args, **kwargs):
        super(DwhEtlFinishSensor, self).__init__(*args, **kwargs)
 
    def poke(self, context):
        current_minute = datetime.now().minute
        if current_minute % 2 != 0:
            self.log.info("Current minute (%s) not is divisible by 2, sensor will retry.", current_minute)
            return False
 
        self.log.info("Current minute (%s) is divisible by 2, sensor finishing.", current_minute)
        return True
