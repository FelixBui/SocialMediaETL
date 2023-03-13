from dwh.adv2.adv2_base import Adv2Base


class VtnEmailMarketingLog(Adv2Base):
    def __init__(self, config):
        super(VtnEmailMarketingLog, self).__init__(config)
        self.columns = [
            'id', 'campaign_id', 'seeker_id', 'email', 'campaign',
            'created_at', 'status', 'source', 'total_job']

        self.cast_dt = ['created_at', ]

        self.mysql_conf = self.get_param_config(['db', 'vtn'])
        self.column_seed = self.get_param_config(["column_seed"])
        self.time_col = self.get_param_config(["time_col"])
