from dwh.adv2.adv2_base import Adv2Base


class UpsertTest(Adv2Base):
    def __init__(self, config):
        super(UpsertTest, self).__init__(config)
        self.columns = ['id', 'seeker_id', 'name', 'created_dt']

        self.fix_dts = [
            'resume_apply_expired', 'approved_at', 'deleted_at', 'refresh_at']

        self.mysql_conf = self.get_param_config(['db'])
        self.column_seed = self.get_param_config(["column_seed"])
        self.time_col = self.get_param_config(["time_col"])
        self.from_date = "2022-06-01"
        self.to_date = "2022-06-07"

    def execute(self):
        self.upsert(self.from_date, self.to_date)



