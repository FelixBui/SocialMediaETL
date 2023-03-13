from dwh.adv2.adv2_base import Adv2Base


class VtnEmployerResumePoint(Adv2Base):
    def __init__(self, config):
        super(VtnEmployerResumePoint, self).__init__(config)
        self.columns = [
            'id', 'employer_id', 'resume_id', 'created_at', 'data', 'point', 'updated_at',
            'package', 'ip', 'is_change_title', 'employer_point_detail_id']

        self.fix_dts = ['created_at', 'updated_at']

        self.mysql_conf = self.get_param_config(['db', 'vtn'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
