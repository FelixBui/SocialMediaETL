from dwh.adv2.adv2_base import Adv2Base


class Adv2SeekerResumeExperience(Adv2Base):
    def __init__(self, config):
        super(Adv2SeekerResumeExperience, self).__init__(config)
        self.columns = ['id', 'channel_code', 'resume_id', 'seeker_id', 'salary', 'salary_unit', 'achieved',
                        'qtty_year_exp', 'is_current_work', 'start_date', 'end_date', 'start_text', 'end_text',
                        'company_name', 'position', 'description', 'created_at', 'created_by', 'updated_at',
                        'updated_by', 'created_source', 'update_ts']
        self.fix_dts = ['start_date', 'end_date']
        self.cast_dt = ['created_at', 'updated_at', 'update_ts']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])

    def execute(self):
        self.full()
