from dwh.adv2.adv2_base import Adv2Base


class Adv2SeekerSeekerEmailMarketingLog(Adv2Base):
    def __init__(self, config):
        super(Adv2SeekerSeekerEmailMarketingLog, self).__init__(config)
        self.columns = [
            'id', 'seeker_id', 'created_at', 'campaign_id', 'seeker_email_marketing_id', 'count_job',
            'is_send_mail', 'status', 'channel_code', 'update_ts', 'job_ids', 'type', 'reason']
        # self.fix_dts = ['created_at', 'update_ts']
        self.mysql_conf = self.get_param_config(['db', 'adv2_main'])
        self.column_seed = self.get_param_config(["column_seed"])
        self.time_col = self.get_param_config(["time_col"])
