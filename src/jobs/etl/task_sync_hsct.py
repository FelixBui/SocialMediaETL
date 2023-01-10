import datetime
from dateutil import tz
import tqdm
from bson.objectid import ObjectId
from core.app_base import AppBase
from libs.storage_utils import yield_df_load_from_mongo, insert_df_to_postgres, load_df_from_postgres

local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class TaskSyncHsct(AppBase):

    def __init__(self, config):
        super(TaskSyncHsct, self).__init__(config)
        self.postgres_conf = self.get_param_config(['db', 'db_data_services'])
        self.mongo_conf = self.get_param_config(['db', 'mongo'])
        self.from_date = self.get_process_info(['from_date'])
        self.to_date = self.get_process_info(['to_date'])

    def load_it(self, from_time, to_time):
        from_id = ObjectId.from_datetime(from_time)
        to_id = ObjectId.from_datetime(to_time)
        query = {"_id": {"$gte": from_id, "$lt": to_id}}
        it_raw = yield_df_load_from_mongo(
            self.mongo_conf, "hsct_company_info", query, no_id=False, selected_keys=None, batch_size=10000)
        return it_raw

    def load_company_profile(self, it_raw):
        with tqdm.tqdm() as pbar:
            for df_raw in it_raw:
                df_raw['created_at'] = df_raw['_id'].map(lambda x: x.generation_time.astimezone(local_tz))
                cols = ["id", "tax_code", "company_name", "primary_industry_id", "international_name", "short_name",
                        "tax_address", "legal_representative", "primary_industry",
                        "business_group_ids", "business_group_names", "email", "phone", "status",
                        "tax_created_at", "updated_at", "created_at", "url", "website"]
                for icol in cols:
                    if icol not in df_raw.columns:
                        df_raw[icol] = None
                df_raw = df_raw[cols]
                df_raw['updated_at'] = datetime.datetime.now(local_tz)
                df_raw.dropna(subset=['tax_code'], inplace=True)
                insert_df_to_postgres(
                    self.postgres_conf,
                    tbl_name="company_profile",
                    df=df_raw.drop_duplicates(['tax_code']),
                    primary_keys=['tax_code']
                )
                pbar.update(df_raw.shape[0])

    def update_company_profile_final(self):
        q = """
             with tbl_process as
              (select t.id,
                      t.tax_code,
                      t.primary_industry_id,
                      t.company_name,
                      t.international_name,
                      t.short_name,
                      t.tax_address,
                      t.business_group_ids,
                      t.business_group_names,
                      t.email,
                      t.phone,
                      UPPER(case
                                when s.status is null then t.status
                                else s.status
                            end) status,
                      tax_created_at,
                      case
                          when s.tax_updated_at is null then t.updated_at
                          else s.tax_updated_at
                      end updated_at,
                      t.created_at,
                      url,
                      website,
                      legal_representative
              from public.company_profile t
              left join public.official_company_status s on t.tax_code = s.tax_code
               where (t.updated_at >= '{0}'
                      and t.updated_at < '{1}')
                 OR (s.tax_updated_at >= '{0}'
                     and s.tax_updated_at < '{1}')
               order by tax_code
              )
              select e.*,
                   case
                       when status like '%GIẢI THỂ%'
                            or status like '%PHÁ SẢN%'
                            or status like '%NGỪNG HOẠT ĐỘNG%'
                            or status like '%ĐÓNG MÃ SỐ THUẾ%'
                            or status like '%XÓA TRÙNG%'
                            or status like '%THU HỒI%' then 'NGỪNG HOẠT ĐỘNG'
                       when status like '%ĐANG HOẠT ĐỘNG%'
                            or status like '%THÔNG BÁO THAY ĐỔI%'
                            or status like '%CHUYỂN ĐỔI LOẠI HÌNH DOANH NGHIỆP%'
                            or status like '%ĐĂNG KÝ MỚI%'
                            or status like '%THAY ĐỔI NỘI DUNG ĐKDN%'
                            or status like '%THÔNG BÁO HIỆU ĐÍNH%'
                            or status like '%ĐĂNG KÝ MỚI%' then 'ĐANG HOẠT ĐỘNG'
                       when status like '%TẠM%' then 'TẠM DỪNG'
                       else 'KHÔNG XÁC ĐỊNH'
                   end as status_group,
                   case
                       when status like '%GIẢI THỂ%'
                            or status like '%PHÁ SẢN%'
                            or status like '%NGỪNG HOẠT ĐỘNG%'
                            or status like '%ĐÓNG MÃ SỐ THUẾ%'
                            or status like '%XÓA TRÙNG%'
                            or status like '%THU HỒI%' then 3 --'NGỪNG HOẠT ĐỘNG'
            
                       when status like '%ĐANG HOẠT ĐỘNG%'
                            or status like '%THÔNG BÁO THAY ĐỔI%'
                            or status like '%CHUYỂN ĐỔI LOẠI HÌNH DOANH NGHIỆP%'
                            or status like '%ĐĂNG KÝ MỚI%'
                            or status like '%THAY ĐỔI NỘI DUNG ĐKDN%'
                            or status like '%THÔNG BÁO HIỆU ĐÍNH%'
                            or status like '%ĐĂNG KÝ MỚI%' then 1 --'ĐANG HOẠT ĐỘNG'
            
                       when status like '%TẠM%' then 2 --'TẠM DỪNG'
            
                       else 4 --'KHÔNG XÁC ĐỊNH'
            
                   end as status_group_id
              from tbl_process e
        """.format(self.from_date, self.to_date)
        self.log.info("query data updated")
        df_update = load_df_from_postgres(self.postgres_conf, q)
        self.log.info("load updated {}".format(df_update.shape))
        insert_df_to_postgres(
            self.postgres_conf,
            tbl_name="company_profile_final",
            df=df_update,
            primary_keys=['tax_code']
        )

    def execute(self):
        self.log.info("step 1: extract")
        it_raw = self.load_it(self.from_date, self.to_date)
        self.log.info("step 2: load")
        self.load_company_profile(it_raw)
        self.log.info("step 3: load company profile final")
        self.update_company_profile_final()
