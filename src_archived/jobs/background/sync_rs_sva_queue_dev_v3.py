from dateutil import tz
import time
import pymongo
import datetime
from core.app_base import AppBase
from bson.json_util import dumps as bson_dumps
from bson.objectid import ObjectId


local_tz = tz.tzlocal()
utc_tz = tz.tzutc()


class SyncRsSvaQueueDevV3(AppBase):
    def __init__(self, config):
        super().__init__(config)
        mongo_ubl_uri = self.get_param_config(['mongo_ubl_uri'])
        mongo_dp_docdb_uri = self.get_param_config(['dp_docdb_uri'])
        self.ubl_conn = pymongo.MongoClient(mongo_ubl_uri)
        self.dp_docdb_conn = pymongo.MongoClient(mongo_dp_docdb_uri)
        self.interval_check = 600  # 10 minutes
        self.queue_size = 5
        self._next_check = time.time() + self.interval_check

    @staticmethod
    def parse_apply_job(x):
        channel_code = x['trackingId'].split('-')[1]
        _id = x['_id']
        exp_session_id = x['raw']['subject']['expSessionId']
        subject_id = x['raw']['subject']['id']
        subject_type = x['raw']['subject']['type']
        object_id = x['raw']['object']['id']
        verb = x['raw']['verb']
        job_id = int(object_id.split('-')[-1])
        created_at = datetime.datetime.fromtimestamp(
            x['created_at_ts']).replace(tzinfo=local_tz).astimezone(utc_tz)
        return {
            "channel_code": channel_code,
            "_id": _id,
            "exp_session_id": exp_session_id,
            "subject_id": subject_id,
            "subject_type": subject_type,
            "verb": verb,
            "job_id": job_id,
            "created_at": created_at
        }

    @staticmethod
    def parse_view_job(x):
        if x['trackingId'] == 'SV-2021':
            channel_code = 'SV'
        else:
            channel_code = x['trackingId'].split('-')[1]

        _id = x['_id']
        exp_session_id = x['raw']['subject']['expSessionId']
        subject_id = x['raw']['subject']['id']
        subject_type = x['raw']['subject']['type']
        job_id = x['raw']['object']['id']
        verb = x['raw']['verb']
        created_at = datetime.datetime.fromtimestamp(
            x['created_at_ts']).replace(tzinfo=local_tz).astimezone(utc_tz)
        return {
            "channel_code": channel_code,
            "_id": _id,
            "exp_session_id": exp_session_id,
            "subject_id": subject_id,
            "subject_type": subject_type,
            "verb": verb,
            "job_id": job_id,
            "created_at": created_at
        }

    @AppBase.wrapper_simple_log
    def execute(self):
        ubl_db = self.ubl_conn['tracking_tool']
        recsys_db = self.dp_docdb_conn['recsys']

        while True:
            try:
                time_id = recsys_db['ubl_view_apply_raw'].find_one({}, sort=[("_id", -1)])['_id'].generation_time
            except TypeError:
                time_id = datetime.datetime.now().replace(tzinfo=local_tz) - datetime.timedelta(days=1)
            checkpoint_id = ObjectId.from_datetime(time_id - datetime.timedelta(seconds=3))

            it_raw = ubl_db['log'].find(
                {
                    "_id": {"$gt": checkpoint_id},
                    "raw.verb": {
                        "$in": ["view_job", "apply_job"]
                    },
                    "raw.subject.expSessionId": {"$exists": True},
                },
                [
                    'trackingId', 'raw.subject.id', 'raw.subject.type',
                    'raw.subject.expSessionId', 'raw.verb',
                    'raw.object.id', 'raw.object.type', 'created_at_ts'
                ]
            ).sort([("_id", pymongo.ASCENDING)])

            for e in it_raw:
                try:
                    if e['raw']['verb'] == 'view_job':
                        x = self.parse_view_job(e)
                    else:
                        x = self.parse_apply_job(e)
                    if recsys_db['ubl_view_apply_raw'].find_one({"_id": x['_id']}) is None:
                        recsys_db['ubl_view_apply_raw'].update_one(
                            {"_id": x['_id']},
                            {"$set": x}, upsert=True)
                    else:
                        continue
                except Exception as ex:
                    self.log.error("""
                    {} 
                    {}
                    """.format(
                        ex,
                        bson_dumps(e)
                    ))
                    continue
                channel_code = x['channel_code']
                subject_id = x['subject_id']
                subject_type = x['subject_type']
                exp_session_id = x['exp_session_id']
                verb = x['verb']
                job_id = x['job_id']
                updated_at = x['created_at']
                if verb == 'view_job':
                    recsys_db['session_queue'].update_one({
                            "channel_code": channel_code,
                            "exp_session_id": exp_session_id
                        }, {
                            "$set": {
                                "exp_session_id": exp_session_id,
                                "subject_id": subject_id,
                                "subject_type": subject_type,
                                "updated_at": updated_at
                            },
                            "$push": {
                                "jobs": {
                                    "$each": [{"job_id": job_id, "is_apply": False, "updated_at": updated_at}],
                                    "$slice": - self.queue_size,
                                }
                            }
                        }, upsert=True
                    )
                    if subject_id is not None:
                        recsys_db['user_queue'].update_one({
                            "subject_id": subject_id
                        }, {
                            "$set": {
                                "channel_code": channel_code,
                                "exp_session_id": exp_session_id,
                                "subject_id": subject_id,
                                "subject_type": subject_type,
                                "updated_at": updated_at
                            },
                            "$push": {
                                "jobs": {
                                    "$each": [{"job_id": job_id, "is_apply": False, "updated_at": updated_at}],
                                    "$slice": - self.queue_size,
                                }
                            }
                        }, upsert=True
                        )
                else:
                    recsys_db['session_queue'].update_one({
                            "channel_code": channel_code,
                            "exp_session_id": exp_session_id,
                            "jobs": {"$elemMatch": {"job_id": job_id}}
                        }, {
                            "$set": {
                                "jobs.$.is_apply": True,
                                "updated_at": updated_at
                            }
                        }
                    )

                    if subject_id is not None:
                        recsys_db['user_queue'].update_one(
                            {
                                "subject_id": subject_id,
                                "jobs": {"$elemMatch": {"job_id": job_id}}
                            },
                            {
                                "$set": {
                                    "jobs.$.is_apply": True,
                                    "updated_at": updated_at
                                }
                            }
                        )

                _current = time.time()
                if _current > self._next_check:
                    _now = datetime.datetime.now().replace(tzinfo=local_tz)
                    self.log.info(
                        "current at {0}, ubl at {1}, latency: {2} seconds".format(
                            _now,
                            x['created_at'],
                            (_now - x["created_at"]).total_seconds()
                        )
                    )
                    self._next_check = _current + self.interval_check

            time.sleep(0.001)
