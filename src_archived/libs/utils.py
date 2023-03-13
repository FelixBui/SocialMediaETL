import json
import copy
from functools import reduce
import tarfile
import os.path
import pathlib
# from bson import json_util
from faker import Faker
import uuid
import io


def deep_get(dictionary, *keys):
    return reduce(lambda d, key: d.get(key, None) if isinstance(d, dict) else None, keys, dictionary)


def deep_get_with_default(dct, lst_keys, default=None, require=False):
    v = deep_get(dct, *lst_keys)
    if v is None:
        if require:
            if default is None:
                raise KeyError(f"key not found, {dct}, {lst_keys}, default {default}, require {require}")
        return default
    return v


def deep_copy(obj):
    return copy.deepcopy(obj)


def make_tarfile(out_path, in_path):
    with tarfile.open(out_path, "w:gz") as tar:
        tar.add(in_path, arcname=os.path.basename(in_path))


def make_datetime_string_date(dt, fmt="%Y-%m-%d"):
    return dt.strftime(fmt)


def make_datetime_string_time(dt, fmt="%H-%M-%S"):
    return dt.strftime(fmt)


def mkdir_dir(in_path, parents=True, exist_ok=True):
    pathlib.Path(in_path).mkdir(parents=parents, exist_ok=exist_ok)


def gen_random_tbl_name():
    return str(uuid.uuid4())


def gen_update_column_names(columns, seed_column):
    list_of_update_cols = [
        "t.{} = s.{}".format(col_i, col_i)
        for i, col_i in enumerate(columns)
    ]
    update_column_names = ",".join(list_of_update_cols)

    return update_column_names


def generate_obj(is_json=False, limit=100):
    fake = Faker()
    i = 0
    while i < limit:
        obj = {
            "name": fake.name(),
            "dob": fake.date_time(),
            "address": fake.address(),
            "email": fake.ascii_safe_email(),
            "city": fake.city()
        }
        if is_json:
            obj = json.dumps(obj, default=json_util.default)

        yield obj
        i += 1

# utm_url -> extract source/ medium/ content/ campaign with get_fields input
def extract_utm_field_from_url(utm_url, get_field):
    try:
        pos = utm_url.find("?")
        if pos > 0 :
            utm_url = utm_url[pos+1:]
        params_list = utm_url.split("&")
        for i,p in enumerate(params_list):
            field, value = p.split("=")
            if field == "utm_" + get_field:
                return value
    except:
        return None


def gen_pandas_gbq_schema(bq_engine, schema):
    f = io.StringIO("")
    bq_engine.schema_to_json(schema, f)
    schema_to_json = json.loads(f.getvalue())

    return schema_to_json