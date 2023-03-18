# Seiryu

The purpose of this repo was to manage ETL projects

## Installation

```bash
pip install -r requirements.txt
mv .env_SAMPLE .env

```

## Directory structure
```
����SocialMediaETL
    ����configs
    ����dags
    �   ����tiktok
    �   ����youtube
    ����data
    �   ����raw
    �   �   ����source_1
    �   �   ����source_2
    �   ����transformed
    �       ����source_1
    �       ����source_2
    ����factories
    ����plugins
    �   ����helpers
    �   ����hooks
    �   ����operators
    �   ����sensors
    ����scripts
    ����test
        ����tiktok
        ����ytb
```

## Usage

```sh
# use direct main.py
python main.py --ptype etl --pgroup centralize --job task_centralize_cb \
  --from_date 2021-01-10 --to_date 2021-01-15
```


## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.