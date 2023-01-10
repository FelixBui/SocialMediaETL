import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# import socket

# from stem import Signal as StemSignal
# from stem.control import Controller as StemController
#
#
# def change_tor_identity(tor_conf):
#     ip = socket.gethostbyname(tor_conf['host'])
#     print(ip)
#     with StemController.from_port(address=ip, port=tor_conf['port']) as controller:
#         controller.authenticate(password=tor_conf['password'])
#         controller.signal(StemSignal.NEWNYM)


def get_dict_proxy(tor_conf):
    http_proxy = "http://{}:{}".format(tor_conf['host'], tor_conf['http_port'])
    https_proxy = "http://{}:{}".format(tor_conf['host'], tor_conf['http_port'])
    return {
        "http": http_proxy,
        "https": https_proxy,
    }


def requests_retry_session(
        retries=3,
        backoff_factor=0.2,
        status_forcelist=(500, 502, 504),
        session=None,
):
    """
    :param retries:
    :param backoff_factor:
    :param status_forcelist:
    :param session:
    :return:
    urllib3 will sleep for: {backoff factor} * (2 ^ ({number of total retries} - 1)) seconds.
    """

    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
