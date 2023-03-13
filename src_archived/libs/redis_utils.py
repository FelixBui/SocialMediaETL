#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
* Project: seiryu
* Author: thucpk
* Created: 2021/09/26
"""

from redis.sentinel import Sentinel


class RedisUtils(object):
    def __init__(self, config: dict):
        sentinel_password = config['sentinel_password']
        redis_password = config['redis_password']
        sentinel_hosts = config['sentinel_hosts']
        master_set = config['master_set']
        decode_responses = config.get('decode_responses', False)

        self.sentinel = Sentinel(
            sentinel_hosts, socket_timeout=0.5, sentinel_kwargs={'password': sentinel_password})
        self.master = self.sentinel.master_for(
            master_set, socket_timeout=0.1, password=redis_password, decode_responses=decode_responses)
        self.slave = self.sentinel.slave_for(
            master_set, socket_timeout=0.1, password=redis_password, decode_responses=decode_responses)

    def set_key(self, k, v):
        self.master.set(k, v)

    def get_key(self, k):
        return self.slave.get(k)
