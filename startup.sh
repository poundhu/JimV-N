#!/usr/bin/env bash
#
# JimV-N
#
# Copyright (C) 2017 JimV <james.iter.cn@gmail.com>
#
# Author: James Iter <james.iter.cn@gmail.com>
#
#  Start up the JimV-N.
#

systemctl start jimvn.service
systemctl status jimvn.service -l
