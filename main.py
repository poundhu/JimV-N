#!/usr/bin/env python
# -*- coding: utf-8 -*-


import threading
import traceback
import signal
import daemon
import atexit
import os

import time

from models.initialize import logger, threads_status, config
from models.event_process import EventProcess
from models.event_loop import vir_event_loop_poll_register, vir_event_loop_poll_run, eventLoop
from models import Host
from models import Utils
from models import PidFile


__author__ = 'James Iter'
__date__ = '2017/3/12'
__contact__ = 'james.iter.cn@gmail.com'
__copyright__ = '(c) 2017 by James Iter.'


def main():
    pidfile = PidFile(file_name=config['pidfile'])
    pidfile.create(pid=os.getpid())
    atexit.register(pidfile.unlink)

    threads = []

    signal.signal(signal.SIGTERM, Utils.signal_handle)
    signal.signal(signal.SIGINT, Utils.signal_handle)

    t_ = threading.Thread(
        target=Host().guest_creating_progress_report_engine, args=())
    threads.append(t_)

    t_ = threading.Thread(
        target=Host().guest_state_report_engine, args=())
    threads.append(t_)

    t_ = threading.Thread(target=Host().instruction_process_engine, args=())
    threads.append(t_)

    t_ = threading.Thread(target=Host().host_state_report_engine, args=())
    threads.append(t_)

    t_ = threading.Thread(target=Host().guest_performance_collection_engine, args=())
    threads.append(t_)

    t_ = threading.Thread(target=Host().host_performance_collection_engine, args=())
    threads.append(t_)

    vir_event_loop_poll_register()
    t_ = threading.Thread(target=vir_event_loop_poll_run, name="libvirtEventLoop")
    threads.append(t_)

    for t in threads:
        t.setDaemon(True)
        t.start()

    i = 0
    while not eventLoop.runningPoll and i <= 10:
        """
        避免在 timer 还没启动的时候，就去注册事件。那样会抛出如下异常：
        libvirtError: internal error: could not initialize domain event timer
        """
        i += 1
        time.sleep(1)

    EventProcess.guest_event_register()

    while True:
        if Utils.exit_flag:
            # 主线程即将结束
            EventProcess.guest_event_deregister()
            break

        if config['DEBUG']:
            print threads_status

        time.sleep(1)

    # 等待子线程结束
    for t in threads:
        t.join()

    msg = 'Main say bye-bye!'
    print msg
    logger.info(msg=msg)


if __name__ == '__main__':

    # noinspection PyBroadException
    try:

        if config['daemon']:
            with daemon.DaemonContext(files_preserve=[logger.handlers[0].stream.fileno()]):
                main()

        else:
            main()

    except:
        logger.error(traceback.format_exc())
        exit(-1)

