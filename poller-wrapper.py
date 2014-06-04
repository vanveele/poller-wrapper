#!/usr/bin/env python

import argparse
import logging as log
from contextlib import closing
import socket

import MySQLdb as MySQL
from re import sub
from os import path, access, R_OK
from celery import group, exceptions
from celery.result import AsyncResult

import framework.observium.tasks as observium_tasks


class ObserviumDB:
    _db = None

    def __init__(self,
                 dbhost='localhost',
                 dbname='observium',
                 dbuser='observium',
                 default_file=path.expanduser('~/.my.cnf')):
        try:
            self._db = MySQL.connect(host=dbhost,
                                     user=dbuser,
                                     read_default_file=default_file,
                                     db=dbname)
        except Exception as e:
            raise Exception("Unable to connect to DB: {}".format(e))

    def __del__(self):
        if self._db is not None:
            self._db.close()

    @property
    def fetch_all_hosts_by_poll_time(self):
        """Return all known host IDs from observium DB sorted slowest first."""
        query = """
              SELECT device_id
              FROM devices
              WHERE disabled != '1' """
        order = " ORDER BY last_polled_timetaken DESC"
        with closing(self._db.cursor(MySQL.cursors.DictCursor)) as cur:
            try:
                cur.execute(query + order)
            except Exception as e:
                log.warning("Fetch failed: {}".format(e))
                return
            devices = [device['device_id'] for device in cur.fetchall()]
        return devices

    def fetch_host_id(self, hostname):
        """Return Host ID using Host Name"""
        query = """
              SELECT device_id
              FROM devices
              WHERE disabled != '1'
              AND hostname LIKE %s """
        with closing(self._db.cursor(MySQL.cursors.DictCursor)) as cur:
            try:
                cur.execute(query, hostname)
            except Exception as e:
                print("Fetch failed: {}".format(e))
                return
            # Only use first result
            hostid = cur.fetchone()
        assert isinstance(hostid, long)
        return hostid

    def fetch_host_name(self, hostid):
        """Return Host Name using Host ID"""
        query = """
              SELECT hostname
              FROM devices
              WHERE device_id = %s """
        with closing(self._db.cursor(MySQL.cursors.DictCursor)) as cur:
            try:
                cur.execute(query, hostid)
            except Exception as e:
                print("Fetch failed: {}".format(e))
                return
            # Only use first result
            hostname = sub('\.pdtpartners\.com', '', cur.fetchone()['hostname'])
            log.debug("fetchHostName result is {}".format(hostname))

        assert isinstance(hostname, basestring)
        return hostname


def _get_args():
    """
    Parse arguments from command line
    @return ArgumentParser: parsed argument object
    """
    p = argparse.ArgumentParser(description='Distributed poller/discovery util for observium',
                                prog='poller-wrapper')
    p.add_argument(
        '-v',
        '--verbose',
        dest='verbose',
        action='store_true',
        help='Enables verbose output'
    )
    p.add_argument(
        '-f',
        '--defaultfile',
        type=str,
        metavar='FILE',
        help='MySQL client default file',
        required=False,
        default='~/.my.cnf'
    )
    p.add_argument(
        '-d',
        '--dbhost',
        type=str,
        metavar='HOST',
        help='MySQL server hostname',
        required=True
    )
    p.add_argument(
        '-o',
        '--operation',
        type=str,
        choices=['discover', 'poll'],
        required=True
    )
    p.add_argument(
        '--version',
        action='version',
        version='%(prog)s 0.1'
    )
    return p.parse_args()


def _check_file_access(fn, access_mode):
    """
    Ensure that file is accessible
    @param fn: filename
    @param access_mode:
    @return boolean:
    """
    if path.isfile(fn) and access(fn, access_mode):
        return True
    else:
        log.debug('File {} is not readable.'.format(fn))
        return False


def _setup_logging(log_level='info'):
    """
    Setup verbosity of output
    @param log_level: verbose or something else
    """
    if log_level == 'verbose':
        log.basicConfig(format="%(levelname)s: %(message)s", level=log.DEBUG)
    else:
        log.basicConfig(format="%(levelname)s: %(message)s", level=log.INFO)
    log.debug("Logging set to {}".format(log_level))


def _select_operation(name):
    """
    Select the operation to apply in the task
    @param name: either 'poll' or 'discover'
    """
    if name == 'poll':
        return observium_tasks.poll
    elif name == 'discover':
        return observium_tasks.discover
    else:
        raise AttributeError("Undefined operation: {}".format(name))


def _submit_task(hostlist, operation):
    """
    Submit a taskset for all hosts in hostlist
    @param hostlist: list of Host IDs for task
    @param operation: operation to apply to hostlist
    @return GroupResult:
    """
    log.debug('Task will run on {}'.format(hostlist))
    ts = group(operation.subtask((h,)) for h in hostlist)
    try:
        run = ts.apply_async()
    except:
        raise Exception('Task submit failed')
    return run


def _print_task_stat(task_id, value):
    """
    Placeholder for group callback
    @param task_id:
    @param value:
    @return AsyncResult:
    """
    h = AsyncResult(task_id)
    if h.successful() and value is not None:
        log.info('Hostid {} completed successfully.'.format(task_id))
    return h


def _collect_results(taskset):
    """
    Handle results collection
    @param taskset: instance for results caching backend
    @return:
    """
    log.info('Waiting for tasks to complete')
    total_tasks = len(taskset)
    try:
        if taskset.supports_native_join:
            taskset.join_native(timeout=500, callback=_print_task_stat)
        else:
            taskset.join(timeout=500, callback=_print_task_stat)
    except (exceptions.TimeoutError, socket.timeout):
        log.info('Continuing due to timeout.')

    log.info('{} tasks submitted and {} tasks completed'.format(total_tasks, taskset.completed_count()))
    return


def main():
    """
    poller-wrapper will query Observium DB for all current SNMP targets
    and then proceed to asynchronously poll / discover using all available
    celery workers. Long running updates will encounter a timeout.
    """
    args = _get_args()
    observium = None

    if args.verbose is True:
        _setup_logging('verbose')
    else:
        _setup_logging()

    args.defaultfile = path.expanduser(args.defaultfile)

    if not _check_file_access(args.defaultfile, R_OK):
        exit(2)

    try:
        observium = ObserviumDB(
            dbhost=args.dbhost,
            default_file=args.defaultfile
        )
    except AttributeError as e:
        log.error("Cannot connect to Observium: {}".format(e))
        exit(2)

    operation = _select_operation(args.operation)
    result = _submit_task(observium.fetch_all_hosts_by_poll_time, operation)

    _collect_results(result)

    exit(0)


if __name__ == '__main__':
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        raise
