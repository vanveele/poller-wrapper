from socket import gethostbyname, gethostbyaddr

from celery.utils.log import get_task_logger
from sh import sudo, Command, ErrorReturnCode

from framework.celery.celery import observium


sudo_observium = sudo.bake([
    '-u',
    'observium',
    '-n',
    '--'])
task_log = get_task_logger(__name__)


def resolve(host):
    """
    Returns primary A record if host resolves through DNS
    @param host: str
    @return host: str
    """
    try:
        ip = gethostbyname(host)
        host = gethostbyaddr(ip)
    except:
        task_log.error("Cannot find host: {}".format(host))
        raise
    return host


@observium.task(time_limit=300, ignore_result=False)
def discover(host):
    """
    Run observium device discovery and return exit code
    @param host:
    @return exit_code:
    """
    cmd = Command('/opt/observium/discovery.php')
    result = None
    task_log.debug("Running command {}".format(cmd))
    try:
        with sudo_observium:
            task_log.debug("Entered sudo")
            result = cmd(h=host, _err='/dev/null', _out='/dev/null')
    except ErrorReturnCode as e:
        task_log.error("Command failed for host {}: {}".format(host, e))
    return result.exit_code


@observium.task(time_limit=300, ignore_result=False)
def poll(host):
    """
    Run observium device poll and return exit code
    @param host:
    @return exit_code:
    """
    cmd = Command('/opt/observium/poller.php')
    result = None
    task_log.debug("Running command {}".format(cmd))
    try:
        with sudo_observium:
            task_log.debug("Entered sudo")
            result = cmd(h=host, _err='/dev/null', _out='/dev/null')
    except ErrorReturnCode as e:
        task_log.error("Command failed for host {}: {}".format(host, e))
    return result.exit_code
