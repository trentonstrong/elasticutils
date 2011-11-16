import logging
from pyes.urllib3.connectionpool import TimeoutError
from time import sleep

log = logging.getLogger('elasticsearch')

try:
    from statsd import statsd
except ImportError:
    statsd = None
    
def retry_on_timeout(fn, args, max_retry=0, retry_wait=0):    
    tries = 0
    while True:
        try:
            tries += 1
            return fn(*args)
        except TimeoutError as e:
            if statsd:
                statsd.incr('search.timeout.retry%s' % tries)
            log.error("ES query({0}) Attempt: {3} timed out, {1}\r\n=={2}"
                    .format(args,
                        "retrying" if tries <= max_retry else "returning",
                        e, tries
                    ))
            if tries > max_retry:
                raise e
            sleep(retry_wait)
            continue