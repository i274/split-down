# Copyright 2014 Jaewon An

import datetime
import functools
import futures
import getpass
import gflags
import logging
import os
import paramiko
import sys
import time
import threading
import tornado.ioloop
import tornado.gen
import gzip
import StringIO

gflags.DEFINE_integer("connect_timeout", 30, "SSH connect timeout.")
gflags.DEFINE_integer("block_timeout", 60, "Block download timeout.")
gflags.DEFINE_integer("max_attempt", 5, "Max attempt to reconnect.")
gflags.DEFINE_integer("blocksize", 128, "Block size in KByte.")
gflags.DEFINE_integer("connection_per_host", 1, "Number of connections per host.")
gflags.DEFINE_boolean("verbose", True, "Verbose mode.")
gflags.DEFINE_string("address", "", "Address list separated by comma.")
gflags.DEFINE_boolean("compression", False, "Compression by gzip.")

FLAGS = gflags.FLAGS
log = logging.getLogger("split-down")

class WrongAddressException(Exception):
  pass

class NoAvailConnectionException(Exception):
  pass

class ConnectionPool(object):
  def __init__(self, \
               addresslist, \
               user, \
               threadpool, \
               io_loop=tornado.ioloop.IOLoop.current(), \
               connect_timeout=30, \
               max_attempt=5):
    self._addresslist = addresslist
    self._user = user
    self._io_loop = io_loop
    self._connect_timeout = connect_timeout
    self._threadpool = threadpool
    self._avail_conns = []
    self._num_possible_connections = len(addresslist)
    self._waitings = []
    self._max_attempt = max_attempt

    for i in range(0, len(addresslist)):
      address = addresslist[i]
      timeout = datetime.timedelta(seconds=i)
      self._io_loop.add_timeout(timeout, lambda: self._try_connect(address, 0))

  def _get_address_pair(self, address):
    try:
      parts = address.split(":")
      port = 22
      if len(parts) == 2:
        port = int(parts[1])
      return parts[0], port
    except:
      raise WrongAddressException()

  def _connect(self, address, user, timeout):
    address, port = self._get_address_pair(address)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(address, port=port, username=user[0], password=user[1], \
                   timeout=timeout)
    client.address = address
    return client

  @tornado.gen.coroutine
  def _try_connect(self, address, attempt):
    max_wait_time = 30
    wait_time = 1
    while attempt < self._max_attempt:
      attempt = attempt + 1
      try:
        conn = yield self._threadpool.submit(self._connect, address, \
                                             self._user, \
                                             self._connect_timeout)
        log.info("<%s>: Connected." % address)
        conn.returned = False
        conn.attempt = attempt
        self.return_connection(conn)
        return

      except paramiko.AuthenticationException:
        log.warning("<%s>: Authentication failed." % address)
        break

      except WrongAddressException:
        log.warning("<%s>: Wrong address." % address)
        break

      except Exception as e:
        log.warning("<%s>: %s." % (address, str(e)))

      log.warning("<%s>: Connect failed. Retrying after %d second(s)." \
                  % (address, wait_time))
      time.sleep(wait_time)
      wait_time = min(max_wait_time, wait_time * 2)

    self._abandon_connection(address)

  def _abandon_connection(self, address):
    log.info("<%s>: Will not use this address anymore." % address)
    self._num_possible_connections = self._num_possible_connections - 1
    if self._num_possible_connections == 0:
      while len(self._waitings) > 0:
        f = self._waitings.pop(0)
        if f.set_running_or_notify_cancel():
          f.set_exception(NoAvailConnectionException())
          return

  def _get_connection(self):
    if len(self._avail_conns) > 0:
      conn = self._avail_conns.pop(0)
      conn.returned = False
      return conn
    else:
      return None

  @property
  def num_max_connections(self):
    return len(self._addresslist)

  def get_connection(self):
    f = futures.Future()
    conn = self._get_connection()
    if conn:
      f.set_result(conn)
    else:
      if self._num_possible_connections > 0:
        self._waitings.append(f)
      else:
        f.set_exception(NoAvailConnectionException())
    return f

  def return_connection(self, conn):
    if conn.returned:
      return
    conn.returned = True
    self._avail_conns.append(conn)
    while len(self._waitings) > 0:
      f = self._waitings.pop(0)
      if f.set_running_or_notify_cancel():
        f.set_result(self._get_connection())
        return

  def report_error(self, conn):
    conn.returned = True
    try:
      conn.close()
    except:
      # Do nothing.
      pass
    log.warning("Error on <%s>. reconnecting." % conn.address)
    self.io_loop.add_callback(self._try_connect, conn.address, conn.attempt)


class SplitDownloader(object):
  index_file_postfix = ".idx"
  partial_file_postfix = ".partial"
  chunk_size = 4096

  def __init__(self, source, target, blocksize, \
               connection_pool, \
               threadpool, \
               io_loop=tornado.ioloop.IOLoop.current(),
               block_timeout=60,
               compression=False,
               resume=True):
    self._source = source
    self._target = target
    self._blocksize = blocksize
    self._connection_pool = connection_pool
    self._threadpool = threadpool
    self._io_loop = io_loop
    self._block_timeout = block_timeout
    self._resume = True
    self._compression = compression
    self._chunk_handlers = []

    self._target_file = open(target, "wb")
    self._in_queue = False

  def _get_filesize(self, conn):
    command = "find '%s' -printf '%%s'" % self._source
    stdin, stdout, stderr = \
      conn.exec_command(command, timeout=self._block_timeout)
    if stderr.read():
      raise ValueError()
    return int(stdout.read())

  @tornado.gen.coroutine
  def download(self):
    # Gets filesize.
    try:
      conn = yield self._connection_pool.get_connection()
    except Exception as e:
      log.error("No available connection.")
      raise

    try:
      self._filesize = self._get_filesize(conn)
    except ValueError as e:
      raise e
    except Exception as e:
      self._connection_pool.report_error(conn)
      raise e
    finally:
      self._connection_pool.return_connection(conn)

    log.info("Downloading %s (%f MBytes.)" \
             % (self._source, self._filesize / 1000000.))

    # Makes queue.
    self._num_blocks = (self._filesize + (self._blocksize - 1)) / self._blocksize
    self._queue = range(0, self._num_blocks)
    self._queue.reverse()

    self._downloaded_blocks = 0
    self._start_time = datetime.datetime.now()
    self._done = futures.Future()

    yield self._proceed_queue()
    yield self._done

  @tornado.gen.coroutine
  def _proceed_queue(self):
    if self._in_queue:
      return
    self._in_queue = True
    try:
      while len(self._queue) > 0:
        block = self._queue.pop(len(self._queue) - 1)
        conn = yield self._connection_pool.get_connection()
        self._io_loop.add_future( \
          self._threadpool.submit(self._download_block, block, conn), \
          functools.partial(self._on_block_downloaded, block, conn))
    except:
      raise
    finally:
      self._in_queue = False

  def add_chunk_handler(self, handler):
    self._chunk_handlers.append(handler)

  def _download_chunks(self, channel):
    data = ""
    while True:
      recv = channel.read(self.chunk_size)
      if not recv:
        break
      for handler in self._chunk_handlers:
        handler(len(recv))
      data = data + recv
    return data

  def _download_block(self, block, conn):
    try:
      offset = block * self._blocksize
      command = "xxd -p -s %d -l %d '%s' | xxd -r -p" \
                % (offset, self._blocksize, self._source)
      if self._compression:
        command = command + " | gzip --best"
      stdin, stdout, stderr = \
        conn.exec_command(command, timeout=self._block_timeout)
      data = self._download_chunks(stdout)
      err = self._download_chunks(stderr)
      if err:
        log.error("<%s>: server error - %s" % (conn.address, err))
        return None
      if self._compression:
        fileobj = StringIO.StringIO(data)
        data = gzip.GzipFile(fileobj=fileobj).read()
      return data
    except Exception as e:
      print e
      return None

  @tornado.gen.coroutine
  def _on_block_downloaded(self, block, conn, future):
    data = future.result()
    if data:
      offset = block * self._blocksize
      self._target_file.seek(offset)
      self._target_file.write(data)
      self._downloaded_blocks = self._downloaded_blocks + 1
      self._connection_pool.return_connection(conn)

      if self._downloaded_blocks == self._num_blocks:
        self._target_file.close()
        self._elapsed = datetime.datetime.now() - self._start_time
        print "Download complete."
        self._mbps = self._filesize / self._elapsed.total_seconds() / 1000000.
        print "Average speed: %f Mb/sec." % self._mbps
        self._done.set_result(None)

    else:
      self._connection_pool.report_error(conn)
      self._queue.append(block)
      yield self._proceed_queue()


class BandwidthMeasurer(object):
  def __init__(self, callback, stat_callback_period, \
               io_loop=tornado.ioloop.IOLoop.current()):
    self._lock = threading.Lock()
    self._downloaded = 0
    self._callback = callback
    self._periodic_cb = \
      tornado.ioloop.PeriodicCallback(self._stat, stat_callback_period, io_loop)

  def start(self):
    self._periodic_cb.start()

  def stop(self):
    self._periodic_cb.stop()

  def handle_chunk_downloaded(self, chunk_size):
    with self._lock:
      self._downloaded = self._downloaded + chunk_size

  def _stat(self):
    with self._lock:
      downloaded, self._downloaded = self._downloaded, 0
    self._callback(downloaded)


def unique(l):
  assert(isinstance(l, list))
  return list(set(l))

@tornado.gen.coroutine
def main(argv=None):
  usage_doc = 'USAGE: %s [flags] --address=addr1,addr2,... source [dest]\n'
  sys.modules['__main__'].__doc__ = usage_doc

  if not argv:
    argv = sys.argv

  try:
    argv = FLAGS(argv)
    if FLAGS.verbose:
      log.setLevel(logging.DEBUG)
    else:
      log.setLevel(logging.ERROR)

    source = argv[1]
    if len(argv) == 3:
      target = argv[2]
    else:
      target = os.path.basename(source)

    if not FLAGS.address:
      raise Exception("No address given.")
    addresslist = unique(FLAGS.address.split(",")) * FLAGS.connection_per_host

  except Exception as e:
    sys.stderr.write("Error: %s\n" % str(e))
    sys.stderr.write("Try `%s --help' for more information.\n" % argv[0])
    sys.exit(1)

  io_loop = tornado.ioloop.IOLoop.current()

  print "user:",
  ssh_id = raw_input()
  ssh_passwd = getpass.getpass("password: ")

  threadpool = futures.ThreadPoolExecutor(max_workers=len(addresslist) + 5)
  pool = ConnectionPool(addresslist, (ssh_id, ssh_passwd), threadpool, \
                        connect_timeout=FLAGS.connect_timeout, \
                        max_attempt=FLAGS.max_attempt)
  downloader = SplitDownloader(
    source, target, FLAGS.blocksize * 1024, pool, threadpool,
    compression=FLAGS.compression)

  def print_bw(x):
    print "%f mb/s" % (x / 1000000.)
  measure = BandwidthMeasurer(print_bw, 1000)
  downloader.add_chunk_handler(measure.handle_chunk_downloaded)
  measure.start()

  try:
    yield downloader.download()
  except:
    log.error("download failed.")

if __name__ == '__main__':
  tornado.ioloop.IOLoop.instance().run_sync(main)
