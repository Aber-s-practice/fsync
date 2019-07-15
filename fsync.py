import os
import time
import datetime
import socket
import logging
import threading

import pysftp
from paramiko.util import ClosingContextManager
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


logger: logging.Logger = logging.getLogger("sync")
logging.getLogger("paramiko").setLevel(logging.WARNING)


class MonitorFileEventHandler:

    def __init__(self, localpath, remotepath, create_connection: pysftp.Connection):
        self.localpath = localpath
        self.remotepath = remotepath
        self.create_sftp = create_connection

    @property
    def sftp(self) -> pysftp.Connection:
        return self.create_sftp(self.remotepath)

    def dispatch(self, event):
        """
        Dispatches events to the appropriate methods.
        """
        self.on_any_event(event)

        if event.is_directory:
            handler = getattr(self, f"on_dir_{event.event_type}")
        else:
            handler = getattr(self, f"on_file_{event.event_type}")
        threading.Thread(target=handler, args=(event,), daemon=True).start()
        # return handler(event)

    def on_any_event(self, event):
        event.rel_path = os.path.relpath(event.src_path, self.localpath).replace("\\", "/")
        event.remote_path = os.path.join(self.remotepath, event.rel_path).replace("\\", "/")

        def log(l, r):
            logger.debug(f"<{event.__class__.__name__} {event.rel_path}> {l} {r}")
        event.callback = log
        logger.info(f"<{event.__class__.__name__}: {event.rel_path}>")

    def on_dir_modified(self, event):
        """
        此时仅为文件夹内产生变化, 不做任何事
        """

    def on_dir_moved(self, event):
        dest_path = os.path.join(
            self.remotepath,
            os.path.relpath(event.dest_path, self.localpath)
        ).replace("\\", "/")
        self.sftp.rename(event.remote_path, dest_path)

    def on_dir_created(self, event):
        self.sftp.makedirs(event.remote_path)

    def on_file_modified(self, event):
        for _ in range(2):
            try:
                # self.sftp.remove(event.rel_path)
                self.sftp.put(
                    event.src_path,
                    event.remote_path,
                    callback=event.callback
                )
                break
            except FileNotFoundError:
                dir_path = os.path.dirname(event.remote_path)
                logger.debug(f"Try to create dir {dir_path}")
                self.sftp.makedirs(dir_path)

    def on_file_moved(self, event):
        dest_path = os.path.join(
            self.remotepath,
            os.path.relpath(event.dest_path, self.localpath)
        ).replace("\\", "/")
        try:
            self.sftp.rename(event.remote_path, dest_path)
        except FileNotFoundError:
            # directory move
            # nothing to do
            pass

    def on_file_deleted(self, event):
        if self.sftp.isfile(event.remote_path):
            self.sftp.remove(event.remote_path)
        else:
            self.sftp.rmdir(event.remote_path)

    def on_file_created(self, event):
        self.on_file_modified(event)


class SyncFile(ClosingContextManager):
    def __init__(self, localpath: str, remotepath: str, user: str, passwd: str, host: str, port: int = 22):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        def create_connection(default_path=None):
            return pysftp.Connection(
                host=host,
                port=port,
                username=user,
                password=passwd,
                cnopts=cnopts,
                default_path=default_path
            )

        handler = MonitorFileEventHandler(
            localpath,
            remotepath,
            create_connection
        )
        self.observer = Observer()
        self.observer.schedule(handler, localpath, recursive=True)
        self.observer.start()

    def close(self):
        self.observer.stop()
        self.observer.join()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(levelname)s] %(message)s',
    )
    logger.setLevel(logging.INFO)

    with SyncFile("local path", "remote base path", "username", "password", "hostname", 22) as monitor:
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("server closed.")
