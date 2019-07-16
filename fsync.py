import os
import re
import time
import datetime
import socket
import logging
import threading

import pysftp
import click
from paramiko.util import ClosingContextManager
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


logger: logging.Logger = logging.getLogger("sync")
logging.getLogger("paramiko").setLevel(logging.WARNING)


def is_ignore(path: str, ignore: list, local: str) -> bool:
    path = path.replace("\\", "/")

    for _ignore in ignore:
        _ignore = _ignore.replace("\\", "/")
        if _ignore.startswith("./") or _ignore.startswith("/"):
            if path == os.path.join(local, ignore).replace("\\", "/"):
                return True
        elif re.search("\*\.(.*)", _ignore):
            if path.endswith(_ignore.split("*")[1]):
                return True
        else:
            if _ignore in path:
                return True
    return False


class MonitorFileEventHandler:

    def __init__(self, localpath, remotepath, ignore: list, create_connection: pysftp.Connection):
        self.localpath = localpath
        self.remotepath = remotepath
        self.create_sftp = create_connection
        self.ignore = ignore

    @property
    def sftp(self) -> pysftp.Connection:
        return self.create_sftp(self.remotepath)

    def dispatch(self, event):
        """
        Dispatches events to the appropriate methods.
        """
        if is_ignore(event.src_path, self.ignore, self.localpath):
            return

        self.on_any_event(event)

        if event.is_directory:
            handler = getattr(self, f"on_dir_{event.event_type}")
        else:
            handler = getattr(self, f"on_file_{event.event_type}")
        threading.Thread(target=handler, args=(event,), daemon=True).start()
        # return handler(event)

    def on_any_event(self, event):
        event.rel_path = os.path.relpath(
            event.src_path, self.localpath).replace("\\", "/")
        event.remote_path = os.path.join(
            self.remotepath, event.rel_path).replace("\\", "/")

        def log(l, r):
            logger.debug(
                f"<{event.__class__.__name__} {event.rel_path}> {l} {r}")
        event.callback = log
        logger.info(f"<{event.__class__.__name__}: {event.rel_path}>")

    def on_dir_modified(self, event):
        """
        trigger by file in directory
        Nothing to do.
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
    def __init__(self, localpath: str, remotepath: str, user: str, passwd: str, host: str, ignore: list, port: int = 22):
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
            ignore,
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


@click.group()
def main():
    pass


@main.command(help="Upload all file from local to remote")
@click.option('-l', '--local', default=os.getcwd(), help='default os.getcwd()')
@click.option('-r', '--remote', required=True)
@click.option('-h', '--host', required=True)
@click.option('-p', '--port', type=int, default=22)
@click.option('-u', '--user', required=True)
@click.option('--password', required=True)
@click.option('--ignore', multiple=True)
def upload(local, remote, host, port, user, password, ignore):
    click.secho(f"Upload all file in ", nl=False)
    click.secho(local, fg="blue", nl=False)
    click.secho(" to ", nl=False)
    click.secho(f"{host}:{remote}", fg="blue")

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(
        host=host,
        port=port,
        username=user,
        password=password,
        cnopts=cnopts
    ) as sftp:  # pysftp.Connection
        if not sftp.exists(remote):
            sftp.makedirs(remote)
        sftp.chdir(remote)

        for root, directories, files in os.walk(local):
            root = os.path.relpath(root, local).replace("\\", "/")

            for directory in directories:
                directory = os.path.join(root, directory).replace("\\", "/")

                if is_ignore(directory, ignore, local):
                    continue

                if not sftp.exists(directory):
                    sftp.mkdir(directory)

            for file in files:
                file = os.path.join(root, file).replace("\\", "/")

                if is_ignore(file, ignore, local):
                    continue

                sftp.put(os.path.join(local, file), file)


@main.command(help="Sync file by watchdog")
@click.option('-l', '--local', default=os.getcwd(), help='default os.getcwd()')
@click.option('-r', '--remote', required=True)
@click.option('-h', '--host', required=True)
@click.option('-p', '--port', type=int, default=22)
@click.option('-u', '--user', required=True)
@click.option('--password', required=True)
@click.option('--ignore', multiple=True)
def sync(local, remote, host, port, user, password, ignore):
    with SyncFile(local, remote, user, password, host, ignore, port):
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("server closed.")


if __name__ == "__main__":
    main()
