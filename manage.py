#!/usr/bin/env python3

"""
CLI to install the backend and manage plugins and data entries
"""

import grp
import pwd
import sys
import os
import json
import subprocess
import getpass
import argparse
import redis

from typing import Dict, Callable, Sequence, Iterable, Tuple

from autotest_backend.config import config
from autotest_backend import run_test_command

REDIS_CONNECTION = redis.Redis.from_url(config["redis_url"], decode_responses=True)


def _schema() -> Dict:
    """
    Return a dictionary representation of the json loaded from the schema_skeleton.json file or from the redis
    database if it exists.
    """
    schema = REDIS_CONNECTION.get("autotest:schema")

    if schema:
        return json.loads(schema)

    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "schema_skeleton.json")) as f:
        return json.load(f)


def _print(*args_, **kwargs) -> None:
    """
    Exactly the same as the builtin print function but prepends "[AUTOTESTER]"
    """
    print("[AUTOTESTER]", *args_, **kwargs)


class _Manager:
    """
    Abstract Manager class used to manage resources
    """

    args: argparse.Namespace

    def __init__(self, args):
        self.args = args


def parse_args() -> Callable:
    """
    Parses command line arguments using the argparse module and returns a function to call to
    execute the requested command.
    """
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="manager")

    subparsers.add_parser("tester", help="testers", description="testers")
    subparsers.add_parser("plugin", help="plugins", description="plugins")
    subparsers.add_parser("data", help="data", description="data")

    for name, parser_ in subparsers.choices.items():
        subsubparser = parser_.add_subparsers(dest="action")

        install_parser = subsubparser.add_parser("install", help=f"install {parser_.description}")

        if name == "data":
            install_parser.add_argument("name", help="unique name to give the data")
            install_parser.add_argument("path", help="path to a file or directory on disk that contains the data")
        else:
            install_parser.add_argument("paths", nargs="+")

        remove_parser = subsubparser.add_parser("remove", help=f"remove {parser_.description}")
        remove_parser.add_argument("names", nargs="+")

        subsubparser.add_parser("list", help=f"list {parser_.description}")

        subsubparser.add_parser("clean", help=f"remove {parser_.description} that have been deleted on disk.")

    subparsers.add_parser("install", help="install backend")

    managers = {"install": BackendManager, "tester": TesterManager, "plugin": PluginManager, "data": DataManager}

    args = parser.parse_args()

    if args.manager == "install":
        args.action = "install"

    return getattr(managers[args.manager](args), args.action)


class PluginManager(_Manager):
    """
    Manger for plugins
    """

    def install(self) -> None:
        """
        Install plugins
        """
        schema = _schema()
        for path in self.args.paths:
            cli = os.path.join(path, "classic.cli")
            if os.path.isfile(cli):
                proc = subprocess.run([cli, "settings"], capture_output=True, check=False, universal_newlines=True)
                if proc.returncode:
                    _print(
                        f"Plugin settings could not be retrieved from plugin at {path}. Failed with:\n{proc.stderr}",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
                settings = json.loads(proc.stdout)
                plugin_name = list(settings.keys())[0]
                installed_plugins = schema["definitions"]["plugins"]["properties"]
                if plugin_name in installed_plugins:
                    _print(f"A plugin named {plugin_name} is already installed", file=sys.stderr, flush=True)
                    continue
                proc = subprocess.run([cli, "install"], capture_output=True, check=False, universal_newlines=True)
                if proc.returncode:
                    _print(f"Plugin installation at {path} failed with:\n{proc.stderr}", file=sys.stderr, flush=True)
                    continue
                installed_plugins.update(settings)
                REDIS_CONNECTION.set(f"autotest:plugin:{plugin_name}", path)
        REDIS_CONNECTION.set("autotest:schema", json.dumps(schema))

    def remove(self, additional: Sequence = tuple()):
        """
        Removes installed plugins specified in self.args. Additional plugins to remove can be specified
        with the additional keyword
        """
        schema = _schema()

        installed_plugins = schema["definitions"]["plugins"]["properties"]
        for name in self.args.names + list(additional):
            REDIS_CONNECTION.delete(f"autotest:plugin:{name}")
            if name in installed_plugins:
                installed_plugins.remove(name)
            try:
                installed_plugins.pop(name)
            except KeyError:
                continue
        REDIS_CONNECTION.set("autotest:schema", json.dumps(schema))

    @staticmethod
    def _get_installed() -> Iterable[Tuple[str, str]]:
        """
        Yield the name and path of all installed plugins
        """
        for plugin_key in REDIS_CONNECTION.keys("autotest:tuple:*"):
            plugin_name = plugin_key.split(":")[-1]
            path = REDIS_CONNECTION.get(plugin_key)
            yield plugin_name, path

    def list(self) -> None:
        """
        Print the name and path of all installed plugins
        """
        for plugin_name, path in self._get_installed():
            print(f"{plugin_name} @ {path}")

    def clean(self) -> None:
        """
        Remove all plugins that are installed but whose data has been removed from disk
        """
        to_remove = [plugin_name for plugin_name, path in self._get_installed() if not os.path.isdir(path)]
        _print("Removing the following plugins:", *to_remove, sep="\t\n")
        self.remove(additional=to_remove)


class TesterManager(_Manager):
    """
    Manager for testers
    """

    def install(self) -> None:
        """
        Install testers
        """
        schema = _schema()
        for path in self.args.paths:
            cli = os.path.join(path, "classic.cli")
            if os.path.isfile(cli):
                proc = subprocess.run([cli, "settings"], capture_output=True, check=False, universal_newlines=True)
                if proc.returncode:
                    _print(
                        f"Tester settings could not be retrieved from tester at {path}. Failed with:\n{proc.stderr}",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
                settings = json.loads(proc.stdout)
                tester_name = settings["properties"]["tester_type"]["const"]
                installed_testers = schema["definitions"]["installed_testers"]["enum"]
                if tester_name in installed_testers:
                    _print(f"A tester named {tester_name} is already installed", file=sys.stderr, flush=True)
                    continue
                proc = subprocess.run([cli, "install"], capture_output=True, check=False, universal_newlines=True)
                if proc.returncode:
                    _print(f"Tester installation at {path} failed with:\n{proc.stderr}", file=sys.stderr, flush=True)
                    continue
                installed_testers.append(tester_name)
                schema["definitions"]["tester_schemas"]["oneOf"].append(settings)
                REDIS_CONNECTION.set(f"autotest:tester:{tester_name}", path)
        REDIS_CONNECTION.set("autotest:schema", json.dumps(schema))

    def remove(self, additional: Sequence = tuple()) -> None:
        """
        Removes installed testers specified in self.args. Additional testers to remove can be specified
        with the additional keyword
        """
        schema = _schema()
        tester_settings = schema["definitions"]["tester_schemas"]["oneOf"]
        installed_testers = schema["definitions"]["installed_testers"]["enum"]
        for name in self.args.names + list(additional):
            REDIS_CONNECTION.delete(f"autotest:tester:{name}")
            if name in installed_testers:
                installed_testers.remove(name)
            for i, settings in enumerate(tester_settings):
                if name == settings["properties"]["tester_type"]["const"]:
                    tester_settings.pop(i)
                    break
        REDIS_CONNECTION.set("autotest:schema", json.dumps(schema))

    @staticmethod
    def _get_installed() -> Iterable[Tuple[str, str]]:
        """
        Yield the name and path of all installed testers
        """
        for tester_key in REDIS_CONNECTION.keys("autotest:tester:*"):
            tester_name = tester_key.split(":")[-1]
            path = REDIS_CONNECTION.get(tester_key)
            yield tester_name, path

    def list(self) -> None:
        """
        Print the name and path of all installed testers
        """
        for tester_name, path in self._get_installed():
            print(f"{tester_name} @ {path}")

    def clean(self) -> None:
        """
        Remove all testers that are installed but whose data has been removed from disk
        """
        to_remove = [tester_name for tester_name, path in self._get_installed() if not os.path.isdir(path)]
        _print("Removing the following testers:", *to_remove, sep="\t\n")
        self.remove(additional=to_remove)


class DataManager(_Manager):
    """
    Manager for data entries
    """

    def install(self) -> None:
        """
        Install a data entry
        """
        schema = _schema()

        # the data_entries key is used to be consistent with the autotest-backend-docker project
        installed_volumes = schema["definitions"]["data_entries"]["items"]["enum"]
        name = self.args.name
        path = os.path.abspath(self.args.path)
        if name in installed_volumes:
            _print(f"A data mapping named {name} is already installed", file=sys.stderr, flush=True)
            return
        if not os.path.exists(path):
            _print(f"No file or directory can be found at {path}", file=sys.stderr, flush=True)
            return
        installed_volumes.append(name)
        REDIS_CONNECTION.set(f"autotest:data:{name}", path)
        REDIS_CONNECTION.set("autotest:schema", json.dumps(schema))

    def remove(self, additional: Sequence = tuple()) -> None:
        """
        Removes installed data entries specified in self.args. Additional entries to remove can be specified
        with the additional keyword
        """
        schema = _schema()
        installed_volumes = schema["definitions"]["data_entries"]["items"]["enum"]
        for name in self.args.names + list(additional):
            installed_volumes.remove(name)
            REDIS_CONNECTION.delete(f"autotest:data:{name}")
        REDIS_CONNECTION.set("autotest:schema", json.dumps(schema))

    @staticmethod
    def _get_installed() -> Iterable[Tuple[str, str]]:
        """
        Yield the name and path of all installed data entries
        """
        for data_key in REDIS_CONNECTION.keys("autotest:data:*"):
            data_name = data_key.split(":")[-1]
            path = REDIS_CONNECTION.get(data_key)
            yield data_name, path

    def list(self) -> None:
        """
        Print the name and path of all installed entries
        """
        for data_name, path in self._get_installed():
            print(f"{data_name} @ {path}")

    def clean(self) -> None:
        """
        Remove all data entries that are installed but whose data has been removed from disk
        """
        to_remove = [data_name for data_name, path in self._get_installed() if not os.path.isdir(path)]
        _print("Removing the following data mappings:", *to_remove, sep="\t\n")
        self.remove(additional=to_remove)


class BackendManager(_Manager):
    """
    Manager for the autotest backend
    """

    @staticmethod
    def _check_dependencies() -> None:
        """
        Check if all dependencies are installed and accessible
        """
        _print("checking if redis url is valid:")
        try:
            REDIS_CONNECTION.keys()
        except Exception as e:
            raise Exception(f'Cannot connect to redis database with url: {config["redis_url"]}') from e

    @staticmethod
    def _check_users_exist() -> None:
        """
        Checks that all users specified in the configuration files:

        - exist
        - can be sudo'd to from the current user (ex: "sudo -u other_user -- some command")
        - has a primary group that the current user belongs to as well
        """
        groups = {grp.getgrgid(g).gr_name for g in os.getgroups()}
        for w in config["workers"]:
            username = w["user"]
            _print(f"checking if worker with username {username} exists")
            try:
                pwd.getpwnam(username)
            except KeyError:
                raise Exception(f"user with username {username} does not exist")
            _print(
                f"checking if worker with username {username} can be accessed by the current user {getpass.getuser()}"
            )
            try:
                subprocess.run(
                    run_test_command(username).format("echo", "test"), stdout=subprocess.DEVNULL, shell=True, check=True
                )
            except Exception as e:
                raise Exception(f"user {getpass.getuser()} cannot run commands as the {username} user") from e
            _print(f"checking if the current user belongs to the {username} group")
            if username not in groups:
                raise Exception(f"user {getpass.getuser()} does not belong to group: {username}")

    @staticmethod
    def _create_workspace() -> None:
        """
        Creates the workspace directory on disk if it doesn't exist already
        """
        _print(f'creating workspace at {config["workspace"]}')
        os.makedirs(config["workspace"], exist_ok=True)

    def install(self) -> None:
        """
        Check that the server is set up properly and create the workspace.
        """
        self._check_dependencies()
        self._check_users_exist()
        self._create_workspace()
        REDIS_CONNECTION.set("autotest:schema", json.dumps(_schema()))


if __name__ == "__main__":
    parse_args()()
