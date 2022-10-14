import os
import shutil
import time
import json
import subprocess
import signal
import socket
import getpass
import requests
import gzip
import redis
import mimetypes
from typing import Optional, Dict, Union, List, Tuple, Callable, Type
from types import TracebackType

from .config import config
from .utils import loads_partial_json, set_rlimits_before_test, extract_zip_stream, recursive_iglob, copy_tree

REDIS_URL = config["redis_url"]
TEST_SCRIPT_DIR = os.path.join(config["workspace"], "scripts")

ResultData = Dict[str, Union[str, int, type(None), Dict]]


def redis_connection() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


def run_test_command(test_username: Optional[str] = None) -> str:
    """
    Return a command used to run test scripts as a the test_username
    user, with the correct arguments. Set test_username to None to
    run as the current user.

    >>> test_script = 'mysscript.py'
    >>> run_test_command('f').format(test_script)
    "sudo -u f -- ./myscript.py"

    >>> run_test_command().format(test_script)
    './myscript.py'
    """
    cmd = "{}"
    if test_username is not None:
        cmd = f"sudo -Eu {test_username} -- " + "{}"

    return cmd


def _create_test_group_result(
    stdout: str, stderr: str, run_time: int, extra_info: Dict, feedback: List, timeout: Optional[int] = None
) -> ResultData:
    """
    Return the arguments passed to this function in a dictionary. If stderr is
    falsy, change it to None. Load the json string in stdout as a dictionary.
    """
    all_results, malformed = loads_partial_json(stdout, dict)
    result = {
        "time": run_time,
        "timeout": timeout,
        "tests": [],
        "stderr": stderr or None,
        "malformed": stdout if malformed else None,
        "extra_info": extra_info or {},
        "annotations": None,
        "feedback": feedback,
    }
    for res in all_results:
        if "annotations" in res:
            result["annotations"] = res["annotations"]
        else:
            result["tests"].append(res)

    return result


def _kill_user_processes(test_username: str) -> None:
    """
    Kill all processes that test_username is able to kill
    """
    kill_cmd = f"sudo -u {test_username} -- bash -c 'kill -KILL -1'"
    subprocess.run(kill_cmd, shell=True)


def get_available_port(min_, max_, host: str = "localhost") -> str:
    """Return the next available open port on host."""
    for next_port in range(min_, max_ + 1):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((host, next_port))
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                port = s.getsockname()[1]
                return str(port)
        except OSError:
            continue

def set_up_plugins(test_username, plugin_data):
    environment = {}
    for name, data in plugin_data.items():
        if data.get("enabled"):
            path = redis_connection().get(f"autotest:plugin:{name}")
            if path is None:
                raise Exception(f"plugin {name} is not installed")
            cli = os.path.join(path, 'classic.cli')
            stringified_data = {k: str(v) for k, v in data.items() if k != 'enabled'}
            proc = subprocess.run(
                [cli, 'before_test', test_username],
                capture_output=True,
                check=False,
                universal_newlines=True,
                env=stringified_data
            )
            environment.update(json.loads(proc.stdout))
    return environment

def get_data_environment(data_names):
    environment = {}
    for name in data_names:
        path = redis_connection().get(f"autotest:data:{name}")
        if path is None:
            raise Exception(f"data {name} is not installed")
        if not os.path.exists(path):
            raise Exception(f"data {name} at path {path} does not exist")
        environment[f"AUTOTEST_DATA_{name.upper()}"] = path
    return environment

def _get_env_vars(test_username: str, plugin_data: Dict, data_names: List) -> Dict[str, str]:
    """Return a dictionary containing all environment variables to pass to the next test"""
    env_vars = {"AUTOTESTENV": "true", **set_up_plugins(test_username, plugin_data), **get_data_environment(data_names)}
    worker_config = [w for w in config["workers"] if w["user"] == test_username][0]
    resources_config = worker_config.get("resources", {})
    if resources_config:
        port_config = resources_config.get("port")
        if port_config:
            env_vars["PORT"] = get_available_port(port_config["min"], port_config["max"])

    return env_vars


def _get_feedback(test_data, tests_path, test_id):
    feedback_files = test_data.get("feedback_file_names", [])
    feedback = []
    for feedback_file in feedback_files:
        feedback_path = os.path.join(tests_path, feedback_file)
        if os.path.isfile(feedback_path):
            with open(feedback_path, "rb") as f:
                conn = redis_connection()
                id_ = conn.incr("autotest:feedback_files_id")
                key = f"autotest:feedback_file:{test_id}:{id_}"
                conn.set(key, gzip.compress(f.read()))
                conn.expire(key, 3600)  # TODO: make this configurable
                feedback.append(
                    {
                        "filename": feedback_file,
                        "mime_type": mimetypes.guess_type(feedback_path)[0] or "text/plain",
                        "compression": "gzip",
                        "id": id_,
                    }
                )
        else:
            raise Exception(f"Cannot find feedback file at '{feedback_path}'.")
    return feedback


def _update_env_vars(base_env: Dict, test_env: Dict) -> Dict:
    """
    Update base_env with the key/value pairs in test_env.
    If any keys in test_env also occur in base_env, raise an error.
    Since, the content of test_env is defined by the client, this ensures that the client cannot overwrite environment
    variables set by this autotester.
    """
    conflict = base_env.keys() & test_env.keys()
    if conflict:
        raise Exception(
            f"The following environment variables cannot be overwritten for this test: {', '.join(conflict)}"
        )
    return {**base_env, **test_env}


def _run_test_specs(
    cmd: str,
    test_settings: dict,
    categories: List[str],
    tests_path: str,
    test_username: str,
    test_id: Union[int, str],
    test_env_vars: Dict[str, str],
) -> List[ResultData]:
    """
    Run each test script in test_scripts in the tests_path directory using the
    command cmd. Return the results.
    """
    results = []

    for settings in test_settings["testers"]:
        args = cmd.format(settings["_command"])

        for test_data in settings["test_data"]:
            test_category = test_data.get("category", [])
            if set(test_category) & set(categories):
                start = time.time()
                out, err = "", ""
                timeout_expired = None
                timeout = test_data.get("timeout")
                plugin_data = test_data.get("plugins", {})
                data_names = test_data.get("data_volumes", [])
                try:
                    additional_env_vars = _get_env_vars(test_username, plugin_data, data_names)
                    env_vars = {**os.environ, **additional_env_vars, **settings["_env"]}
                    env_vars = _update_env_vars(env_vars, test_env_vars)
                    proc = subprocess.Popen(
                        args,
                        start_new_session=True,
                        cwd=tests_path,
                        shell=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        stdin=subprocess.PIPE,
                        preexec_fn=set_rlimits_before_test,
                        universal_newlines=True,
                        env=env_vars,
                    )
                    try:
                        settings_json = json.dumps({**settings, "test_data": test_data})
                        out, err = proc.communicate(input=settings_json, timeout=timeout)
                    except subprocess.TimeoutExpired:
                        if test_username == getpass.getuser():
                            pgrp = os.getpgid(proc.pid)
                            os.killpg(pgrp, signal.SIGKILL)
                        else:
                            _kill_user_processes(test_username)
                        out, err = proc.communicate()
                        timeout_expired = timeout
                except Exception as e:
                    err += "\n\n{}".format(e)
                finally:
                    duration = int(round(time.time() - start, 3) * 1000)
                    extra_info = test_data.get("extra_info", {})
                    feedback = _get_feedback(test_data, tests_path, test_id)
                    results.append(_create_test_group_result(out, err, duration, extra_info, feedback, timeout_expired))
    return results


def _clear_working_directory(tests_path: str, test_username: str) -> None:
    """
    Run commands that clear the tests_path working directory
    """
    if test_username != getpass.getuser():
        chmod_cmd = f"sudo -u {test_username} -- bash -c 'chmod -Rf ugo+rwX {tests_path}'"
    else:
        chmod_cmd = f"chmod -Rf ugo+rwX {tests_path}"

    subprocess.run(chmod_cmd, shell=True)

    # be careful not to remove the tests_path dir itself since we have to
    # set the group ownership with sudo (and that is only done in ../install.sh)
    clean_cmd = f"rm -rf {tests_path}/.[!.]* {tests_path}/*"
    subprocess.run(clean_cmd, shell=True)


def _stop_tester_processes(test_username: str) -> None:
    """
    Run a command that kills all tester processes either by killing all
    user processes or killing with a reaper user (see https://lwn.net/Articles/754980/
    for reference).
    """
    if test_username != getpass.getuser():
        _kill_user_processes(test_username)


def _setup_files(settings_id: int, user: str, files_url: str, tests_path: str, test_username: str) -> None:
    """
    Copy test script files and student files to the working directory tests_path,
    then make it the current working directory.
    The following permissions are also set:
        - tests_path directory:     rwxrwx--T
        - test subdirectories:      rwxrwx--T
        - test files:               rw-r-----
        - student subdirectories:   rwxrwx---
        - student files:            rw-rw----
    """
    creds = json.loads(redis_connection().hget("autotest:user_credentials", key=user))
    r = requests.get(files_url, headers={"Authorization": f"{creds['auth_type']} {creds['credentials']}"})
    extract_zip_stream(r.content, tests_path, ignore_root_dirs=1)
    for fd, file_or_dir in recursive_iglob(tests_path):
        if fd == "d":
            os.chmod(file_or_dir, 0o770)
        else:
            os.chmod(file_or_dir, 0o770)
        shutil.chown(file_or_dir, group=test_username)
    test_script_dir = json.loads(redis_connection().hget("autotest:settings", settings_id))["_files"]
    script_files = copy_tree(test_script_dir, tests_path)
    for fd, file_or_dir in script_files:
        if fd == "d":
            os.chmod(file_or_dir, 0o1770)
        else:
            os.chmod(file_or_dir, 0o750)
        shutil.chown(file_or_dir, group=test_username)


def tester_user() -> Tuple[str, str]:
    """
    Get the workspace for the tester user specified by the WORKERUSER
    environment variable, return the user_name and path to that user's workspace.

    Raises an AutotestError if a tester user is not specified or if a workspace
    has not been setup for that user.
    """
    user_name = os.environ.get("WORKERUSER")
    if user_name is None:
        raise Exception("No worker users available to run this job")

    workers_dir = os.path.join(config["workspace"], "workers")
    user_workspace = os.path.join(workers_dir, user_name)
    os.makedirs(user_workspace, exist_ok=True)
    os.chmod(workers_dir, 0o755)
    shutil.chown(user_workspace, group=user_name)
    os.chmod(user_workspace, 0o1770)
    if not os.path.isdir(user_workspace):
        raise Exception(f"No workspace directory for user: {user_name}")

    return user_name, user_workspace


def run_test(settings_id, test_id, files_url, categories, user, test_env_vars):
    results = []
    error = None
    try:
        settings = json.loads(redis_connection().hget("autotest:settings", key=settings_id))
        settings["_last_access"] = int(time.time())
        redis_connection().hset("autotest:settings", key=settings_id, value=json.dumps(settings))
        test_username, tests_path = tester_user()
        try:
            _setup_files(settings_id, user, files_url, tests_path, test_username)
            cmd = run_test_command(test_username=test_username)
            results = _run_test_specs(cmd, settings, categories, tests_path, test_username, test_id, test_env_vars)
        finally:
            _stop_tester_processes(test_username)
            _clear_working_directory(tests_path, test_username)
    except Exception as e:
        error = str(e)
    finally:
        key = f"autotest:test_result:{test_id}"
        redis_connection().set(key, json.dumps({"test_groups": results, "error": error}))
        redis_connection().expire(key, 3600)  # TODO: make this configurable


def ignore_missing_dir_error(
    _func: Callable,
    _path: str,
    excinfo: Tuple[Type[BaseException], BaseException, Optional[TracebackType]],
) -> None:
    """Used by shutil.rmtree to ignore a FileNotFoundError"""
    err_type, err_inst, traceback = excinfo
    if err_type == FileNotFoundError:
        return
    raise err_inst


def update_test_settings(user, settings_id, test_settings, file_url):
    try:
        settings_dir = os.path.join(TEST_SCRIPT_DIR, str(settings_id))

        os.makedirs(settings_dir, exist_ok=True)
        os.chmod(TEST_SCRIPT_DIR, 0o755)

        files_dir = os.path.join(settings_dir, "files")
        shutil.rmtree(files_dir, onerror=ignore_missing_dir_error)
        os.makedirs(files_dir, exist_ok=True)
        creds = json.loads(redis_connection().hget("autotest:user_credentials", key=user))
        r = requests.get(file_url, headers={"Authorization": f"{creds['auth_type']} {creds['credentials']}"})
        extract_zip_stream(r.content, files_dir, ignore_root_dirs=0)

        schema = json.loads(redis_connection().get("autotest:schema"))
        installed_testers = schema["definitions"]["installed_testers"]["enum"]

        for i, tester_settings in enumerate(test_settings["testers"]):
            tester_type = tester_settings["tester_type"]
            if tester_type not in installed_testers:
                raise Exception(f"tester {tester_type} is not installed")
            env_dir = os.path.join(settings_dir, f"{tester_type}_{i}")

            tester_path = redis_connection().get(f"autotest:tester:{tester_type}")
            try:
                env_data = tester_settings.get("env_data", {})
                version = env_data.get("version", "")
                requirements = env_data.get("requirements", "")
                proc = subprocess.run(
                    [os.path.join(tester_path, 'classic.cli'), 'create_environment', version, requirements, env_dir],
                    capture_output=True
                )
                if proc.returncode:
                    raise Exception(proc.stderr)
                result = json.loads(proc.stdout)
                tester_settings["_command"] = result.pop("COMMAND")
                tester_settings["_env"] = result
            except Exception as e:
                raise Exception(f"create tester environment failed:\n{e}") from e
            test_settings["testers"][i] = tester_settings
        test_settings["_files"] = files_dir
        test_settings.pop("_error", None)
    except Exception as e:
        test_settings["_error"] = str(e)
        raise
    finally:
        test_settings["_user"] = user
        test_settings["_last_access"] = int(time.time())
        redis_connection().hset("autotest:settings", key=settings_id, value=json.dumps(test_settings))
