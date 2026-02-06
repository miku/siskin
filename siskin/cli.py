"""
Single entry point for siskin. Usage:

    siskin <command> [args...]

Commands map to the former task* scripts, e.g.:

    siskin run AIUpdate --workers 4     (was: taskdo)
    siskin names                        (was: tasknames)
    siskin cat AIExport                 (was: taskcat)

Run `siskin --help` for a full list of commands.
"""

import collections
import configparser
import datetime
import gzip
import hashlib
import importlib
import inspect
import json
import os
import platform
import re
import shutil
import subprocess
import sys
from io import StringIO

import luigi
import requests
import urllib3
from luigi.cmdline_parser import CmdlineParser
from luigi.parameter import MissingParameterException
from luigi.task import Register
from luigi.task_register import TaskClassNotFoundException
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import PythonLexer

from siskin import __version__
from siskin.benchmark import green, yellow
from siskin.configuration import Config
from siskin.utils import get_task_import_cache, iterfiles, random_string


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _ensure_task_imports(taskname):
    """
    Import the module for a given task name using the import cache, falling
    back to star imports if the task is not cached.
    """
    task_import_cache, path = get_task_import_cache()
    if taskname in task_import_cache:
        importlib.import_module(task_import_cache[taskname])
    else:
        try:
            os.remove(path)
        except OSError:
            pass
        _star_imports()


def _star_imports():
    """
    Force-import all sources and workflows so Luigi can discover tasks.
    This replaces the ``from siskin.sources import *`` pattern which is not
    allowed inside functions in Python 3.
    """
    import siskin.sources
    import siskin.workflows
    for name in siskin.sources.__all__:
        if name.startswith("_"):
            continue
        importlib.import_module(f"siskin.sources.{name}")
    for name in siskin.workflows.__all__:
        if name.startswith("_"):
            continue
        importlib.import_module(f"siskin.workflows.{name}")


def _get_output_path(args):
    """
    Given task CLI args (e.g. ['AIExport']), return the output file path.
    """
    if not args:
        print("usage: siskin output TASKNAME [--param value ...]", file=sys.stderr)
        sys.exit(1)
    _ensure_task_imports(args[0])
    parser = CmdlineParser(args)
    output = parser.get_task_obj().output()
    try:
        return output.path
    except AttributeError:
        print("output of task has no path", file=sys.stderr)
        sys.exit(1)


def _get_task_dir(args):
    """
    Return the directory containing the output of a task.
    """
    return os.path.dirname(_get_output_path(args))


def _cat_output(args, out=None):
    """
    Write task output to `out` (defaults to sys.stdout.buffer), handling
    compressed formats transparently. Returns the number of bytes written.
    """
    if out is None:
        out = sys.stdout.buffer
    path = _get_output_path(args)
    if not os.path.exists(path):
        print(f"output does not exist: {path}", file=sys.stderr)
        sys.exit(1)
    if path.endswith(".mrc"):
        subprocess.run(["yaz-marcdump", path])
        return
    if path.endswith(".gz"):
        with gzip.open(path, "rb") as f:
            shutil.copyfileobj(f, out)
        return
    if path.endswith(".zip"):
        subprocess.run(["unzip", "-l", path])
        return
    if path.endswith(".zst"):
        subprocess.run(["unzstd", "-q", "-c", path])
        return
    with open(path, "rb") as f:
        shutil.copyfileobj(f, out)


# ---------------------------------------------------------------------------
# commands — ported from Python scripts
# ---------------------------------------------------------------------------

def cmd_run():
    """Run a Luigi task."""
    if len(sys.argv) < 2:
        print("usage: siskin run TASKNAME [--param value ...]", file=sys.stderr)
        sys.exit(1)
    _ensure_task_imports(sys.argv[1])
    try:
        luigi.run()
    except MissingParameterException as exc:
        print(f"missing parameter: {exc}", file=sys.stderr)
        sys.exit(1)
    except TaskClassNotFoundException as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)


def cmd_names():
    """List all available task names."""
    task_import_cache, _ = get_task_import_cache()
    for name in sorted(task_import_cache.keys()):
        print(name)


def cmd_inspect():
    """Show the source code of a task."""
    if len(sys.argv) < 2:
        print("usage: siskin inspect TASKNAME", file=sys.stderr)
        sys.exit(1)
    _ensure_task_imports(sys.argv[1])
    try:
        parser = CmdlineParser(sys.argv[1:])
        obj = parser.get_task_obj()
        snippet = inspect.getsource(obj.__class__)
        hlsnippet = highlight(snippet, PythonLexer(), TerminalFormatter())
        print(hlsnippet)
    except MissingParameterException as err:
        print(f"missing parameter: {err}", file=sys.stderr)
        sys.exit(1)
    except TaskClassNotFoundException as err:
        print(err, file=sys.stderr)
        sys.exit(1)


def cmd_output():
    """Show the output path of a task."""
    try:
        print(_get_output_path(sys.argv[1:]))
    except MissingParameterException as err:
        print(f"missing parameter: {err}", file=sys.stderr)
        sys.exit(1)
    except TaskClassNotFoundException as err:
        print(err, file=sys.stderr)
        sys.exit(1)


def cmd_config():
    """Display the current siskin configuration."""
    output = StringIO()
    config = Config.instance()
    config.write(output)
    print(output.getvalue())


def cmd_deps():
    """Show the dependency tree of a task (ASCII art)."""
    _star_imports()
    g = collections.defaultdict(set)

    def sanitize(s):
        s = re.sub(r"host=[^ ,]+", "host=example.com", s, 0)
        s = re.sub(r"username=[^ ,]+", "username=xxxx", s, 0)
        s = re.sub(r"password=[^ ,]+", "password=xxxx", s, 0)
        return s

    def dump(root=None, indent=0, output=None):
        if indent == 0:
            output.write('%s ── %s\n' % ('    ' * indent, root))
        else:
            output.write('%s └─ %s\n' % ('    ' * indent, root))
        for dep in g[root]:
            dump(root=dep, indent=indent + 1, output=output)

    try:
        parser = CmdlineParser(sys.argv[1:])
        root_task = parser.get_task_obj()
        queue = [root_task]
        while queue:
            task = queue.pop()
            for dep in task.deps():
                g[task].add(dep)
                queue.append(dep)
        output = StringIO()
        dump(root=root_task, output=output)
        print(sanitize(output.getvalue()))
    except MissingParameterException as err:
        print(f"missing parameter: {err}", file=sys.stderr)
        sys.exit(1)
    except TaskClassNotFoundException as err:
        print(err, file=sys.stderr)
        sys.exit(1)
    except BrokenPipeError:
        pass


def cmd_deps_dot():
    """Generate a Graphviz DOT representation of the task dependency tree."""
    _star_imports()
    g = collections.defaultdict(set)
    seen = set()
    INCLUDE_FULLY = {'Executable', 'FTPMirror'}

    def iterdeps(task):
        for dependency in task.deps():
            if task.task_family in INCLUDE_FULLY:
                taskname = (task.task_id,)
            else:
                taskname = (task.task_family,)
            if dependency.task_family in INCLUDE_FULLY:
                depname = (dependency.task_id,)
            else:
                depname = (dependency.task_family,)
            g[taskname].add(depname)
            if dependency not in seen:
                seen.add(dependency)
                iterdeps(dependency)

    def simpleformat(graph):
        s = 'digraph %s {' % random_string()
        for task, deps in graph.items():
            for dep in deps:
                s += '\t"%s" [fontname="Helvetica"]; ' % task[0]
                s += '\t"%s" [fontname="Helvetica"]; ' % dep[0]
                s += '\t"%s" -> "%s"; ' % (task[0], dep[0])
                s += "\n"
        s += '}'
        return s

    try:
        parser = CmdlineParser(sys.argv[1:])
        task = parser.get_task_obj()
        iterdeps(task)
        print(simpleformat(g))
    except MissingParameterException as err:
        print(f"missing parameter: {err}", file=sys.stderr)
        sys.exit(1)
    except TaskClassNotFoundException as err:
        print(err, file=sys.stderr)
        sys.exit(1)


def cmd_docs():
    """Show documentation for all tasks."""
    _star_imports()
    task_names = Register.task_names()
    print(f"{len(task_names)} tasks found\n")
    for name in task_names:
        if name.islower():
            continue
        klass = Register.get_task_cls(name)
        doc = klass.__doc__ or yellow("@TODO: docs")
        print(f"{green(name)} {doc}\n")


def cmd_cleanup():
    """Remove date-based task outputs before a given date."""
    if len(sys.argv) < 3:
        print("usage: siskin cleanup TASKNAME DATE", file=sys.stderr)
        sys.exit(1)
    taskname = sys.argv[1]
    boundary = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d")
    date_pattern = re.compile(r"date-[\d]{4,4}-[\d]{2,2}-[\d]{2,2}")
    _ensure_task_imports(taskname)
    try:
        parser = CmdlineParser(sys.argv[1:2])
        task = parser.get_task_obj()
        try:
            for path in iterfiles(task.taskdir()):
                match = date_pattern.search(path)
                if not match:
                    continue
                parsed = datetime.datetime.strptime(match.group(), "date-%Y-%m-%d")
                if parsed < boundary:
                    print(f"removing: {path}")
                    os.remove(path)
        except AttributeError:
            print("output of task has no path", file=sys.stderr)
            sys.exit(1)
    except MissingParameterException as err:
        print(f"missing parameter: {err}", file=sys.stderr)
        sys.exit(1)
    except TaskClassNotFoundException as err:
        print(err, file=sys.stderr)
        sys.exit(1)


def cmd_hash():
    """Calculate SHA1 hashes of task source code to detect changes."""
    def calculate_task_hashes():
        hashes = []
        task_import_cache, _ = get_task_import_cache()
        for klassname, modulename in sorted(task_import_cache.items()):
            module = importlib.import_module(modulename)
            klass = getattr(module, klassname)
            sha1 = hashlib.sha1()
            sha1.update(inspect.getsource(klass).encode("utf-8"))
            hashes.append((sha1.hexdigest(), f"{modulename}.{klassname}"))
        return hashes

    def dump_hashes():
        for h, name in calculate_task_hashes():
            print(f"{h}\t{name}")

    def compare(filename):
        fromfile = {}
        with open(filename) as handle:
            for line in handle:
                parts = line.strip().split("\t")
                if len(parts) != 2:
                    raise ValueError(f"invalid format: got {len(parts)} columns, want 2")
                fromfile[parts[1]] = parts[0]
        for h, taskname in calculate_task_hashes():
            if fromfile[taskname] != h:
                print(taskname.split(".")[-1])

    if len(sys.argv) == 1:
        dump_hashes()
    elif len(sys.argv) == 2 and sys.argv[1] in ("-h", "-help", "--help"):
        print("usage: siskin hash [FILE]")
        print()
        print("Without FILE, dump (hash, taskname) to stdout.")
        print("Given a FILE, compare and print task names with modified hashes.")
    elif len(sys.argv) == 2:
        compare(sys.argv[1])
    else:
        print("usage: siskin hash [FILE]", file=sys.stderr)
        sys.exit(1)


def cmd_ps():
    """Display running/pending/done/failed tasks from the Luigi scheduler."""
    addr = "localhost:8082"
    if len(sys.argv) >= 2:
        addr = sys.argv[1]
        if ":" not in addr:
            addr = f"{addr}:8082"
    try:
        print()
        print(f">> {addr}, {datetime.datetime.now()}")
        print()
        for status in ("RUNNING", "FAILED", "PENDING", "DONE", "DISABLED"):
            data = {"status": status, "upstream_status": "", "search": ""}
            r = requests.get(
                f"http://{addr}/api/task_list",
                params={"data": json.dumps(data)},
            )
            if r.status_code >= 400:
                raise RuntimeError(f"API ({r.url}) returned {r.status_code}: {r.text}")
            response = r.json()
            tasks = [task for task, _ in sorted(response["response"].items())]
            if not tasks:
                continue
            head = f"# {status} ({len(tasks)})"
            print(head)
            print()
            for task in tasks:
                print(f"{status[0].lower()}\t* {task}")
            print()
    except (
        ConnectionRefusedError,
        urllib3.exceptions.NewConnectionError,
        requests.exceptions.ConnectionError,
    ):
        pass
    except BrokenPipeError:
        pass


def cmd_home():
    """Show the task home directory."""
    try:
        config = Config.instance()
        print(config.get("core", "home"))
    except configparser.Error as err:
        print(f"invalid configuration: {err}", file=sys.stderr)
        sys.exit(1)


def cmd_importcache():
    """Show the location of the task import cache."""
    _, path = get_task_import_cache()
    print(path)


def cmd_checksetup():
    """Check external tool dependencies."""
    def which(program):
        return shutil.which(program)

    FAIL = "\033[91m"
    OKGREEN = "\033[92m"
    ENDC = "\033[0m"

    deps = [
        ("7z", "http://www.7-zip.org/7z.html"),
        ("csvcut", "http://csvkit.rtfd.org/"),
        ("curl", "https://curl.haxx.se/"),
        ("filterline", "http://github.com/miku/filterline/releases"),
        ("flux.sh", "https://github.com/culturegraph/metafacture-core/wiki"),
        ("groupcover", "http://github.com/miku/groupcover/releases"),
        ("iconv", "https://linux.die.net/man/1/iconv"),
        ("iconv-chunks", "https://github.com/mla/iconv-chunks"),
        ("jq", "https://stedolan.github.io/jq/"),
        ("metha-sync", "http://github.com/miku/metha/releases"),
        ("pigz", "https://zlib.net/pigz/"),
        ("rclone", "https://rclone.org"),
        ("solrbulk", "http://github.com/miku/solrbulk/releases"),
        ("span-import", "http://github.com/miku/span/releases"),
        ("unzip", "http://www.info-zip.org/pub/infozip/"),
        ("wget", "https://www.gnu.org/software/wget/"),
        ("xmllint", "http://xmlsoft.org/xmllint.html"),
        ("yaz-marcdump", "http://www.indexdata.com/yaz"),
    ]
    for program, msg in sorted(deps):
        if which(program) is None:
            print(f"{FAIL}xx\t{ENDC}{program}\t{msg}")
        else:
            print(f"{OKGREEN}ok\t{ENDC}{program}")


def cmd_version():
    """Show the siskin version."""
    print(__version__)


def cmd_tags():
    """Show task tags (source IDs)."""
    _star_imports()
    if len(sys.argv) > 1 and sys.argv[1] == "-h":
        print("Tag", "Class")
    for name in Register.task_names():
        klass = Register.get_task_cls(name)
        if not hasattr(klass, "TAG"):
            continue
        print(f"{getattr(klass, 'TAG')}\t{name}")


# ---------------------------------------------------------------------------
# commands — ported from shell scripts
# ---------------------------------------------------------------------------

def cmd_cat():
    """Display task output, handling compressed formats transparently."""
    if len(sys.argv) < 2:
        print("usage: siskin cat TASKNAME [--param value ...]", file=sys.stderr)
        sys.exit(1)
    try:
        _cat_output(sys.argv[1:])
    except BrokenPipeError:
        pass


def cmd_help():
    """Show Luigi help for a task."""
    if len(sys.argv) < 2:
        print("usage: siskin help TASKNAME", file=sys.stderr)
        sys.exit(1)
    sys.argv.append("--help")
    cmd_run()


def cmd_rm():
    """Delete task output files."""
    if len(sys.argv) < 2:
        print("usage: siskin rm TASKNAME [--param value ...] [TASKNAME ...]", file=sys.stderr)
        sys.exit(0)

    # Single task name, no flags.
    if len(sys.argv) == 2:
        path = _get_output_path(sys.argv[1:])
        if os.path.exists(path):
            os.remove(path)
            print(f"removed '{path}'")
        return

    # Check if second arg is a flag.
    if sys.argv[2].startswith("--"):
        # Task with parameters.
        path = _get_output_path(sys.argv[1:])
        if os.path.exists(path):
            os.remove(path)
            print(f"removed '{path}'")
        else:
            print("[ok] nothing to remove", file=sys.stderr)
    else:
        # Multiple task names.
        for name in sys.argv[1:]:
            path = _get_output_path([name])
            if os.path.exists(path):
                os.remove(path)
                print(f"removed '{path}'")


def cmd_dir():
    """Show the directory path for a task's output."""
    if len(sys.argv) < 2:
        print("usage: siskin dir TASKNAME [--param value ...]", file=sys.stderr)
        sys.exit(1)
    print(_get_task_dir(sys.argv[1:]))


def cmd_du():
    """Show disk usage for a task (or the entire task home)."""
    if len(sys.argv) < 2:
        try:
            config = Config.instance()
            d = config.get("core", "home")
        except configparser.Error:
            print("cannot determine task home", file=sys.stderr)
            sys.exit(1)
    else:
        d = _get_task_dir(sys.argv[1:])
    subprocess.run(["du", "-hs", d])


def cmd_gc():
    """Garbage collection: remove obsolete task artifacts."""
    CO = "\033[0;33m"
    NC = "\033[0m"

    try:
        config = Config.instance()
        taskhome = config.get("core", "home")
    except configparser.Error:
        print("cannot determine task home", file=sys.stderr)
        sys.exit(1)

    tasks = {
        "ai": "AIApplyOpenAccessFlag AIExport AIIntermediateSchema AIIntermediateSchemaDeduplicated AILicensing AILocalData AIRedact AIInstitutionChanges",
        "28": "DOAJIntermediateSchemaDirty DOAJIntermediateSchema DOAJHarvest DOAJDownloadDump DOAJTable",
        "48": "GeniosIntermediateSchema GeniosCombinedIntermediateSchema",
        "55": "JstorXML JstorIntermediateSchemaGenericCollection JstorIntermediateSchema JstorMembers JstorLatestMembers",
        "85": "ElsevierJournalsIntermediateSchema ElsevierJournalsUpdatesIntermediateSchema",
        "89": "IEEEUpdatesIntermediateSchema IEEEIntermediateSchema",
        "126": "BaseSingleFile",
    }

    current_month_prefix = datetime.datetime.now().strftime("date-%Y-%m-")
    matching_files = []

    for src, task_names in tasks.items():
        for t in task_names.split():
            task_dir = os.path.join(taskhome, src, t)
            if not os.path.isdir(task_dir):
                continue
            for root, _, files in os.walk(task_dir):
                for f in files:
                    if not f.startswith(current_month_prefix):
                        matching_files.append(os.path.join(root, f))

    if not matching_files:
        print("Nothing to delete. Bye.")
        return

    for f in matching_files:
        print(f)

    if len(sys.argv) > 1 and sys.argv[1] == "--force":
        for f in matching_files:
            os.remove(f)
            print(f"removed '{f}'")
        return

    total_bytes = sum(os.path.getsize(f) for f in matching_files)
    freed_gb = total_bytes / 1073741824

    answer = input(
        f"\nDelete the above {len(matching_files)} file(s) "
        f"[{CO}{freed_gb:.2f}GB{NC} would be freed] [y/N]? "
    )
    if answer.lower().startswith("y"):
        for f in matching_files:
            os.remove(f)
            print(f"removed '{f}'")
    else:
        print("Not deleting anything. Bye.")


def cmd_head():
    """Show the first 10 lines of task output."""
    if len(sys.argv) < 2:
        print("usage: siskin head TASKNAME [--param value ...]", file=sys.stderr)
        sys.exit(1)
    path = _get_output_path(sys.argv[1:])
    if not os.path.exists(path):
        print(f"output does not exist: {path}", file=sys.stderr)
        sys.exit(1)
    # Use subprocess pipeline to handle all formats.
    cat_cmd = [sys.executable, "-m", "siskin.cli", "cat"] + sys.argv[1:]
    cat_proc = subprocess.Popen(cat_cmd, stdout=subprocess.PIPE)
    head_proc = subprocess.Popen(["head", "-10"], stdin=cat_proc.stdout)
    cat_proc.stdout.close()
    head_proc.communicate()


def cmd_less():
    """View task output with the less pager."""
    if len(sys.argv) < 2:
        print("usage: siskin less TASKNAME [--param value ...]", file=sys.stderr)
        sys.exit(1)
    cat_cmd = [sys.executable, "-m", "siskin.cli", "cat"] + sys.argv[1:]
    cat_proc = subprocess.Popen(cat_cmd, stdout=subprocess.PIPE)
    less_proc = subprocess.Popen(["less"], stdin=cat_proc.stdout)
    cat_proc.stdout.close()
    less_proc.communicate()


def cmd_ls():
    """List the task output file."""
    if len(sys.argv) < 2:
        print("usage: siskin ls TASKNAME [--param value ...]", file=sys.stderr)
        sys.exit(1)
    path = _get_output_path(sys.argv[1:])
    subprocess.run(["ls", "-lah", path])


def cmd_open():
    """Open task output with the default application."""
    if len(sys.argv) < 2:
        print("usage: siskin open TASKNAME [--param value ...]", file=sys.stderr)
        sys.exit(1)
    path = _get_output_path(sys.argv[1:])
    system = platform.system()
    if system == "Darwin":
        subprocess.run(["open", path])
    elif system == "Linux":
        opener = "xdg-open"
        if shutil.which("gnome-open"):
            opener = "gnome-open"
        subprocess.run([opener, path])
    else:
        print(f"unsupported platform: {system}", file=sys.stderr)
        sys.exit(1)


def cmd_redo():
    """Re-run a task (remove output, then run)."""
    if len(sys.argv) < 2:
        print("usage: siskin redo TASKNAME [--param value ...]", file=sys.stderr)
        sys.exit(1)
    # Remove output first (silently ignore if it doesn't exist).
    try:
        path = _get_output_path(sys.argv[1:])
        if os.path.exists(path):
            os.remove(path)
            print(f"removed '{path}'")
    except SystemExit:
        pass
    # Then run.
    cmd_run()


def cmd_status():
    """Check whether a task is done or not."""
    if len(sys.argv) < 2:
        print("usage: siskin status TASKNAME [--param value ...]", file=sys.stderr)
        sys.exit(1)
    path = _get_output_path(sys.argv[1:])
    if os.path.exists(path):
        print(f"DONE {path}")
    else:
        print(f"TODO {path}")
        sys.exit(1)


def cmd_sync():
    """Sync task outputs to remote servers."""
    for cmd in ("curl", "grep", "rsync", "ssh"):
        if not shutil.which(cmd):
            print(f"{cmd} required", file=sys.stderr)
            sys.exit(1)

    whatislive_url = "https://ai.ub.uni-leipzig.de/whatislive"
    try:
        r = requests.get(whatislive_url, timeout=10)
        r.raise_for_status()
    except Exception:
        print(f"cannot reach {whatislive_url}")
        sys.exit(1)

    user = os.environ.get("USER", "siskin")
    is_path = _get_output_path(["AIRedact"])
    solr_path = _get_output_path(["AIExport"])

    content = r.text
    mb_match = re.search(r"microblob_nonlive.*?(\d+\.\d+\.\d+\.\d+)", content)
    solr_match = re.search(r"solr_nonlive.*?(\d+\.\d+\.\d+\.\d+)", content)
    if not mb_match or not solr_match:
        print("cannot parse nonlive IPs from whatislive", file=sys.stderr)
        sys.exit(1)

    mb_ip = mb_match.group(1)
    solr_ip = solr_match.group(1)
    ssh_cmd = f"ssh -o 'PubkeyAcceptedKeyTypes +ssh-rsa' -i /home/{user}/.ssh/id_rsa"
    rsync_opts = "-avP"

    dry_run = os.environ.get("TASKSYNC", "no") == "no"
    cmds = [
        f'rsync {rsync_opts} -e "{ssh_cmd}" {is_path} {user}@{mb_ip}:/home/{user}',
        f'rsync {rsync_opts} -e "{ssh_cmd}" {solr_path} {user}@{solr_ip}:/home/{user}',
    ]
    for c in cmds:
        if dry_run:
            print(c)
        else:
            subprocess.run(c, shell=True)


def cmd_tree():
    """Display a directory tree for a task (or the entire task home)."""
    if len(sys.argv) < 2:
        try:
            config = Config.instance()
            d = config.get("core", "home")
        except configparser.Error:
            print("cannot determine task home", file=sys.stderr)
            sys.exit(1)
        subprocess.run(["tree", "-ash", d])
    else:
        d = _get_task_dir(sys.argv[1:])
        subprocess.run(["tree", "-sh", d])


def cmd_wc():
    """Count lines in task output."""
    if len(sys.argv) < 2:
        print("usage: siskin wc TASKNAME [--param value ...]", file=sys.stderr)
        sys.exit(1)
    cat_cmd = [sys.executable, "-m", "siskin.cli", "cat"] + sys.argv[1:]
    cat_proc = subprocess.Popen(cat_cmd, stdout=subprocess.PIPE)
    wc_proc = subprocess.Popen(["wc", "-l"], stdin=cat_proc.stdout)
    cat_proc.stdout.close()
    wc_proc.communicate()


# ---------------------------------------------------------------------------
# command registry
# ---------------------------------------------------------------------------

COMMANDS = {
    "run": cmd_run,
    "names": cmd_names,
    "inspect": cmd_inspect,
    "output": cmd_output,
    "config": cmd_config,
    "deps": cmd_deps,
    "deps-dot": cmd_deps_dot,
    "docs": cmd_docs,
    "cleanup": cmd_cleanup,
    "hash": cmd_hash,
    "ps": cmd_ps,
    "home": cmd_home,
    "importcache": cmd_importcache,
    "checksetup": cmd_checksetup,
    "version": cmd_version,
    "tags": cmd_tags,
    "cat": cmd_cat,
    "help": cmd_help,
    "rm": cmd_rm,
    "dir": cmd_dir,
    "du": cmd_du,
    "gc": cmd_gc,
    "head": cmd_head,
    "less": cmd_less,
    "ls": cmd_ls,
    "open": cmd_open,
    "redo": cmd_redo,
    "status": cmd_status,
    "sync": cmd_sync,
    "tree": cmd_tree,
    "wc": cmd_wc,
}

# Grouped for help display.
COMMAND_GROUPS = [
    ("Task execution", [
        ("run", "Run a Luigi task"),
        ("redo", "Re-run a task (remove output, then run)"),
        ("help", "Show Luigi help for a task"),
    ]),
    ("Task information", [
        ("names", "List all available task names"),
        ("docs", "Show documentation for all tasks"),
        ("tags", "Show task tags (source IDs)"),
        ("inspect", "Show the source code of a task"),
        ("deps", "Show the dependency tree (ASCII)"),
        ("deps-dot", "Show the dependency tree (Graphviz DOT)"),
        ("hash", "Calculate SHA1 hashes of task source code"),
    ]),
    ("Task output", [
        ("output", "Show the output path of a task"),
        ("dir", "Show the directory of a task's output"),
        ("status", "Check whether a task is done"),
        ("cat", "Display task output (handles compressed formats)"),
        ("head", "Show the first 10 lines of task output"),
        ("less", "View task output in the less pager"),
        ("ls", "List the task output file"),
        ("open", "Open task output with default application"),
        ("wc", "Count lines in task output"),
        ("rm", "Delete task output files"),
    ]),
    ("Maintenance", [
        ("cleanup", "Remove date-based task outputs before a date"),
        ("gc", "Garbage collection for old task artifacts"),
        ("du", "Show disk usage for a task"),
        ("tree", "Display a directory tree for a task"),
        ("sync", "Sync task outputs to remote servers"),
        ("ps", "Show running/pending/done tasks from scheduler"),
    ]),
    ("Configuration", [
        ("config", "Display the current configuration"),
        ("home", "Show the task home directory"),
        ("version", "Show the siskin version"),
        ("checksetup", "Check external tool dependencies"),
        ("importcache", "Show the task import cache path"),
    ]),
]


def print_usage():
    print(f"siskin {__version__} — Luigi task runner for finc metadata aggregation")
    print()
    print("Usage: siskin <command> [args...]")
    print()
    max_name_len = max(len(name) for _, cmds in COMMAND_GROUPS for name, _ in cmds)
    for group_name, cmds in COMMAND_GROUPS:
        print(f"  {group_name}:")
        for name, desc in cmds:
            print(f"    {name:<{max_name_len + 2}} {desc}")
        print()
    print("Run 'siskin <command> --help' for more information on a command.")
    print()
    print("Legacy commands (taskdo, taskcat, ...) are still available as aliases.")


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print_usage()
        return
    if sys.argv[1] in ("-V", "--version"):
        print(__version__)
        return

    subcmd = sys.argv[1]
    # Rewrite sys.argv so subcommand functions see their args as sys.argv[1:].
    sys.argv = [f"siskin-{subcmd}"] + sys.argv[2:]

    if subcmd in COMMANDS:
        COMMANDS[subcmd]()
    else:
        print(f"siskin: unknown command '{subcmd}'", file=sys.stderr)
        print(f"Run 'siskin --help' for a list of commands.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
