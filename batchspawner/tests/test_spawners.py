"""Test BatchSpawner and subclasses"""

import asyncio
import logging
import os
import pwd
import re
import time
from getpass import getuser
from unittest import mock

import pytest
from jupyterhub import orm
from jupyterhub.objects import Hub, Server
from jupyterhub.user import User
from traitlets import Unicode

from .. import BatchSpawnerRegexStates, JobStatus
from ..batchspawner import BatchCommandError

testhost = "userhost123"
testjob = "12345"
testport = 54321


@pytest.fixture(autouse=True)
def _always_get_my_home():
    # pwd.getbwnam() is always called with the current user
    # ignoring the requested name, which usually doesn't exist
    getpwnam = pwd.getpwnam
    with mock.patch.object(pwd, "getpwnam", lambda name: getpwnam(getuser())):
        yield


class BatchDummy(BatchSpawnerRegexStates):
    exec_prefix = ""
    batch_submit_cmd = Unicode("cat > /dev/null; echo " + testjob)
    batch_query_cmd = Unicode("echo RUN " + testhost)
    batch_cancel_cmd = Unicode("echo STOP")
    batch_script = Unicode("{cmd}")
    state_pending_re = Unicode("PEND")
    state_running_re = Unicode("RUN")
    state_exechost_re = Unicode("RUN (.*)$")
    state_unknown_re = Unicode("UNKNOWN")

    cmd_expectlist = None
    out_expectlist = None

    async def run_command(self, *args, **kwargs):
        """Overwriten run command to test templating and outputs"""
        cmd = args[0]
        # Test that the command matches the expectations
        if self.cmd_expectlist:
            run_re = self.cmd_expectlist.pop(0)
            if run_re:
                print("run:", run_re)
                assert (
                    run_re.search(cmd) is not None
                ), f"Failed test: re={run_re} cmd={cmd}"
        # Run command normally
        out = await super().run_command(*args, **kwargs)
        # Test that the command matches the expectations
        if self.out_expectlist:
            out_re = self.out_expectlist.pop(0)
            if out_re:
                print("out:", out_re)
                assert (
                    out_re.search(cmd) is not None
                ), f"Failed output: re={out_re} cmd={cmd} out={out}"
        return out


def new_spawner(db, spawner_class=BatchDummy, **kwargs):
    kwargs.setdefault("cmd", ["singleuser_command"])
    user = db.query(orm.User).first()
    hub = Hub()
    user = User(user, {})
    server = Server()
    # Set it after constructions because it isn't a traitlet.
    kwargs.setdefault("hub", hub)
    kwargs.setdefault("user", user)
    kwargs.setdefault("poll_interval", 1)

    # These are not traitlets so we have to set them here
    spawner = user._new_spawner("", spawner_class=spawner_class, **kwargs)
    spawner.server = server
    spawner.mock_port = testport
    return spawner


def check_ip(spawner, value):
    assert spawner.ip == value


async def test_spawner_start_stop_poll(db):
    spawner = new_spawner(db=db)

    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1
    assert spawner.job_id == ""
    assert spawner.get_state() == {}

    await asyncio.wait_for(spawner.start(), timeout=5)
    check_ip(spawner, testhost)
    assert spawner.job_id == testjob

    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status is None
    spawner.batch_query_cmd = "echo NOPE"
    await asyncio.wait_for(spawner.stop(), timeout=5)
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1
    assert spawner.get_state() == {}


async def test_stress_submit(db):
    for i in range(200):
        time.sleep(0.01)
        test_spawner_start_stop_poll(db)


async def test_spawner_state_reload(db):
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}

    await asyncio.wait_for(spawner.start(), timeout=30)
    check_ip(spawner, testhost)
    assert spawner.job_id == testjob

    state = spawner.get_state()
    assert state == dict(job_id=testjob, job_status="RUN " + testhost)
    spawner = new_spawner(db=db)
    spawner.clear_state()
    assert spawner.get_state() == {}
    spawner.load_state(state)
    # We used to check IP here, but that is actually only computed on start(),
    # and is not part of the spawner's persistent state
    assert spawner.job_id == testjob


async def test_submit_failure(db):
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    spawner.batch_submit_cmd = "cat > /dev/null; true"
    with pytest.raises(RuntimeError):
        await asyncio.wait_for(spawner.start(), timeout=30)
    assert spawner.job_id == ""
    assert spawner.job_status == ""


async def test_submit_pending_fails(db):
    """Submission works, but the batch query command immediately fails"""
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    spawner.batch_query_cmd = "echo xyz"
    with pytest.raises(RuntimeError):
        await asyncio.wait_for(spawner.start(), timeout=30)
    status = await asyncio.wait_for(spawner.query_job_status(), timeout=30)
    assert status == JobStatus.NOTFOUND
    assert spawner.job_id == ""
    assert spawner.job_status == ""


async def test_poll_fails(db):
    """Submission works, but a later .poll() fails"""
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    # The start is successful:
    await asyncio.wait_for(spawner.start(), timeout=30)
    spawner.batch_query_cmd = "echo xyz"
    # Now, the poll fails:
    await asyncio.wait_for(spawner.poll(), timeout=30)
    # .poll() will run self.clear_state() if it's not found:
    assert spawner.job_id == ""
    assert spawner.job_status == ""


async def test_unknown_status(db):
    """Polling returns an unknown status"""
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    # The start is successful:
    await asyncio.wait_for(spawner.start(), timeout=30)
    spawner.batch_query_cmd = "echo UNKNOWN"
    # This poll should not fail:
    await asyncio.wait_for(spawner.poll(), timeout=30)
    status = await asyncio.wait_for(spawner.query_job_status(), timeout=30)
    assert status == JobStatus.UNKNOWN
    assert spawner.job_id == "12345"
    assert spawner.job_status != ""


async def test_templates(db):
    """Test templates in the run_command commands"""
    spawner = new_spawner(db=db)

    # Test when not running
    spawner.cmd_expectlist = [
        re.compile(".*RUN"),
    ]
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1
    assert spawner.job_id == ""
    assert spawner.get_state() == {}

    # Test starting
    spawner.cmd_expectlist = [
        re.compile(".*echo"),
        re.compile(".*RUN"),
    ]
    await asyncio.wait_for(spawner.start(), timeout=5)
    check_ip(spawner, testhost)
    assert spawner.job_id == testjob

    # Test poll - running
    spawner.cmd_expectlist = [
        re.compile(".*RUN"),
    ]
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status is None

    # Test stopping
    spawner.batch_query_cmd = "echo NOPE"
    spawner.cmd_expectlist = [
        re.compile(".*STOP"),
        re.compile(".*NOPE"),
    ]
    await asyncio.wait_for(spawner.stop(), timeout=5)
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1
    assert spawner.get_state() == {}


async def test_batch_script(db):
    """Test that the batch script substitutes {cmd}"""

    class BatchDummyTestScript(BatchDummy):
        async def _get_batch_script(self, **subvars):
            script = await super()._get_batch_script(**subvars)
            assert "singleuser_command" in script
            return script

    spawner = new_spawner(db=db, spawner_class=BatchDummyTestScript)
    # status = await asyncio.wait_for(spawner.poll(), timeout=5)
    await asyncio.wait_for(spawner.start(), timeout=5)
    # status = await asyncio.wait_for(spawner.poll(), timeout=5)
    # await asyncio.wait_for(spawner.stop(), timeout=5)


async def test_exec_prefix(db):
    """Test that all run_commands have exec_prefix"""

    class BatchDummyTestScript(BatchDummy):
        exec_prefix = "PREFIX"

        async def run_command(self, cmd, *args, **kwargs):
            assert cmd.startswith("PREFIX ")
            cmd = cmd[7:]
            print(cmd)
            out = await super().run_command(cmd, *args, **kwargs)
            return out

    spawner = new_spawner(db=db, spawner_class=BatchDummyTestScript)
    # Not running
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1
    # Start
    await asyncio.wait_for(spawner.start(), timeout=5)
    assert spawner.job_id == testjob
    # Poll
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status is None
    # Stop
    spawner.batch_query_cmd = "echo NOPE"
    await asyncio.wait_for(spawner.stop(), timeout=5)
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1


async def run_spawner_script(
    db, spawner, script, batch_script_re_list=None, spawner_kwargs={}
):
    """Run a spawner script and test that the output and behavior is as expected.

    db: same as in this module
    spawner: the BatchSpawnerBase subclass to test
    script: list of (input_re_to_match, output)
    batch_script_re_list: if given, assert batch script matches all of these
    """
    # Create the expected scripts
    cmd_expectlist, out_list = zip(*script)
    cmd_expectlist = list(cmd_expectlist)
    out_list = list(out_list)

    class BatchDummyTestScript(spawner):
        async def run_command(self, cmd, input=None, env=None):
            # Test the input
            run_re = cmd_expectlist.pop(0)
            if run_re:
                print(f'run: "{cmd}"   [{run_re}]')
                assert (
                    run_re.search(cmd) is not None
                ), f"Failed test: re={run_re} cmd={cmd}"
            # Test the stdin - will only be the batch script.  For
            # each regular expression in batch_script_re_list, assert that
            # each re in that list matches the batch script.
            if batch_script_re_list and input:
                batch_script = input
                for match_re in batch_script_re_list:
                    assert (
                        match_re.search(batch_script) is not None
                    ), f"Batch script does not match {match_re}"
            # Return expected output.
            out = out_list.pop(0)
            print("  --> " + out)
            return out

    spawner = new_spawner(db=db, spawner_class=BatchDummyTestScript, **spawner_kwargs)
    # Not running at beginning (no command run)
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1
    # batch_submit_cmd
    # batch_query_cmd    (result=pending)
    # batch_query_cmd    (result=running)
    await asyncio.wait_for(spawner.start(), timeout=5)
    assert spawner.job_id == testjob
    check_ip(spawner, testhost)
    # batch_query_cmd
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status is None
    # batch_cancel_cmd
    await asyncio.wait_for(spawner.stop(), timeout=5)
    # batch_poll_cmd
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1


async def test_torque(db):
    spawner_kwargs = {
        "req_nprocs": "5",
        "req_memory": "5678",
        "req_options": "some_option_asdf",
        "req_prologue": "PROLOGUE",
        "req_epilogue": "EPILOGUE",
    }
    batch_script_re_list = [
        re.compile(
            r"^PROLOGUE.*^batchspawner-singleuser singleuser_command.*^EPILOGUE",
            re.S | re.M,
        ),
        re.compile(r"mem=5678"),
        re.compile(r"ppn=5"),
        re.compile(r"^#PBS some_option_asdf", re.M),
    ]
    poll_running = (
        re.compile(r"sudo.*qstat"),
        f"<job_state>R</job_state><exec_host>{testhost}/1</exec_host>",
    )
    script = [
        (re.compile(r"sudo.*qsub"), str(testjob)),
        (
            re.compile(r"sudo.*qstat"),
            "<job_state>Q</job_state><exec_host></exec_host>",
        ),  # pending
        poll_running,
        poll_running,
        poll_running,
        (re.compile(r"sudo.*qdel"), "STOP"),
        (re.compile(r"sudo.*qstat"), ""),
    ]
    from .. import TorqueSpawner

    await run_spawner_script(
        db,
        TorqueSpawner,
        script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


async def test_moab(db):
    spawner_kwargs = {
        "req_nprocs": "5",
        "req_memory": "5678",
        "req_options": "some_option_asdf",
        "req_prologue": "PROLOGUE",
        "req_epilogue": "EPILOGUE",
    }
    batch_script_re_list = [
        re.compile(
            r"^PROLOGUE.*^batchspawner-singleuser singleuser_command.*^EPILOGUE",
            re.S | re.M,
        ),
        re.compile(r"mem=5678"),
        re.compile(r"ppn=5"),
        re.compile(r"^#PBS some_option_asdf", re.M),
    ]
    poll_running = (
        re.compile(r"sudo.*mdiag"),
        f'State="Running" AllocNodeList="{testhost}"',
    )
    script = [
        (re.compile(r"sudo.*msub"), str(testjob)),
        (re.compile(r"sudo.*mdiag"), 'State="Idle"'),  # pending
        poll_running,
        poll_running,
        poll_running,
        (re.compile(r"sudo.*mjobctl.*-c"), "STOP"),
        (re.compile(r"sudo.*mdiag"), ""),
    ]
    from .. import MoabSpawner

    await run_spawner_script(
        db,
        MoabSpawner,
        script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


async def test_pbs(db):
    spawner_kwargs = {
        "req_nprocs": "4",
        "req_memory": "10256",
        "req_options": "some_option_asdf",
        "req_host": "some_pbs_admin_node",
        "req_runtime": "08:00:00",
    }
    batch_script_re_list = [
        re.compile(r"singleuser_command"),
        re.compile(r"select=1"),
        re.compile(r"ncpus=4"),
        re.compile(r"mem=10256"),
        re.compile(r"walltime=08:00:00"),
        re.compile(r"@some_pbs_admin_node"),
        re.compile(r"^#PBS some_option_asdf", re.M),
    ]
    poll_running = (
        re.compile(r"sudo.*qstat"),
        f"job_state = R\nexec_host = {testhost}/2*1",
    )
    script = [
        (re.compile(r"sudo.*qsub"), str(testjob)),
        (re.compile(r"sudo.*qstat"), "job_state = Q"),  # pending
        poll_running,
        poll_running,
        poll_running,
        (re.compile(r"sudo.*qdel"), "STOP"),
        (re.compile(r"sudo.*qstat"), ""),
    ]
    from .. import PBSSpawner

    await run_spawner_script(
        db,
        PBSSpawner,
        script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


async def test_slurm(db):
    spawner_kwargs = {
        "req_runtime": "3-05:10:10",
        "req_nprocs": "5",
        "req_memory": "5678",
        "req_options": "some_option_asdf",
        "req_prologue": "PROLOGUE",
        "req_epilogue": "EPILOGUE",
        "req_reservation": "RES123",
        "req_gres": "GRES123",
    }
    batch_script_re_list = [
        re.compile(
            r"PROLOGUE.*srun batchspawner-singleuser singleuser_command.*EPILOGUE", re.S
        ),
        re.compile(r"^\#SBATCH \s+ --cpus-per-task=5", re.X | re.M),
        re.compile(r"^\#SBATCH \s+ --time=3-05:10:10", re.X | re.M),
        re.compile(r"^\#SBATCH \s+ some_option_asdf", re.X | re.M),
        re.compile(r"^\#SBATCH \s+ --reservation=RES123", re.X | re.M),
        re.compile(r"^\#SBATCH \s+ --gres=GRES123", re.X | re.M),
    ]
    from .. import SlurmSpawner

    await run_spawner_script(
        db,
        SlurmSpawner,
        normal_slurm_script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


# We tend to use slurm as our typical example job.  These allow quick
# Slurm examples.
normal_slurm_script = [
    (re.compile(r"sudo.*sbatch"), str(testjob)),
    (re.compile(r"sudo.*squeue"), "PENDING "),  # pending
    (
        re.compile(r"sudo.*squeue"),
        "slurm_load_jobs error: Unable to contact slurm controller",
    ),  # unknown
    (re.compile(r"sudo.*squeue"), "RUNNING " + testhost),  # running
    (re.compile(r"sudo.*squeue"), "RUNNING " + testhost),
    (re.compile(r"sudo.*squeue"), "RUNNING " + testhost),
    (re.compile(r"sudo.*scancel"), "STOP"),
    (re.compile(r"sudo.*squeue"), ""),  # stop() confirms the job is gone
    (re.compile(r"sudo.*squeue"), ""),  # the poll() after stop() finds nothing
]
from .. import SlurmSpawner


async def run_typical_slurm_spawner(
    db,
    spawner=SlurmSpawner,
    script=normal_slurm_script,
    batch_script_re_list=None,
    spawner_kwargs={},
):
    """Run a full slurm job with default (overrideable) parameters.

    This is useful, for example, for changing options and testing effect
    of batch scripts.
    """
    return await run_spawner_script(
        db,
        spawner,
        script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


# async def test_gridengine(db):
#    spawner_kwargs = {
#        'req_options': 'some_option_asdf',
#        }
#    batch_script_re_list = [
#        re.compile(r'singleuser_command'),
#        re.compile(r'#$\s+some_option_asdf'),
#        ]
#    script = [
#        (re.compile(r'sudo.*qsub'),   'x x '+str(testjob)),
#        (re.compile(r'sudo.*qstat'),   'PENDING '),
#        (re.compile(r'sudo.*qstat'),   'RUNNING '+testhost),
#        (re.compile(r'sudo.*qstat'),   'RUNNING '+testhost),
#        (re.compile(r'sudo.*qdel'),  'STOP'),
#        (re.compile(r'sudo.*qstat'),   ''),
#        ]
#    from .. import GridengineSpawner
#    await run_spawner_script(db, GridengineSpawner, script,
#                       batch_script_re_list=batch_script_re_list,
#                       spawner_kwargs=spawner_kwargs)


async def test_condor(db):
    spawner_kwargs = {
        "req_nprocs": "5",
        "req_memory": "5678",
        "req_options": "some_option_asdf",
    }
    batch_script_re_list = [
        re.compile(r"exec batchspawner-singleuser singleuser_command"),
        re.compile(r"RequestCpus = 5"),
        re.compile(r"RequestMemory = 5678"),
        re.compile(r"^some_option_asdf", re.M),
    ]
    script = [
        (
            re.compile(r"sudo.*condor_submit"),
            f"submitted to cluster {str(testjob)}",
        ),
        (re.compile(r"sudo.*condor_q"), "1,"),  # pending
        (re.compile(r"sudo.*condor_q"), f"2, @{testhost}"),  # runing
        (re.compile(r"sudo.*condor_q"), f"2, @{testhost}"),
        (re.compile(r"sudo.*condor_q"), f"2, @{testhost}"),
        (re.compile(r"sudo.*condor_rm"), "STOP"),
        (re.compile(r"sudo.*condor_q"), ""),
    ]
    from .. import CondorSpawner

    await run_spawner_script(
        db,
        CondorSpawner,
        script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


async def test_lfs(db):
    spawner_kwargs = {
        "req_nprocs": "5",
        "req_memory": "5678",
        "req_options": "some_option_asdf",
        "req_queue": "some_queue",
        "req_prologue": "PROLOGUE",
        "req_epilogue": "EPILOGUE",
    }
    batch_script_re_list = [
        re.compile(
            r"^PROLOGUE.*^batchspawner-singleuser singleuser_command.*^EPILOGUE",
            re.S | re.M,
        ),
        re.compile(r"#BSUB\s+-q\s+some_queue", re.M),
    ]
    script = [
        (
            re.compile(r"sudo.*bsub"),
            f"Job <{str(testjob)}> is submitted to default queue <normal>",
        ),
        (re.compile(r"sudo.*bjobs"), "PEND "),  # pending
        (re.compile(r"sudo.*bjobs"), f"RUN {testhost}"),  # running
        (re.compile(r"sudo.*bjobs"), f"RUN {testhost}"),
        (re.compile(r"sudo.*bjobs"), f"RUN {testhost}"),
        (re.compile(r"sudo.*bkill"), "STOP"),
        (re.compile(r"sudo.*bjobs"), ""),
    ]
    from .. import LsfSpawner

    await run_spawner_script(
        db,
        LsfSpawner,
        script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


async def test_keepvars(db):
    # req_keepvars
    spawner_kwargs = {
        "req_keepvars": "ABCDE",
    }
    batch_script_re_list = [
        re.compile(r"--export=ABCDE", re.X | re.M),
    ]
    await run_typical_slurm_spawner(
        db,
        spawner_kwargs=spawner_kwargs,
        batch_script_re_list=batch_script_re_list,
    )

    # req_keepvars AND req_keepvars together
    spawner_kwargs = {
        "req_keepvars": "ABCDE",
        "req_keepvars_extra": "XYZ",
    }
    batch_script_re_list = [
        re.compile(r"--export=ABCDE,XYZ", re.X | re.M),
    ]
    await run_typical_slurm_spawner(
        db,
        spawner_kwargs=spawner_kwargs,
        batch_script_re_list=batch_script_re_list,
    )


async def test_early_stop(db):
    script = [
        (re.compile(r"sudo.*sbatch"), str(testjob)),
        (re.compile(r"sudo.*squeue"), "PENDING "),  # pending
        (
            re.compile(r"sudo.*squeue"),
            "slurm_load_jobs error: Unable to contact slurm controller",
        ),  # unknown
        # job exits early during start
        (re.compile(r"sudo.*squeue"), ""),
        (re.compile(r"sudo.*scancel"), "STOP"),
    ]
    with pytest.raises(RuntimeError, match="job has disappeared"):
        await run_spawner_script(db, SlurmSpawner, script)


# --- Robustness: timeouts, cancellation, scheduler outages, cancel handling ---


class BatchDummyGone(BatchDummy):
    """Like BatchDummy, but can tell 'job finished' apart from 'scheduler unreachable'."""

    state_notfound_re = Unicode("Invalid job id")


async def test_run_command_times_out(db):
    spawner = new_spawner(db=db)
    spawner.command_timeout = 0.5
    started = time.monotonic()
    with pytest.raises(BatchCommandError) as excinfo:
        await spawner.run_command("sleep 30")
    assert excinfo.value.timed_out
    assert time.monotonic() - started < 10


async def test_run_command_failure_is_runtime_error(db):
    """BatchCommandError stays catchable as RuntimeError, with stderr as the message."""
    spawner = new_spawner(db=db)
    with pytest.raises(RuntimeError) as excinfo:
        await spawner.run_command("echo oops >&2; exit 3")
    assert excinfo.value.args[0] == "oops"
    assert excinfo.value.returncode == 3


async def test_run_command_propagates_cancellation(db):
    """Cancelling (e.g. the Hub's start timeout) must raise CancelledError, not RuntimeError."""
    spawner = new_spawner(db=db)
    task = asyncio.ensure_future(spawner.run_command("sleep 30"))
    await asyncio.sleep(0.3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_query_timeout_is_tolerated_then_gives_up(db):
    spawner = new_spawner(db=db)
    spawner.query_failure_tolerance = 2
    await asyncio.wait_for(spawner.start(), timeout=30)
    good_status = spawner.job_status
    spawner.command_timeout = 0.3
    spawner.batch_query_cmd = "sleep 30"

    # Two timeouts are reported as UNKNOWN and keep the server and its last good status
    for _ in range(2):
        assert await spawner.poll() is None
        assert spawner.job_id == testjob
        assert spawner.job_status == good_status
    # The third gives up
    assert await spawner.poll() == 1
    assert spawner.job_id == ""


async def test_query_outage_is_tolerated_when_job_gone_is_detectable(db):
    spawner = new_spawner(db=db, spawner_class=BatchDummyGone)
    await asyncio.wait_for(spawner.start(), timeout=30)
    spawner.batch_query_cmd = "echo 'Problem talking to database' >&2; exit 1"

    assert await spawner.poll() is None
    assert spawner.job_id == testjob
    assert spawner.query_failures == 1

    # Recovery resets the counter
    spawner.batch_query_cmd = "echo RUN " + testhost
    assert await spawner.poll() is None
    assert spawner.query_failures == 0


async def test_finished_job_is_not_found_immediately(db):
    spawner = new_spawner(db=db, spawner_class=BatchDummyGone)
    await asyncio.wait_for(spawner.start(), timeout=30)
    spawner.batch_query_cmd = "echo 'slurm_load_jobs error: Invalid job id specified' >&2; exit 1"

    assert await spawner.poll() == 1
    assert spawner.job_id == ""


async def test_query_failure_without_gone_detection_keeps_old_behaviour(db):
    """Spawners that do not define state_notfound_re still treat a failed query as not found."""
    spawner = new_spawner(db=db)
    await asyncio.wait_for(spawner.start(), timeout=30)
    spawner.batch_query_cmd = "echo broken >&2; exit 1"

    assert await spawner.poll() == 1
    assert spawner.job_id == ""


async def test_tolerance_zero_disables_tolerance(db):
    spawner = new_spawner(db=db, spawner_class=BatchDummyGone)
    spawner.query_failure_tolerance = 0
    await asyncio.wait_for(spawner.start(), timeout=30)
    spawner.batch_query_cmd = "echo outage >&2; exit 1"

    assert await spawner.poll() == 1


def test_slurm_spawner_recognises_finished_jobs():
    from .. import SlurmSpawner

    assert SlurmSpawner.state_notfound_re.default_value == "Invalid job id specified"


async def test_stop_without_job_id_runs_no_cancel_command(db):
    """After a failed submit there is nothing to cancel; stop() must not run scancel/qdel."""
    spawner = new_spawner(db=db)
    spawner.batch_cancel_cmd = "echo cancel-ran >&2; exit 1"
    assert spawner.job_id == ""
    await asyncio.wait_for(spawner.stop(now=True), timeout=5)
    await asyncio.wait_for(spawner.stop(), timeout=5)


async def test_cancel_of_finished_job_is_not_an_error(db):
    spawner = new_spawner(db=db, spawner_class=BatchDummyGone)
    await asyncio.wait_for(spawner.start(), timeout=30)
    spawner.batch_cancel_cmd = "echo 'Invalid job id specified' >&2; exit 1"
    spawner.batch_query_cmd = "echo NOPE"

    await asyncio.wait_for(spawner.stop(), timeout=10)


async def test_cancel_failure_is_still_raised(db):
    spawner = new_spawner(db=db, spawner_class=BatchDummyGone)
    await asyncio.wait_for(spawner.start(), timeout=30)
    spawner.batch_cancel_cmd = "echo 'permission denied' >&2; exit 1"

    with pytest.raises(RuntimeError):
        await asyncio.wait_for(spawner.stop(now=True), timeout=10)


async def test_batch_command_env_applies_to_all_commands(db):
    spawner = new_spawner(db=db)
    spawner.batch_command_env = {"BS_TEST_VALUE": "pinned"}
    # query runs with the Hub's environment plus batch_command_env; submit and cancel merge it
    # into the spawner environment the same way (see _command_env)
    spawner.batch_query_cmd = "echo RUN $BS_TEST_VALUE"

    await asyncio.wait_for(spawner.start(), timeout=30)
    check_ip(spawner, "pinned")


async def test_secrets_not_logged_when_submitting(db, caplog):
    spawner = new_spawner(db=db)
    secret = "s3cr3t-token-value"
    env = {"PATH": os.environ.get("PATH", ""), "JUPYTERHUB_API_TOKEN": secret}
    spawner.get_env = lambda: dict(env)

    with caplog.at_level(logging.DEBUG):
        await asyncio.wait_for(spawner.start(), timeout=30)

    assert secret not in caplog.text
    assert "JUPYTERHUB_API_TOKEN" in caplog.text  # names are still logged


# --- Compatibility with Slurm 25.05 and current JupyterHub ---


def test_slurm_script_get_user_env_takes_no_argument():
    """Slurm 25.05 rejects '--get-user-env=<mode>' ("option doesn't allow an argument"); the bare
    flag works on old and new versions."""
    from .. import SlurmSpawner

    script = SlurmSpawner.batch_script.default_value
    assert re.search(r"^#SBATCH --get-user-env$", script, re.M)
    assert "--get-user-env=" not in script


async def test_start_does_not_use_deprecated_spawner_db(db):
    """Spawner.db is deprecated and slated for removal; start() must not touch it."""

    class NoDb(BatchDummy):
        @property
        def db(self):
            raise AssertionError("start() must not use the deprecated Spawner.db")

        @db.setter
        def db(self, value):
            pass

    spawner = new_spawner(db=db, spawner_class=NoDb)
    await asyncio.wait_for(spawner.start(), timeout=30)
    assert spawner.job_id == testjob


def test_slurm_recognises_finished_job_messages(db):
    """Messages seen from Slurm for a job that no longer exists (squeue -M / scancel -M, Slurm 25.x)."""
    from .. import SlurmSpawner

    spawner = new_spawner(db=db, spawner_class=SlurmSpawner)
    for message in (
        "slurm_load_jobs error: Invalid job id specified",
        "scancel: error: Kill job error on job id 43316: Invalid job id specified",
    ):
        assert spawner.state_isgone(message), message
    # Scheduler/database outages must NOT count as a finished job
    for message in (
        "slurm_load_jobs error: Unable to contact slurm controller",
        "squeue: error: Problem talking to database",
        "",
    ):
        assert not spawner.state_isgone(message), message
