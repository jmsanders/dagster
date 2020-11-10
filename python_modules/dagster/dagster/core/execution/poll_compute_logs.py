from __future__ import print_function

import os
import sys
import time

from dagster.utils import delay_interrupts, pop_delayed_interrupts

POLLING_INTERVAL = 0.1


def current_process_is_orphaned(parent_pid):
    parent_pid = int(parent_pid)
    if sys.platform == "win32":
        import psutil  # pylint: disable=import-error

        try:
            parent = psutil.Process(parent_pid)
            status = parent.status()
            if status != psutil.STATUS_RUNNING:
                print("PARENT STATUS: " + repr(status) + "\n")
                return True
            else:
                return False
        except psutil.NoSuchProcess:
            print("NO SUCH PROCESS: " + repr(parent_pid) + "\n")
            return True

    else:
        return os.getppid() != parent_pid


def tail_polling(filepath, stream=sys.stdout, parent_pid=None):
    """
    Tails a file and outputs the content to the specified stream via polling.
    The pid of the parent process (if provided) is checked to see if the tail process should be
    terminated, in case the parent is hard-killed / segfaults
    """
    with open(filepath, "r") as file:
        for block in iter(lambda: file.read(1024), None):
            if pop_delayed_interrupts():
                print(str(time.time()) + " : " + "GET AN INTERRUPT, QUITTING\n")
                return
            if block:
                print(block, end="", file=stream)  # pylint: disable=print-call
            else:
                if parent_pid and current_process_is_orphaned(parent_pid):
                    print(str(time.time()) + " : " + "CURRENT PROCESS IS ORPHANED, QUITTING\n")
                    return
                time.sleep(POLLING_INTERVAL)


def execute_polling(args):
    if not args or len(args) != 2:
        return

    filepath = args[0]
    parent_pid = int(args[1])

    print(
        str(time.time())
        + " : "
        + "STARTING, MY PID: "
        + str(os.getpid())
        + ", ARGS: "
        + repr(args)
        + "\n"
    )

    tail_polling(filepath, sys.stdout, parent_pid)

    print("FINISHING, MY PID: " + str(os.getpid()) + ", ARGS: " + repr(args) + "\n")


if __name__ == "__main__":

    print(
        str(time.time())
        + " : "
        + "HERE WE ARE AT THE BEGINNING OF POLL_COMPUTE_LOGS FOR "
        + repr(sys.argv[1:])
    )

    with delay_interrupts(raise_afterwards=False):
        execute_polling(sys.argv[1:])

    print(str(time.time()) + " : " + "ALL DONE GOODBYE\n")
