#!/usr/bin/env python2.7
'''Reset MCDRAM configuration and NUMA mode on KNL nodes. If we are on a Cray,
this will use CAPMC controls, otherwise we're using Intel controls.

Must run at sufficient privilege to access memory mode modification commands and
node reboot commands (CAPMC or IPMI needed)

The CAPMC module should be loaded on Cray systems.

'''
import sys


import subprocess
from subprocess import Popen, PIPE, STDOUT
import time
import json

SUCCESS = 0
RESET_FAILURE = 3
BOOT_FAILURE = 4
BAD_ARGS = 5

DEFAULT_MCDRAM = 'cache'
DEFAULT_NUMA = 'a2a'

AVAILABLE_MCDRAM = ['cache', 'hybrid', 'half', 'flat']
AVAILABLE_NUMA = ['a2a', 'snc2', 'snc4', 'hemi', 'quad']

TIMEOUT = 900 # reboot timeout in seconds

POLL_INT = 0.25

CAPMC_CMD = '/opt/cray/capmc/default/bin/capmc'


def expand_num_list(num_list):
    '''Take a compact, comma-seperated string of integer values and ranges and
    expand to a list of integers that is represented by that string.  Ranges of
    the form "a-b" will be expanded to the full sequience of integers from a to
    b, inclusive.

    '''
    retlist = []
    elems = num_list.split(',')
    for elem in elems:
        if elem == '':
            continue
        elif len(elem.split('-')) == 1:
            retlist.append(int(elem))
        else:
            nums = elem.split('-')
            low = min(int(nums[0]), int(nums[1]))
            high = max(int(nums[0]), int(nums[1])) + 1
            retlist.extend(xrange(low, high))
    return retlist

def get_current_modes():

    return 'mode1', 'mode2'

def exec_fetch_output(cmd, args, timeout=None):
    '''execute commands and return stdout/stderr tuple.

    Raise exception in the event of a nonzero return.

    '''
    endtime = None
    timeout_trip = False
    if timeout is not None:
        endtime = int(time.time()) + int(timeout)
    proc = Popen(cmd, args, stdout=PIPE, stderr=PIPE)
    while(True):
        if endtime is not None and int(time.time()) >= endtime:
            #signal and kill
            timeout_trip
            proc.terminate()
            break
        #check to see if the process has terminated.
        if proc.poll() is not None:
            break
        time.sleep(POLL_INT)

    stdout, stderr = proc.communicate()
    if timeout_trip:
        raise RuntimeError("%s timed out!" % cmd)
    if proc.returncode != 0:
        raise RuntimeError("%s failed with an exit status of %s" % (cmd, proc.returncode))
    return (stdout, stderr)


def reset_modes(node_list, mcdram_mode, numa_mode):
    '''execute commands to reconfigure KNLs'''
    try:
        exec_fetch_output(CAPMC_CMD,
            ['set_mcdram_cfg', '--mode', mcdram_mode, '--nids', node_list])
    except RuntimeError:
        print >> sys.stderr, "Could not reset mcdram_mode"
        return False
    try:
        exec_fetch_output(CAPMC_CMD,
            ['set_numa_cfg', '--mode', numa_mode, '--nids', node_list])
    except RuntimeError:
        print >> sys.stderr, "Could not reset numa_mode"
        return False
    return True

def reboot_nodes(node_list, mcdram_mode, numa_mode):
    '''Initiate reboot of node list.  This call doesn't block.  Check with
    reboot complete.

    '''
    try:
        exec_fetch_output(CAPMC_CMD,
            ['node_reinit', '--nids', node_list, '-r',
             'Reset MCDRAM/NUMA mode to %s/%s' % (mcdram_mode, numa_mode)])
    except RuntimeError:
        print >> sys.stderr, "Unable to initiate node reboot"
        return False
    return True

def reboot_complete(node_list, timeout):
    endtime = int(time.time()) + int(timeout)
    exp_nodelist = set(expand_num_list(node_list))
    while(True):
        if int(time.time()) > endtime:
            print >> sys.stderr, "Reboot timed out."
            return False
        try:
            stdout, stderr = exec_fetch_output(CAPMC_CMD,
                    ['node_status', '--nids', node_list, '--filter', 'show_ready'])
        except RuntimeError as exc:
            print >> sys.stderr, "Unable to complete reboot: %s" % exc.message
            return False
        node_info = json.loads(stdout)
        if not (set(node_info['ready']) - set(exp_nodelist)):
            break
    return True

def main():

    mcdram_mode = DEFAULT_MCDRAM
    numa_mode = DEFAULT_NUMA
    node_list = []
    for arg in sys.argv:
        try:
            splitarg = arg.split('=')
            key = splitarg[0]
            val = '='.join(splitarg[1:])
        except (IndexError, ValueError, TypeError):
            print >> sys.stderr, "Missing or badly formatted args."
            return BAD_ARGS
        if key == 'mcdram':
            if val.lower() in AVAILABLE_MCDRAM:
                mcdram_mode = val
            else:
                print >> sys.stderr, "%s is an invalid MCDRAM mode" % val
                return BAD_ARGS
        elif key == 'numa':
            if val.lower() in AVAILABLE_NUMA:
                numa_mode = val
            else:
                print >> sys.stderr, "%s is an invalid NUMA mode" % val
                return BAD_ARGS
        else:
            pass

    current_mcdram, current_numa = get_current_modes()
    # assuming that mode change is immediately followed by reboot.  Modify when
    # current setting inspection available.
    if node_list != [] and (not (mcdram_mode != current_mcdram or
                                 numa_mode != current_numa)):
        if not reset_modes(node_list, mcdram_mode, numa_mode):
            print >> sys.stderr, "Failed to reset memory mode"
            return RESET_FAILURE
        if not reboot_nodes(node_list, mcdram_mode, numa_mode):
            print >> sys.stderr, "Failed to initiate node reboot"
            return BOOT_FAILURE
        if not reboot_complete(node_list, TIMEOUT):
            print >> sys.stderr, "Node reboot Failed to complete"
            return BOOT_FAILURE

    return SUCCESS



if __name__ == '__main__':
   sys.exit(main)
