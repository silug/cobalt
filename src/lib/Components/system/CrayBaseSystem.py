"""Base Resource management for Cray ALPS based systems

All bridge calls are in the final implementation so we can leverage most of this
in the simulator.

Calls that require implementation:


"""

import logging
import threading
import time
import sys
import xmlrpclib
import json
import ConfigParser
import xml.etree.ElementTree
from xml.etree.ElementTree import Element
import xml.dom.minidom
import Cobalt.Util
from Cobalt.Components.base import Component, exposed, automatic, query, locking
from Cobalt.Components.system.base_system import BaseSystem
from Cobalt.Components.system.base_pg_manager import ProcessGroupManager
from Cobalt.Components.system.ALPSProcessGroup import ALPSProcessGroup
from Cobalt.Exceptions import ComponentLookupError
from Cobalt.Exceptions import JobNotInteractive
from Cobalt.Exceptions import JobValidationError
from Cobalt.Util import compact_num_list, expand_num_list
from Cobalt.Util import init_cobalt_config, get_config_option

_logger = logging.getLogger(__name__)

init_cobalt_config()

UPDATE_THREAD_TIMEOUT = int(get_config_option('alpssystem', 'update_thread_timeout', 10))
TEMP_RESERVATION_TIME = int(get_config_option('alpssystem', 'temp_reservation_time', 300))
SAVE_ME_INTERVAL = float(get_config_option('alpsssytem', 'save_me_interval', 10.0))
PENDING_STARTUP_TIMEOUT = float(get_config_option('alpssystem',
    'pending_startup_timeout', 1200)) #default 20 minutes to account for boot.
DRAIN_MODES = ['first-fit', 'backfill']
DRAIN_MODE = get_config_option('system', 'drain_mode', 'first-fit')
#cleanup time in seconds
CLEANUP_DRAIN_WINDOW = get_config_option('system', 'cleanup_drain_window', 300)
#SSD information
DEFAULT_MIN_SSD_SIZE = int(get_config_option('alpssystem', 'min_ssd_size', -1))
#Epsilon for backfilling.  This system does not do this on a per-node basis.
BACKFILL_EPSILON = int(get_config_option('system', 'backfill_epsilon', 120))
ELOGIN_HOSTS = [host for host in get_config_option('system', 'elogin_hosts', '').split(':')]
if ELOGIN_HOSTS == ['']:
    ELOGIN_HOSTS = []
DRAIN_MODES = ['first-fit', 'backfill']
CLEANING_ID = -1
DEFAULT_MCDRAM_MODE = get_config_option('alpssystem', 'default_mcdram_mode', 'cache')
DEFAULT_NUMA_MODE = get_config_option('alpssystem', 'default_numa_mode', 'quad')
MCDRAM_TO_HBMCACHEPCT = {'flat':'0', 'cache':'100', 'split':'25', 'equal':'50', '0':'0', '25':'25', '50':'50', '100':'100'}
VALID_MCDRAM_MODES = ['flat', 'cache', 'split', 'equal', '0', '25', '50', '100']
VALID_NUMA_MODES = ['a2a', 'hemi', 'quad', 'snc2', 'snc4']

def chain_loc_list(loc_list):
    '''Take a list of compact Cray locations,
    expand and concatenate them.

    '''
    retlist = []
    for locs in loc_list:
        retlist.extend(expand_num_list(locs))
    return retlist


class CrayBaseSystem(BaseSystem):
    '''Cray/ALPS-specific system component.  Behaviors should go here.  Direct
    ALPS interaction through BASIL/other APIs should go through the ALPSBridge
    (or other bridge) module as appropriate.

    '''
    name = "system"
    implementation = "alps_system"
    logger = _logger

    def __init__(self, *args, **kwargs):
        '''Initialize system.  Read initial states from bridge.
        Get current state

        '''
        start_time = time.time()
        super(CrayBaseSystem, self).__init__(*args, **kwargs)
        CrayBaseSystem._common_init_restart(self)

    def _common_init_restart(self, spec=None):
        '''Common routine for cold and restart intialization of the system
        component.

        '''
        try:
            self.system_size = int(get_config_option('system', 'size'))
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
            _logger.critical('ALPS SYSTEM: ABORT STARTUP: System size must be specified in the [system] section of the cobalt configuration file.')
            sys.exit(1)
        if DRAIN_MODE not in DRAIN_MODES:
            #abort startup, we have a completely invalid config.
            _logger.critical('ALPS SYSTEM: ABORT STARTUP: %s is not a valid drain mode.  Must be one of %s.',
                DRAIN_MODE, ", ".join(DRAIN_MODES))
            sys.exit(1)
        #process manager setup
        if spec is None:
            self.process_manager = ProcessGroupManager(pgroup_type=ALPSProcessGroup)
        else:
            self.process_manager = ProcessGroupManager(pgroup_type=ALPSProcessGroup).__setstate__(spec['process_manager'])
            self.logger.debug('pg type %s', self.process_manager.process_groups.item_cls)
        #self.process_manager.forkers.append('alps_script_forker')
        self.process_manager.update_launchers()
        self.pending_start_timeout = PENDING_STARTUP_TIMEOUT
        _logger.info('PROCESS MANAGER INTIALIZED')
        #resource management setup
        self.nodes = {} #cray node_id: CrayNode
        self._node_lock = threading.RLock()
        self.node_name_to_id = {} #cray node name to node_id map
        self.alps_reservations = {} #cobalt jobid : AlpsReservation object
        #storage for pending job starts.  Allows us to handle slow starts vs
        #user deletes
        self.pending_starts = {} #jobid: time this should be cleared.
        self.nodes_by_queue = {} #queue:[node_ids]
        #populate initial state
        #state update thread and lock
        self.current_equivalence_classes = []
        self.killing_jobs = {}

    def __getstate__(self):
        '''Save process, alps-reservation information, along with base
        information'''
        state = {}
        state.update(super(CrayBaseSystem, self).__getstate__())
        state['alps_system_statefile_version'] = 1
        state['process_manager'] = self.process_manager.__getstate__()
        state['alps_reservations'] = self.alps_reservations
        state['node_info'] = self.nodes
        return state

    def __setstate__(self, state):
        start_time = time.time()
        super(CrayBaseSystem, self).__setstate__(state)
        #Do not call child-overriden versions of the common restart.
        CrayBaseSystem._common_init_restart(self, state)


    def save_me(self):
        '''Automatically save a copy of the state of the system component.'''
        #should we be holding the block lock as well?
        Component.save(self)
    save_me = automatic(save_me, SAVE_ME_INTERVAL)

    def _init_nodes_and_reservations(self):
        '''Initialize nodes from ALPS bridge data'''
        raise NotImplementedError

    def _gen_node_to_queue(self):
        '''(Re)Generate a mapping for fast lookup of node-id's to queues.'''
        with self._node_lock:
            self.nodes_by_queue = {}
            for node in self.nodes.values():
                for queue in node.queues:
                    if queue in self.nodes_by_queue.keys():
                        self.nodes_by_queue[queue].add(node.node_id)
                    else:
                        self.nodes_by_queue[queue] = set([node.node_id])

    @exposed
    def get_nodes(self, as_dict=False, node_ids=None, params=None, as_json=False):
        '''fetch the node dictionary.

            as_dict  - Return node information as a dictionary keyed to string
                        node_id value.
            node_ids - A list of node names to return, if None, return all nodes
                       (default None).
            params   - If requesting a dict, only request this list of
                       parameters of the node.
            json     - Encode to json before sending.  Useful on large systems.

            returns the node dictionary.  Can reutrn underlying node data as
            dictionary for XMLRPC purposes

        '''
        def node_filter(node):
            if node_ids is not None:
                return (str(node[0]) in [str(nid) for nid in node_ids])
            return True

        node_info = None
        if as_dict:
            retdict = {k:v.to_dict(True, params) for k, v in self.nodes.items()}
            node_info = dict(filter(node_filter, retdict.items()))
        else:
            node_info = dict(filter(node_filter, self.nodes.items()))
        if as_json:
            return json.dumps(node_info)
        return node_info

    @exposed
    def fetch_system_data(self):
        '''Fetch a system data summary for use in time-stepping simulation.
        This provides a summary of system options in-use as well as a hardware
        inventory and queue associations.

        Returns:
            A string containing the current runnning system configuration

        Notes:
            External facing XMLRPC function

        '''
        system = Element('CraySystem')
        system.set('drain_mode', DRAIN_MODE)
        system.set('backfill_epsilon', str(BACKFILL_EPSILON))
        system.set('cleanup_drain_window', str(CLEANUP_DRAIN_WINDOW))
        system.set('size', str(self.system_size))

        with self._node_lock:
            for node in self.nodes.values():
                system.append(node.to_xml())

        return xml.dom.minidom.parseString(xml.etree.ElementTree.tostring(system)).toprettyxml(indent='    ')

    def _run_update_state(self):
        '''automated node update functions on the update timer go here.'''
        raise NotImplementedError


    @exposed
    def update_node_state(self):
        '''update the state of cray nodes. Check reservation status and system
        stateus as reported by ALPS

        '''
        raise NotImplementedError

    @exposed
    def find_queue_equivalence_classes(self, reservation_dict,
            active_queue_names, passthrough_blocking_res_list=[]):
        '''Aggregate queues together that can impact eachother in the same
        general pass (both drain and backfill pass) in find_job_location.
        Equivalence classes will then be used in find_job_location to consider
        placement of jobs and resources, in separate passes.  If multiple
        equivalence classes are returned, then they must contain orthogonal sets
        of resources.

        Inputs:
        reservation_dict -- a mapping of active reservations to resrouces.
                            These will block any job in a normal queue.
        active_queue_names -- A list of queues that are currently enabled.
                              Queues that are not in the 'running' state
                              are ignored.
        passthrough_partitions -- Not used on Cray systems currently.  This is
                                  for handling hardware that supports
                                  partitioned interconnect networks.

        Output:
        A list of dictionaries of queues that may impact eachother while
        scheduling resources.

        Side effects:
        None

        Internal Data:
        queue_assignments: a mapping of queues to schedulable locations.

        '''
        equiv = []
        node_active_queues = set([])
        self.current_equivalence_classes = [] #reverse mapping of queues to nodes
        for node in self.nodes.values():
            if node.managed and node.schedulable:
                #only condiser nodes that we are scheduling.
                node_active_queues = set([])
                for queue in node.queues:
                    if queue in active_queue_names:
                        node_active_queues.add(queue)
                if node_active_queues == set([]):
                    #this node has nothing active.  The next check can get
                    #expensive, so skip it.
                    continue
            #determine the queues that overlap.  Hardware has to be included so
            #that reservations can be mapped into the equiv classes.
            found_a_match = False
            for e in equiv:
                for queue in node_active_queues:
                    if queue in e['queues']:
                        e['data'].add(node.node_id)
                        e['queues'] = e['queues'] | set(node_active_queues)
                        found_a_match = True
                        break
                if found_a_match:
                    break
            if not found_a_match:
                equiv.append({'queues': set(node_active_queues),
                              'data': set([node.node_id]),
                              'reservations': set()})
        #second pass to merge queue lists based on hardware
        real_equiv = []
        for eq_class in equiv:
            found_a_match = False
            for e in real_equiv:
                if e['queues'].intersection(eq_class['queues']):
                    e['queues'].update(eq_class['queues'])
                    e['data'].update(eq_class['data'])
                    found_a_match = True
                    break
            if not found_a_match:
                real_equiv.append(eq_class)
        equiv = real_equiv
        #add in reservations:
        for eq_class in equiv:
            for res_name in reservation_dict:
                for node_hunk in reservation_dict[res_name].split(":"):
                    for node_id in expand_num_list(node_hunk):
                        if str(node_id) in eq_class['data']:
                            eq_class['reservations'].add(res_name)
                            break
            #don't send what could be a large block list back in the returun
            for key in eq_class:
                eq_class[key] = list(eq_class[key])
            del eq_class['data']
            self.current_equivalence_classes.append(eq_class)
        return equiv

    def _setup_special_locations(self, job):
        forbidden = set([str(loc) for loc in chain_loc_list(job.get('forbidden', []))])
        required = set([str(loc) for loc in chain_loc_list(job.get('required', []))])
        requested_locations = set([str(n) for n in expand_num_list(job['attrs'].get('location', ''))])
        # If ssds are required, add nodes without working SSDs to the forbidden list
        ssd_unavail = set([])
        if job.get('attrs', {}).get("ssds", "none").lower() == "required":
            ssd_min_size = int(job.get('attrs', {}).get("ssd_size", DEFAULT_MIN_SSD_SIZE)) * 1000000000 #convert to bytes
            ssd_unavail.update(set([str(node.node_id) for node in self.nodes.values()
                                  if (node.attributes['ssd_enabled'] == 0 or
                                      int(node.attributes.get('ssd_info', {'size': DEFAULT_MIN_SSD_SIZE})['size'])  < ssd_min_size)
                                ]))
        return (forbidden, required, requested_locations, ssd_unavail)

    def _assemble_queue_data(self, job, idle_only=True, drain_time=None):
        '''put together data for a queue, or queue-like reservation structure.

        Input:
            job - dictionary of job data.
            idle_only - [default: True] if True, return only idle nodes.
                        Otherwise return nodes in any non-down status.

        return count of idle resources, and a list of valid nodes to run on.
        if idle_only is set to false, returns a set of candidate draining nodes.


        '''
        # RESERVATION SUPPORT: Reservation queues are ephemeral, so we will
        # not find the queue normally. In the event of a reservation we'll
        # have to intersect required nodes with the idle and available
        # we also have to forbid a bunch of locations, in  this case.
        unavailable_nodes = []
        forbidden, required, requested_locations, ssd_unavail = self._setup_special_locations(job)
        requested_loc_in_forbidden = False
        for loc in requested_locations:
            if loc in forbidden:
                #don't spam the logs.
                requested_loc_in_forbidden = True
                break
        if job['queue'] not in self.nodes_by_queue.keys():
            # Either a new queue with no resources, or a possible
            # reservation need to do extra work for a reservation
            node_id_list = list(required - forbidden - ssd_unavail)
        else:
            node_id_list = list(set(self.nodes_by_queue[job['queue']]) - forbidden - ssd_unavail)
        if requested_locations != set([]): # handle attrs location= requests
            job_set = set([str(nid) for nid in requested_locations])
            if job['queue'] not in self.nodes_by_queue.keys():
                #we're in a reservation and need to further restrict nodes.
                if job_set <= set(node_id_list):
                    # We are in a reservation there are no forbidden nodes.
                    node_id_list = list(requested_locations - ssd_unavail)
                else:
                    # We can't run this job.  Insufficent resources in this
                    # reservation to do so.  Don't risk blocking anything.
                    node_id_list = []
            else:
                #normal queues.  Restrict to the non-reserved nodes.
                if job_set <= set([str(node_id) for node_id in
                                    self.nodes_by_queue[job['queue']]]):
                    node_id_list = list(requested_locations)
                    if not set(node_id_list).isdisjoint(forbidden):
                        # this job has requested locations that are a part of an
                        # active reservation.  Remove locaitons and drop available
                        # nodecount appropriately.
                        node_id_list = list(set(node_id_list) - forbidden - ssd_unavail)
                    if not requested_loc_in_forbidden:
                        raise ValueError("forbidden locations not in queue")
        with self._node_lock:
            if idle_only:
                unavailable_nodes = [node_id for node_id in node_id_list
                        if self.nodes[str(node_id)].status not in ['idle']]
            else:
                unavailable_nodes = [node_id for node_id in node_id_list
                        if self.nodes[str(node_id)].status in
                        self.nodes[str(node_id)].DOWN_STATUSES]
            if drain_time is not None:
                unavailable_nodes.extend([node_id for node_id in node_id_list
                    if (self.nodes[str(node_id)].draining and
                        (self.nodes[str(node_id)].drain_until - BACKFILL_EPSILON) < int(drain_time))])
        for node_id in set(unavailable_nodes):
            node_id_list.remove(node_id)
        return sorted(node_id_list, key=lambda nid: int(nid))

    def _select_first_nodes(self, job, node_id_list):
        '''Given a list of nids, select the first node count nodes fromt the
        list.  This is the target for alternate allocator replacement.

        Input:
            job - dictionary of job data from the scheduler
            node_id_list - a list of possible candidate nodes

        Return:
            A list of nodes.  [] if insufficient nodes for the allocation.

        Note: hold the node lock while doing this.  We really don't want a
        update to happen while doing this.

        '''
        ret_nodes = []
        with self._node_lock:
            if int(job['nodes']) <= len(node_id_list):
                node_id_list.sort(key=lambda nid: int(nid))
                ret_nodes = node_id_list[:int(job['nodes'])]
        return ret_nodes

    def _select_first_nodes_prefer_memory_match(self, job, node_id_list):
        '''Given a list of nids, select the first node count nodes fromt the
        list.  Prefer nodes that match the memory modes for a given job, then
        go in nid order.

        Input:
            job - dictionary of job data from the scheduler
            node_id_list - a list of possible candidate nodes

        Return:
            A list of nodes.  [] if insufficient nodes for the allocation.

        Note: hold the node lock while doing this.  We really don't want a
        update to happen while doing this.

        '''
        ssd_required = (job.get('attrs', {}).get("ssds", "none").lower() == "required")
        if job.get('attrs', {}).get('mcdram', None) is None or job.get('attrs', {}).get('numa', None) is None:
            # insufficient information to include a mode match
            return self._select_first_nodes(job, node_id_list)
        ret_nids = []
        with self._node_lock:
            considered_nodes = [node for node in self.nodes.values() if node.node_id in node_id_list]
            for node in considered_nodes:
                if (node.attributes['hbm_cache_pct'] == MCDRAM_TO_HBMCACHEPCT[job['attrs']['mcdram']] and
                        node.attributes['numa_cfg'] == job['attrs']['numa']):
                    ret_nids.append(int(node.node_id))
            if len(ret_nids) < int(job['nodes']):
                node_id_list.sort(key=lambda nid: int(nid))
                for nid in node_id_list:
                    if int(nid) not in ret_nids:
                        ret_nids.append(int(nid))
                        if len(ret_nids) >= int(job['nodes']):
                            break
        return ret_nids[:int(job['nodes'])]

    def _associate_and_run_immediate(self, job, resource_until_time, node_id_list):
        '''Given a list of idle node ids, choose a set that can run a job
        immediately, if a set exists in the node_id_list.

        Inputs:
            job - Dictionary of job data
            node_id_list - a list of string node id values

        Side Effects:
            Will reserve resources in ALPS and will set resource reservations on
            allocated nodes.

        Return:
            None if no match, otherwise the pairing of a jobid and set of nids
            that have been allocated to a job.

        '''
        compact_locs = None
        if int(job['nodes']) <= len(node_id_list):
            #this job can be run immediately
            to_alps_list = self._select_first_nodes(job, node_id_list)
            job_locs = self._ALPS_reserve_resources(job, resource_until_time,
                    to_alps_list)
            if job_locs is not None and len(job_locs) == int(job['nodes']):
                compact_locs = compact_num_list(job_locs)
                #temporary reservation until job actually starts
                self.pending_starts[job['jobid']] = resource_until_time
                self.reserve_resources_until(compact_locs, resource_until_time, job['jobid'])
        return compact_locs

    @locking
    @exposed
    def find_job_location(self, arg_list, end_times, pt_blocking_locations=[]):
        '''Given a list of jobs, and when jobs are ending, return a set of
        locations mapped to a jobid that can be run.  Also, set up draining
        as-needed to run top-scored jobs and backfill when possible.

        Called once per equivalence class.

        Args::
            arg_list: A list of dictionaries containning information on jobs to
                   cosnider.
            end_times: list containing a mapping of locations and the times jobs
                    runninng on those locations are scheduled to end.  End times
                    are in seconds from Epoch UTC.
            pt_blocking_locations: Not used for this system.  Used in partitioned
                                interconnect schemes. A list of locations that
                                should not be used to prevent passthrough issues
                                with other scheduler reservations.

        Returns:
        A mapping of jobids to locations to run a job to run immediately.

        Side Effects:
        May set draining flags and backfill windows on nodes.
        If nodes are being returned to run, set ALPS reservations on them.

        Notes:
        The reservation set on ALPS resources is uncomfirmed at this point.
        This reservation may timeout.  The forker when it confirms will detect
        this and will re-reserve as needed.  The alps reservation id may change
        in this case post job startup.

        pt_blocking_locations may be used later to block out nodes that are
        impacted by warmswap operations.

        This function *DOES NOT* hold the component lock.

        '''
        now = time.time()
        resource_until_time = now + TEMP_RESERVATION_TIME
        with self._node_lock:
            # only valid for this scheduler iteration.
            self._clear_draining_for_queues(arg_list[0]['queue'])
            #check if we can run immedaitely, if not drain.  Keep going until all
            #nodes are marked for draining or have a pending run.
            best_match = {} #jobid: list(locations)
            for job in arg_list:
                label = '%s/%s' % (job['jobid'], job['user'])
                # walltime is in minutes.  We should really fix the storage of
                # that --PMR
                job_endtime = now + (int(job['walltime']) * 60)
                try:
                    node_id_list = self._assemble_queue_data(job, drain_time=job_endtime)
                    available_node_list = self._assemble_queue_data(job, idle_only=False)
                except ValueError:
                    _logger.warning('Job %s: requesting locations that are not in requested queue.',
                            job['jobid'])
                    continue
                if int(job['nodes']) > len(available_node_list):
                    # Insufficient operational nodes for this job at all
                    continue
                elif len(node_id_list) == 0:
                    pass #allow for draining pass to run.
                elif int(job['nodes']) <= len(node_id_list):
                    # enough nodes are in a working state to consider the job.
                    # enough nodes are idle that we can run this job
                    compact_locs = self._associate_and_run_immediate(job,
                            resource_until_time, node_id_list)
                    # do we want to allow multiple placements in a single
                    # pass? That would likely help startup times.
                    if compact_locs is not None:
                        best_match[job['jobid']] = [compact_locs]
                        _logger.info("%s: Job selected for running on nodes  %s",
                                label, compact_locs)
                        break #for now only select one location
                if DRAIN_MODE in ['backfill', 'drain-only']:
                    # drain sufficient nodes for this job to run
                    drain_node_ids = self._select_nodes_for_draining(job,
                            end_times)
                    if drain_node_ids != []:
                        _logger.info('%s: nodes %s selected for draining.', label,
                                compact_num_list(drain_node_ids))
        return best_match

    def _ALPS_reserve_resources(self, job, new_time, node_id_list):
        '''Call ALPS to reserve resrources.  Use their allocator.  We can change
        this later to substitute our own allocator if-needed.

        Input:
        Nodecount - number of nodes to reserve for  a job.

        Returns: a list of locations that ALPS has reserved.

        Side effects:
        Places an ALPS reservation on resources.  Calls reserve resources until
        on the set of nodes, and will mark nodes as allocated.

        '''
        raise NotImplementedError

    def _clear_draining_for_queues(self, queue):
        '''Given a list of queues, remove the draining flags on nodes.

        queues - a queue in an equivalence class to consider.  This will clear
        the entire equiv class

        return - none

        Note: does not acquire block lock.  Must be locked externally.

        '''
        current_queues = []
        for equiv_class in self.current_equivalence_classes:
            if queue in equiv_class['queues']:
                current_queues = equiv_class['queues']
        if current_queues:
            with self._node_lock:
                for node in self.nodes.values():
                    for q in node.queues:
                        if q in current_queues:
                            node.clear_drain()

    def _select_nodes_for_draining(self, job, end_times):
        '''Select nodes to be drainined.  Set backfill windows on draining
        nodes.

        Inputs:
            job - dictionary of job information to consider
            end_times - a list of nodes and their endtimes should be sorted
                        in order of location preference

        Side Effect:
            end_times will be sorted in ascending end-time order

        Return:
            List of node ids that have been selected for draining for this job,
            as well as the expected drain time.

        '''
        now = int(time.time())
        end_times.sort(key=lambda x: int(x[1]))
        drain_list = []
        candidate_list = []
        cleanup_statuses = ['cleanup', 'cleanup-pending']
        forbidden, required, requested_locations, ssd_unavail = self._setup_special_locations(job)
        try:
            node_id_list = self._assemble_queue_data(job, idle_only=False)
        except ValueError:
            _logger.warning('Job %s: requesting locations that are not in queue.', job['jobid'])
        else:
            with self._node_lock:
                drain_time = None
                candidate_drain_time = None
                # remove the following from the list:
                # 1. idle nodes that are already marked for draining.
                # 2. Nodes that are in an in-use status (busy, allocated).
                # 3. Nodes marked for cleanup that are not allocated to a real
                #    jobid. CLEANING_ID is a sentiel jobid value so we can set
                #    a drain window on cleaning nodes easiliy.  Not sure if this
                #    is the right thing to do. --PMR
                candidate_list = []
                candidate_list = [nid for nid in node_id_list
                        if (not self.nodes[str(nid)].draining and
                            (self.nodes[str(nid)].status in ['idle']) or
                            (self.nodes[str(nid)].status in cleanup_statuses)
                            )]
                for nid in candidate_list:
                    if self.nodes[str(nid)].status in cleanup_statuses:
                        candidate_drain_time = now + CLEANUP_DRAIN_WINDOW
                for loc_time in end_times:
                    running_nodes = [str(nid) for nid in
                            expand_num_list(",".join(loc_time[0]))
                            if ((job['queue'] in self.nodes[str(nid)].queues or
                                nid in required) and
                                not self.nodes[str(nid)].draining)]
                    for nid in running_nodes:
                        # We set a drain on all running nodes for use in a later
                        # so that we can "favor" draining on the longest
                        # running set of nodes.
                        if (self.nodes[str(nid)].status != 'down' and
                                self.nodes[str(nid)].managed):
                            self.nodes[str(nid)].set_drain(loc_time[1], job['jobid'])
                    candidate_list.extend([nid for nid in running_nodes if
                        self.nodes[str(nid)].draining])
                    candidate_drain_time = int(loc_time[1])
                    if len(candidate_list) >= int(job['nodes']):
                        # Enough nodes have been found to drain for this job
                        break
                candidates = set(candidate_list)
                # We need to further restrict this list based on requested
                # location and reservation avoidance data:
                if forbidden != set([]):
                    candidates = candidates.difference(forbidden)
                if ssd_unavail != set([]):
                    candidates = candidates.difference(ssd_unavail)
                if requested_locations != set([]):
                    candidates = candidates.intersection(requested_locations)
                candidate_list = list(candidates)
                if len(candidate_list) >= int(job['nodes']):
                    drain_time = candidate_drain_time
                if drain_time is not None:
                    # order the node ids by id and drain-time. Longest drain
                    # first
                    candidate_list.sort(key=lambda nid: int(nid))
                    candidate_list.sort(reverse=True,
                            key=lambda nid: self.nodes[str(nid)].drain_until)
                    drain_list = candidate_list[:int(job['nodes'])]
                    for nid in drain_list:
                        self.nodes[str(nid)].set_drain(drain_time, job['jobid'])
        return drain_list

    @exposed
    def reserve_resources_until(self, location, new_time, jobid):
        '''Place, adjust and release resource reservations.

        Input:
            location: the location to reserve [list of nodes]
            new_time: the new end time of a resource reservation
            jobid: the Cobalt jobid that this reservation is for

        Output:
            True if resource reservation is successfully placed.
            Otherwise False.

        Side Effects:
            * Sets/releases reservation on specified node list
            * Sets/releases ALPS reservation.  If set reservation is unconfirmed
              Confirmation must occur a cray_script_forker

        Notes:
            This holds the node data lock while it's running.

        '''
        completed = False
        with self._node_lock:
            succeeded_nodes = []
            failed_nodes = []
            #assemble from locaion list:
            exp_location = []
            if isinstance(location, list):
                exp_location = chain_loc_list(location)
            elif isinstance(location, str):
                exp_location = expand_num_list(location)
            else:
                raise TypeError("location type is %s.  Must be one of 'list' or 'str'", type(location))
            if new_time is not None:
                #reserve the location. Unconfirmed reservations will have to
                #be lengthened.  Maintain a list of what we have reserved, so we
                #extend on the fly, and so that we don't accidentally get an
                #overallocation/user
                for loc in exp_location:
                    # node = self.nodes[self.node_name_to_id[loc]]
                    node = self.nodes[str(loc)]
                    try:
                        node.reserve(new_time, jobid=jobid)
                        succeeded_nodes.append(int(loc))
                    except Cobalt.Exceptions.ResourceReservationFailure as exc:
                        self.logger.error(exc)
                        failed_nodes.append(loc)
                self.logger.info("job %s: nodes '%s' now reserved until %s",
                    jobid, compact_num_list(succeeded_nodes),
                    time.asctime(time.gmtime(new_time)))
                if failed_nodes != []:
                    self.logger.warning("job %s: failed to reserve nodes '%s'",
                        jobid, compact_num_list(failed_nodes))
                else:
                    completed = True
            else:
                #release the reservation and the underlying ALPS reservation
                #and the reserration on blocks.
                for loc in exp_location:
                    # node = self.nodes[self.node_name_to_id[loc]]
                    node = self.nodes[str(loc)]
                    try:
                        node.release(user=None, jobid=jobid)
                        succeeded_nodes.append(int(loc))
                    except Cobalt.Exceptions.ResourceReservationFailure as exc:
                        self.logger.error(exc)
                        failed_nodes.append(loc)
                    #cleanup pending has to be dealt with.  Do this in UNS for
                    #now
                self.logger.info("job %s:  nodes '%s' released. Cleanup pending.",
                    jobid, compact_num_list(succeeded_nodes))
                if failed_nodes != []:
                    self.logger.warning("job %s: failed to release nodes '%s'",
                        jobid, compact_num_list(failed_nodes))
                else:
                    completed = True
        return completed


    @exposed
    def wait_process_groups(self, specs):
        '''Get the exit status of any completed process groups.  If completed,
        initiate the partition cleaning process, and remove the process group
        from system's list of active processes.

        '''

        # process_groups = [pg for pg in
                          # self.process_manager.process_groups.q_get(specs)
                          # if pg.exit_status is not None]
        return self.process_manager.cleanup_groups([pg.id for pg in
            self.process_manager.process_groups.q_get(specs)
            if pg.exit_status is not None])
        # for process_group in process_groups:
            # del self.process_manager.process_groups[process_group.idh
        # return process_groups

    @exposed
    @query
    def get_process_groups(self, specs):
        '''Return a list of process groups using specs as a filter'''
        return self.process_manager.process_groups.q_get(specs)

    @exposed
    def add_process_groups(self, specs):
        '''Add process groups and start their runs.  Adjust the resource
        reservation time to full run time at this point.

        Args:
            specs: A list of dictionaries that contain information on the Cobalt
                   job required to start the backend process.

        Returns:
            A created process group object.  This is wrapped and sent via XMLRPC
            to the caller.

        Side Effects:
            Invokes a forker component to run a user script.  In the event of a
            fatal startup error, will release resource reservations.

        Note:
            Process Group startup and intialization holds the process group data lock.

        '''
        start_apg_timer = time.time()
        with self.process_manager.process_groups_lock:
            for spec in specs:
                spec['forker'] = None
                alps_res = self.alps_reservations.get(str(spec['jobid']), None)
                if alps_res is not None:
                    spec['alps_res_id'] = alps_res.alps_res_id
                new_pgroups = self.process_manager.init_groups(specs)
            for pgroup in new_pgroups:
                _logger.info('%s: process group %s created to track job status',
                        pgroup.label, pgroup.id)
                #check resource reservation, and attempt to start.  If there's a
                #failure here, set exit status in process group to a sentinel value.
                try:
                    started = self.process_manager.start_groups([pgroup.id])
                except ComponentLookupError:
                    _logger.error("%s: failed to contact the %s component",
                            pgroup.label, pgroup.forker)
                    #this should be reraised and the queue-manager handle it
                    #that would allow re-requesting the run instead of killing the
                    #job --PMR
                except xmlrpclib.Fault:
                    _logger.error("%s: a fault occurred while attempting to start "
                            "the process group using the %s component",
                            pgroup.label, pgroup.forker)
                    pgroup.exit_status = 255
                    self.reserve_resources_until(pgroup.location, None,
                            pgroup.jobid)
                except Exception:
                    _logger.error("%s: an unexpected exception occurred while "
                            "attempting to start the process group using the %s "
                            "component; releasing resources", pgroup.label,
                            pgroup.forker, exc_info=True)
                    pgroup.exit_status = 255
                    self.reserve_resources_until(pgroup.location, None,
                            pgroup.jobid)
                else:
                    if started is not None and started != []:
                        _logger.info('%s: Process Group %s started successfully.',
                                pgroup.label, pgroup.id)
                    else:
                        _logger.error('%s: Process Group startup failed. Aborting.',
                                pgroup.label)
                        pgroup.exit_status = 255
                        self.reserve_resources_until(pgroup.location, None,
                                pgroup.jobid)
        end_apg_timer = time.time()
        self.logger.debug("add_process_groups startup time: %s sec", (end_apg_timer - start_apg_timer))
        return new_pgroups


    @exposed
    @query
    def signal_process_groups(self, specs, signame="SIGINT"):
        '''Send a signal to underlying child process.  Defalut signal is SIGINT.
        May be any signal avaliable to the system.  This signal goes to the head
        process group.

        '''
        pgids = [spec['id'] for spec in specs]
        return self.process_manager.signal_groups(pgids, signame)

    def _get_exit_status(self):
        '''Check running process groups and set exit statuses.

        If status is set, cleanup will be invoked next time wait_process_groups
        is called.

        '''
        with self.process_manager.process_groups_lock:
            completed_pgs = self.process_manager.update_groups()
            for pgroup in completed_pgs:
                _logger.info('%s: process group reported as completed with status %s',
                        pgroup.label, pgroup.exit_status)
                self.reserve_resources_until(pgroup.location, None, pgroup.jobid)
        return

    @exposed
    def validate_job(self, spec):
        '''Basic validation of a job to run on a cray system.  Make sure that
        certain arguments have been passsed in.  On failure raise a
        JobValidationError.

        '''
        #set default memory modes.
        if spec.get('attrs', None) is None:
            spec['attrs'] = {'mcdram': DEFAULT_MCDRAM_MODE, 'numa': DEFAULT_NUMA_MODE}
        else:
            if spec['attrs'].get('mcdram', None) is None:
                spec['attrs']['mcdram'] = DEFAULT_MCDRAM_MODE
            if spec['attrs'].get('numa', None) is None:
                spec['attrs']['numa'] = DEFAULT_NUMA_MODE
        # It helps when the mode requested actually exists.
        if spec['attrs']['mcdram'] not in VALID_MCDRAM_MODES:
            raise JobValidationError('mcdram %s not valid must be one of: %s'% (spec['attrs']['mcdram'],
                    ', '.join(VALID_MCDRAM_MODES)))
        if spec['attrs']['numa'] not in VALID_NUMA_MODES:
            raise JobValidationError('numa %s not valid must be one of: %s' % (spec['attrs']['numa'],
                    ', '.join(VALID_NUMA_MODES)))
        # mode on this system defaults to script.
        mode = spec.get('mode', None)
        if ((mode is None) or (mode == False)):
            spec['mode'] = 'script'
        if spec['mode'] not in ['script', 'interactive']:
            raise JobValidationError("Mode %s is not supported on Cray systems." % mode)
        # FIXME: Pull this out of the system configuration from ALPS ultimately.
        # For now, set this from config for the PE count per node
        spec['nodecount'] = int(spec['nodecount'])
        # proccount = spec.get('proccount', None)
        # if proccount is None:
            # nodes *
        if spec['nodecount'] > self.system_size:
            raise JobValidationError('Job requested %s nodes.  Maximum permitted size is %s' %
                    (spec['nodecount'], self.system_size))
        spec['proccount'] = spec['nodecount'] #set multiplier for default depth
        mode = spec.get('mode', 'script')
        spec['mode'] = mode
        if mode == 'interactive':
            # Handle which script host should handle their job if they're on a
            # login.
            if spec.get('qsub_host', None) in ELOGIN_HOSTS:
                try:
                    spec['ssh_host'] = self.process_manager.select_ssh_host()
                except RuntimeError:
                    spec['ssh_host'] = None
                if spec['ssh_host'] is None:
                    raise JobValidationError('No valid SSH host could be found for interactive job.')
        return spec

    @exposed
    def verify_locations(self, nodes):
        '''verify that a list of nodes exist on this system.  Return the list
        that can be found.

        '''
        good_nodes = [node for node in nodes if str(node) in self.nodes.keys()]
        return good_nodes

    @exposed
    def update_nodes(self, updates, node_list, user):
        '''Apply update to a node's status from an external client.

        Updates apply to all nodes.  User is for logging purposes.

        node_list should be a list of nodeids from the cray system

        Hold the node lock while doing this.

        Force a status update while doing this operation.

        '''
        mod_nodes = []
        with self._node_lock:
            for node_id in node_list:
                node = self.nodes[str(node_id)]
                try:
                    if updates.get('down', False):
                        node.admin_down = True
                        node.status = 'down'
                    elif updates.get('up', False):
                        node.admin_down = False
                        node.status = 'idle'
                    elif updates.get('queues', None):
                        node.queues = list(updates['queues'].split(':'))
                except Exception:
                    _logger.error("Unexpected exception encountered!", exc_info=True)
                else:
                    mod_nodes.append(node_id)
        if updates.get('queues', False):
            self._gen_node_to_queue()
        if mod_nodes != []:
            self.update_node_state()
        _logger.info('Updates %s applied to nodes %s by %s', updates,
                compact_num_list(mod_nodes), user)
        return mod_nodes

    @exposed
    def confirm_alps_reservation(self, specs):
        '''confirm or rereserve if needed the ALPS reservation for an
        interactive job.

        '''
        raise NotImplementedError

    @exposed
    def interactive_job_complete (self, jobid):
        """Will terminate the specified interactive job
        """
        job_not_found = True
        for pg in self.process_manager.process_groups.itervalues():
            if pg.jobid == jobid:
                job_not_found = False
                if pg.mode == 'interactive':
                    pg.interactive_complete = True
                else:
                    msg = "Job %s not an interactive" % str(jobid)
                    self.logger.error(msg)
                    raise JobNotInteractive(msg)
                break
        if job_not_found:
            self.logger.warning("%s: Interactive job not found", str(jobid))
        return


