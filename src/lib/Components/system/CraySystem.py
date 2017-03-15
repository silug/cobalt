"""Resource management for Cray ALPS based systems"""

import logging
import thread
import time
import xmlrpclib

import Cobalt.Util
import Cobalt.Components.system.AlpsBridge as ALPSBridge
from Cobalt.Components.base import exposed
from Cobalt.Components.system.CrayBaseSystem import CrayBaseSystem
from Cobalt.Components.system.CrayNode import CrayNode
from Cobalt.Components.system.ALPSReservation import ALPSReservation
from Cobalt.Exceptions import ComponentLookupError
from Cobalt.Util import compact_num_list
from Cobalt.Util import init_cobalt_config, get_config_option

_logger = logging.getLogger(__name__)

init_cobalt_config()

UPDATE_THREAD_TIMEOUT = int(get_config_option('alpssystem',
    'update_thread_timeout', 10))
SAVE_ME_INTERVAL = float(get_config_option('alpsssytem', 'save_me_interval', 10.0))
PENDING_STARTUP_TIMEOUT = float(get_config_option('alpssystem',
    'pending_startup_timeout', 1200)) #default 20 minutes to account for boot.
APKILL_CMD = get_config_option('alps', 'apkill', '/opt/cray/alps/default/bin/apkill')



class CraySystem(CrayBaseSystem):
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
        super(CraySystem, self).__init__(*args, **kwargs)
        _logger.info('BASE SYSTEM INITIALIZED')
        self._common_init_restart()
        _logger.info('ALPS SYSTEM COMPONENT READY TO RUN')
        _logger.info('Initilaization complete in %s sec.', (time.time() -
                start_time))

    def _common_init_restart(self, spec=None):
        '''Common routine for cold and restart intialization of the system
        component.

        '''
        #initilaize bridge.
        bridge_pending = True
        while bridge_pending:
            # purge stale children from prior run.  Also ensure the
            # system_script_forker is currently up.
            # These attempts may fail due to system_script_forker not being up.
            # We don't want to trash the statefile in this case.
            try:
                ALPSBridge.init_bridge()
            except ALPSBridge.BridgeError:
                _logger.error('Bridge Initialization failed.  Retrying.')
                Cobalt.Util.sleep(10)
            except ComponentLookupError:
                _logger.warning('Error reaching forker.  Retrying.')
                Cobalt.Util.sleep(10)
            else:
                bridge_pending = False
                _logger.info('BRIDGE INITIALIZED')
        #resource management setup
        if spec is not None:
            self.alps_reservations = spec['alps_reservations']
        self._init_nodes_and_reservations()
        if spec is not None:
            node_info = spec.get('node_info', {})
            for nid, node in node_info.items():
                try:
                    self.nodes[nid].reset_info(node)
                except: #FIXME: check the exception types later.  Carry on otherwise.
                    self.logger.warning("Node nid: %s not found in restart information.  Bringing up node in a clean configuration.", nid)
        self._gen_node_to_queue()
        self.node_update_thread = thread.start_new_thread(self._run_update_state, tuple())
        _logger.info('UPDATE THREAD STARTED')
        #hold on to the initial spec in case nodes appear out of nowhere.
        self.init_spec = None
        if spec is not None:
            self.init_spec = spec
        _logger.info('PREVIOUS SPEC HELD')

    def __getstate__(self):
        '''Save process, alps-reservation information, along with base
        information'''
        state = {}
        state.update(super(CraySystem, self).__getstate__())
        return state

    def __setstate__(self, state):
        start_time = time.time()
        super(CraySystem, self).__setstate__(state)
        _logger.info('SYSTEM INITIALIZED')
        self._common_init_restart(state)
        _logger.info('ALPS SYSTEM COMPONENT READY TO RUN')
        _logger.info('Reinitilaization complete in %s sec.', (time.time() -
                start_time))


    def _init_nodes_and_reservations(self):
        '''Initialize nodes from ALPS bridge data'''

        pending = True
        while pending:
            try:
                # None of these queries has strictly degenerate data.  Inventory
                # is needed for raw reservation data.  System gets memory and a
                # much more compact representation of data.  Reservednodes gives
                # which notes are reserved.
                inventory = ALPSBridge.fetch_inventory()
                _logger.info('INVENTORY FETCHED')
                system = ALPSBridge.extract_system_node_data(ALPSBridge.system())
                _logger.info('SYSTEM DATA FETCHED')
                #reservations = ALPSBridge.fetch_reservations()
                #_logger.info('ALPS RESERVATION DATA FETCHED')
                # reserved_nodes = ALPSBridge.reserved_nodes()
            except Exception:
                #don't crash out here.  That may trash a statefile.
                _logger.error('Possible transient encountered during initialization. Retrying.',
                        exc_info=True)
                Cobalt.Util.sleep(10)
            else:
                pending = False

        self._assemble_nodes(inventory, system)
        #Reversing the node name to id lookup is going to save a lot of cycles.
        for node in self.nodes.values():
            self.node_name_to_id[node.name] = node.node_id
        _logger.info('NODE INFORMATION INITIALIZED')
        _logger.info('ALPS REPORTS %s NODES', len(self.nodes))
        # self._assemble_reservations(reservations, reserved_nodes)
        return

    def _assemble_nodes(self, inventory, system):
        '''merge together the INVENTORY and SYSTEM query data to form as
        complete a picture of a node as we can.

        '''
        nodes = {}
        for nodespec in inventory['nodes']:
            node = CrayNode(nodespec)
            node.managed = True
            nodes[node.node_id] = node
        for node_id, nodespec in system.iteritems():
            nodes[node_id].attributes.update(nodespec['attrs'])
            # Should this be a different status?
            nodes[node_id].role = nodespec['role'].upper()
            if nodes[node_id].role.upper() not in ['BATCH']:
                nodes[node_id].status = 'down'
            nodes[node_id].status = nodespec['state']
        self.nodes.update(nodes)

    def _run_update_state(self):
        '''automated node update functions on the update timer go here.'''
        while True:
            try:
                self.process_manager.update_launchers()
                self.update_node_state()
                self._get_exit_status()
            except Exception:
                # prevent the update thread from dying
                _logger.critical('Error in _run_update_state', exc_info=True)
            finally:
                Cobalt.Util.sleep(UPDATE_THREAD_TIMEOUT)

    def _reconstruct_node(self, inven_node, inventory):
        '''Reconstruct a node from statefile information.  Needed whenever we
        find a new node.  If no statefile information from the orignal cobalt
        invocation exists, bring up a node in default states and mark node
        administratively down.

        This node was disabled and invisible to ALPS at the time Cobalt was
        initialized and so we have no current record of that node.

        '''
        nid = inven_node['node_id']
        new_node = None
        #construct basic node from inventory
        for node_info in inventory['nodes']:
            if int(node_info['node_id']) == int(nid):
                new_node = CrayNode(node_info)
                break
        if new_node is None:
            #we have a phantom node?
            self.logger.error('Unable to find inventory information for nid: %s', nid)
            return
        # if we have information from the statefile we need to repopulate the
        # node with the saved data.
        # Perhaps this should be how I construct all node data anyway?
        if self.init_spec is not None:
            node_info = self.init_spec.get('node_info', {})
            try:
                new_node.reset_info(node_info[str(nid)])
                self.logger.warning('Node %s reconstructed.', nid)
            except:
                self.logger.warning("Node nid: %s not found in restart information.  Bringing up node in a clean configuration.", nid, exc_info=True)
                #put into admin_down
                new_node.admin_down = True
                new_node.status = 'down'
                self.logger.warning('Node %s marked down.', nid)
        new_node.managed = True
        self.nodes[str(nid)] = new_node
        self.logger.warning('Node %s added to tracking.', nid)

    @exposed
    def update_node_state(self):
        '''update the state of cray nodes. Check reservation status and system
        stateus as reported by ALPS

        '''
        #Check clenaup progress.  Check ALPS reservations.  Check allocated
        #nodes.  If there is no resource reservation and the node is not in
        #current alps reservations, the node is ready to schedule again.
        now = time.time()
        startup_time_to_clear = []
        #clear pending starttimes.
        for jobid, start_time in self.pending_starts.items():
            if int(now) > int(start_time):
                startup_time_to_clear.append(jobid)
        for jobid in startup_time_to_clear:
            del self.pending_starts[jobid]

        self.check_killing_aprun()
        with self._node_lock:
            try:
                #I have seen problems with the kitchen-sink query here, where
                #the output gets truncated on it's way into Cobalt.
                #inventory = ALPSBridge.fetch_inventory(resinfo=True) #This is a full-refresh,
                #determine if summary may be used under normal operation
                #updated for >= 1.6 interface
                inven_nodes = ALPSBridge.extract_system_node_data(ALPSBridge.system())
                reservations = ALPSBridge.fetch_reservations()
                #reserved_nodes = ALPSBridge.reserved_nodes()
            except (ALPSBridge.ALPSError, ComponentLookupError):
                _logger.warning('Error contacting ALPS for state update.  Aborting this update',
                        exc_info=True)
                return
            inven_reservations = reservations.get('reservations', [])
            start_time = time.time()
            self._detect_rereservation(inven_reservations)
            # check our reservation objects.  If a res object doesn't correspond
            # to any backend reservations, this reservation object should be
            # dropped
            current_alps_res_ids = [int(res['reservation_id']) for res in
                    inven_reservations]
            res_jobid_to_delete = []
            if self.alps_reservations == {}:
                # if we have nodes in cleanup-pending but no alps reservations,
                # then the nodes in cleanup pending are considered idle (or
                # at least not in cleanup).  Hardware check can catch these
                # later. This catches leftover reservations from hard-shutdowns
                # while running.
                for node in self.nodes.values():
                    if node.status in ['cleanup', 'cleanup-pending']:
                        node.status = 'idle'
            for alps_res in self.alps_reservations.values():
                if alps_res.jobid in self.pending_starts.keys():
                    continue #Get to this only after timeout happens
                #find alps_id associated reservation
                if int(alps_res.alps_res_id) not in current_alps_res_ids:
                    for node_id in alps_res.node_ids:
                        if not self.nodes[str(node_id)].reserved:
                            #pending hardware status update
                            self.nodes[str(node_id)].status = 'idle'
                    res_jobid_to_delete.append(alps_res.jobid)
                    _logger.info('Nodes %s cleanup complete.',
                            compact_num_list(alps_res.node_ids))
            for jobid in res_jobid_to_delete:
                _logger.info('%s: ALPS reservation for this job complete.', jobid)
                del self.alps_reservations[str(jobid)]
            #process group should already be on the way down since cqm released the
            #resource reservation
            cleanup_nodes = [node for node in self.nodes.values()
                             if node.status in ['cleanup-pending', 'cleanup']]
            #If we have a block marked for cleanup, send a release message.
            released_res_jobids = []
            cleaned_nodes = []
            for node in cleanup_nodes:
                found = False
                for alps_res in self.alps_reservations.values():
                    if str(node.node_id) in alps_res.node_ids:
                        found = True
                        if alps_res.jobid not in released_res_jobids:
                            #send only one release per iteration
                            apids = alps_res.release()
                            if apids is not None:
                                for apid in apids:
                                    self.signal_aprun(apid)
                            released_res_jobids.append(alps_res.jobid)
                if not found:
                    # There is no alps reservation to release, cleanup is
                    # already done.  This happens with very poorly timed
                    # qdel requests. Status will be set properly with the
                    # subsequent hardware status check.
                    _logger.info('Node %s cleanup complete.', node.node_id)
                    node.status = 'idle'
                    cleaned_nodes.append(node)
            for node in cleaned_nodes:
                cleanup_nodes.remove(node)

        #find hardware status
            #so we do this only once for nodes being added.
            #full inventory fetch is expensive.
            recon_inventory = None
            for inven_node in inven_nodes.values():
                if self.nodes.has_key(str(inven_node['node_id'])):
                    node = self.nodes[str(inven_node['node_id'])]
                    node.role = inven_node['role'].upper()
                    if node.reserved:
                        #node marked as reserved.
                        if self.alps_reservations.has_key(str(node.reserved_jobid)):
                            node.status = 'busy'
                        else:
                            # check to see if the resource reservation should be
                            # released.
                            if node.reserved_until >= now:
                                node.status = 'allocated'
                            else:
                                node.release(user=None, jobid=None, force=True)
                    else:
                        node.status = inven_node['state'].upper()
                        if node.role.upper() not in ['BATCH'] and node.status is 'idle':
                            node.status = 'alps-interactive'
                else:
                    # Apparently, we CAN add nodes on the fly.  The node would
                    # have been disabled.  We need to add a new node and update
                    # it's state.
                    _logger.warning('Unknown node %s found. Starting reconstruction.', inven_node['node_id'])
                    try:
                        if recon_inventory is None:
                            recon_inventory = ALPSBridge.fetch_inventory()
                    except:
                        _logger.error('Failed to fetch inventory.  Will retry on next pass.', exc_info=True)
                    else:
                        self._reconstruct_node(inven_node, recon_inventory)
                   # _logger.error('UNS: ALPS reports node %s but not in our node list.',
                   #               inven_node['node_id'])
            #should down win over running in terms of display?
            #keep node that are marked for cleanup still in cleanup
            for node in cleanup_nodes:
                node.status = 'cleanup-pending'
        #_logger.debug("time in UNS lock: %s seconds", (time.time() - start_time))
        return

    def _detect_rereservation(self, inven_reservations):
        '''Detect and update the ALPS reservation associated with a running job.
        We are only concerned with BATCH reservations.  Others would be
        associated with running jobs, and should not be touched.

        '''
        def _construct_alps_res(alps_res):
            with self._node_lock:
                job_nodes = [node.node_id for node in self.nodes.values()
                        if node.reserved_jobid == int(alps_res['batch_id'])]
            new_resspec = {'reserved_nodes': job_nodes,
                           'reservation_id': str(alps_res['reservation_id']),
                           'pagg_id': 0 #unknown.  Not used here.
                            }
            new_jobspec = {'jobid': int(alps_res['batch_id']),
                           'user' : alps_res['user_name']}

            return ALPSReservation(new_jobspec, new_resspec, self.nodes)

        for alps_res in inven_reservations:
            try:
                #This traversal is terrible. May want to hide this in the API
                #somewhere
                if alps_res['ApplicationArray'][0]['Application'][0]['CommandArray'][0]['Command'][0]['cmd'] != 'BASIL':
                    # Not a reservation we're in direct control of.
                    continue
            except (KeyError, IndexError):
                #not a batch reservation
                continue
            if str(alps_res['batch_id']) in self.alps_reservations.keys():
                # This is a reservation we may already know about
                if (int(alps_res['reservation_id']) ==
                        self.alps_reservations[str(alps_res['batch_id'])].alps_res_id):
                    # Already know about this one
                    continue
                # we have a re-reservation.  If this has a batch id, we need
                # to add it to our list of tracked reservations, and inherit
                # other reservation details.  We can pull the reservation
                # information out of reserve_resources_until.

                # If this is a BATCH reservation and no hardware has that
                # reservation id, then this reservation needs to be released
                # Could happen if we have a job starting right at the RRU
                # boundary.
                new_alps_res = _construct_alps_res(alps_res)
                tracked_res = self.alps_reservations.get(new_alps_res.jobid, None)
                if tracked_res is not None:
                    try:
                        apids = tracked_res.release()
                    except ALPSBridge.ALPSError:
                        # backend reservation probably is gone, which is why
                        # we are here in the first place.  This will result in an error from the bridge.
                        pass
                    else:
                        _logger.warning('Job %s: Releasing ALPS reservation %s.  Re-reservation detected.', alps_res['batch_id'],
                                apids)
                self.alps_reservations[str(alps_res['batch_id'])] = new_alps_res
            else:
                #this is a basil reservation we don't know about already.
                new_alps_res = _construct_alps_res(alps_res)
                if len(new_alps_res.node_ids) == 0:
                    # This reservation has no resources, so Cobalt's internal
                    # resource reservation tracking has no record.  This needs to
                    # be removed.
                    new_alps_res.release()
                else:
                    self.alps_reservations[str(alps_res['batch_id'])] = new_alps_res
        return

    def signal_aprun(self, aprun_id, signame='SIGINT'):
        '''Signal an aprun by aprun id (application_id).  Does not block.
        Use check_killing_aprun to determine completion/termination.  Does not
        depend on the host the aprun(s) was launched from.

        Input:
            aprun_id - integer application id number.
            signame  - string name of signal to send (default: SIGINT)
        Notes:
            Valid signals to apkill are:
            SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGABRT, SIGUSR1, SIGUSR2, SIGURG,
            and SIGWINCH (from apkill(1) man page.)  Also allowing SIGKILL.

        '''
        #Expect changes with an API updte

        #mark legal signals from docos
        if (signame not in ['SIGHUP', 'SIGINT', 'SIGQUIT', 'SIGTERM', 'SIGABRT',
            'SIGUSR1', 'SIGUSR2', 'SIGURG','SIGWINCH', 'SIGKILL']):
            raise ValueError('%s is not a legal value for signame.', signame)
        try:
            retval = Cobalt.Proxy.ComponentProxy('system_script_forker').fork(
                    [APKILL_CMD, '-%s' % signame, '%d' % int(aprun_id)],
                    'aprun_termination', '%s cleanup:'% aprun_id)
            _logger.info("killing backend ALPS application_id: %s", aprun_id)
        except xmlrpclib.Fault:
            _logger.warning("XMLRPC Error while killing backend job: %s, will retry.",
                    aprun_id, exc_info=True)
        except:
            _logger.critical("Unknown Error while killing backend job: %s, will retry.",
                    aprun_id, exc_info=True)
        else:
            self.killing_jobs[aprun_id] = retval
        return

    def check_killing_aprun(self):
        '''Check that apkill commands have completed and clean them from the
        system_script_forker.  Allows for non-blocking cleanup initiation.

        '''

        try:
            system_script_forker = Cobalt.Proxy.ComponentProxy('system_script_forker')
        except:
            self.logger.critical("Cannot connect to system_script_forker.",
                    exc_info=True)
            return
        complete_jobs = []
        rev_killing_jobs = dict([(v,k) for (k,v) in self.killing_jobs.iteritems()])
        removed_jobs = []
        current_killing_jobs = system_script_forker.get_children(None, self.killing_jobs.values())

        for job in current_killing_jobs:
            if job['complete']:
                del self.killing_jobs[rev_killing_jobs[int(job['id'])]]
                removed_jobs.append(job['id'])
        system_script_forker.cleanup_children(removed_jobs)
        return

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
        try:
            res_info = ALPSBridge.reserve(job['user'], job['jobid'],
                int(job['nodes']), job['attrs'], node_id_list)
        except ALPSBridge.ALPSError as exc:
            _logger.warning('unable to reserve resources from ALPS: %s', exc.message)
            return None
        new_alps_res = None
        if res_info is not None:
            new_alps_res = ALPSReservation(job, res_info, self.nodes)
            self.alps_reservations[job['jobid']] = new_alps_res
        return new_alps_res.node_ids


    @exposed
    def validate_job(self, spec):
        '''Basic validation of a job to run on a cray system.  Make sure that
        certain arguments have been passsed in.  On failure raise a
        JobValidationError.

        '''
        spec.update(super(CraySystem, self).validate_job(spec))
        return spec


    @exposed
    def confirm_alps_reservation(self, specs):
        '''confirm or rereserve if needed the ALPS reservation for an
        interactive job.

        '''
        try:
            current_pg = None
            for pgroup in self.process_manager.process_groups.values():
                if pgroup.jobid == int(specs['jobid']):
                    current_pg = pgroup
            #pg = self.process_manager.process_groups[int(specs['pg_id'])]
            pg_id = int(specs['pgid'])
        except KeyError:
            raise
        if current_pg is None:
            raise ValueError('invalid jobid specified')
        # Try to find the alps_res_id for this job.  if we don't have it, then we
        # need to reacquire the source reservation.  The job locations will be
        # critical for making this work.
        with self._node_lock:
            # do not want to hit this during an update.
            alps_res = self.alps_reservations.get(str(current_pg.jobid), None)
            # find nodes for jobid.  If we don't have sufficient nodes, job
            # should die
            job_nodes = [node for node in self.nodes.values()
                            if node.reserved_jobid == current_pg.jobid]
            nodecount = len(job_nodes)
            if nodecount == 0:
                _logger.warning('%s: No nodes reserved for job.', current_pg.label)
                return False
            new_time = job_nodes[0].reserved_until
            node_list = compact_num_list([node.node_id for node in job_nodes])
        if alps_res is None:
            job_info = {'user': specs['user'],
                        'jobid':specs['jobid'],
                        'nodes': nodecount,
                        'attrs': {},
                        }
            self._ALPS_reserve_resources(job_info, new_time, node_list)
            alps_res = self.alps_reservations.get(current_pg.jobid, None)
            if alps_res is None:
                _logger.warning('%s: Unable to re-reserve ALPS resources.',
                        current_pg.label)
                return False

        # try to confirm, if we fail at confirmation, try to reserve same
        # resource set again
        _logger.info('%s/%s: confirming with pagg_id %s', specs['jobid'],
                specs['user'], pg_id)
        ALPSBridge.confirm(int(alps_res.alps_res_id), pg_id)
        return True



