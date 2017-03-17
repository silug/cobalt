"""Hardware abstraction layer for the system on which process groups are run.

Classes:
BGSimProcessGroup -- virtual process group running on the system
Simulator -- simulated system component
"""

import logging
import sys
import os
import time

try:
    from elementtree import ElementTree
except ImportError:
    from xml.etree import ElementTree

from Cobalt.Util import init_cobalt_config, get_config_option, compact_num_list
from Cobalt.Components.system.CrayBaseSystem import CrayBaseSystem
from Cobalt.Components.system.base_pg_manager import ProcessGroupManager
from Cobalt.Components.system.CrayNode import CrayNode
from Cobalt.Data import Data, DataDict, IncrID
from Cobalt.Components.base import Component, exposed, automatic, query
from Cobalt.Exceptions import ProcessGroupCreationError, ComponentLookupError
from Cobalt.Proxy import ComponentProxy
from Cobalt.Statistics import Statistics
from Cobalt.DataTypes.ProcessGroup import ProcessGroup

logger = logging.getLogger(__name__)
init_cobalt_config()



class CraySimProcessGroup(ProcessGroup):
    """Process Group modified for Blue Gene Simulator"""

    def __init__(self, spec):
        ProcessGroup.__init__(self, spec)
        self.nodect = spec.get("nodect", None)


class CraySimulator(CrayBaseSystem):

    """Generic system simulator.

    Methods:
    configure -- load partitions from an xml file
    reserve_partition -- lock a partition for use by a process_group (exposed)
    release_partition -- release a locked (busy) partition (exposed)
    add_process_groups -- add (start) a process group on the system (exposed, query)
    get_process_groups -- retrieve process groups (exposed, query)
    wait_process_groups -- get process groups that have exited, and remove them from the system (exposed, query)
    signal_process_groups -- send a signal to the head process of the specified process groups (exposed, query)
    update_partition_state -- simulates updating partition state from the bridge API (automatic)
    """

    name = "system"
    implementation = "simulator"

    logger = logger

    def __init__(self, *args, **kwargs):
        super(CraySimulator, self).__init__(*args, **kwargs)
        self.failed_components = set()
        self.process_manager = ProcessGroupManager(pgroup_type=CraySimProcessGroup)
        self.config_file = kwargs.get("config_file", get_config_option('simulator', 'system_def_file', None))
        if self.config_file is not None:
            self.logger.log(1, "init: loading machine configuration")
            self.configure(self.config_file)
            self.logger.log(1, "init: recomputing partition state")
            self.update_partition_state()
        self._gen_node_to_queue()

    def __getstate__(self):
        state = {}
        state = super(CraySimulator, self).__getstate__()
        state.update({
                'simulator_version': 5,
                'failed_components': self.failed_components})
        return state

    def __setstate__(self, state):
        try:
            self.logger.log(1, "restart: initializing base system class")
            super(CraySimualtor, self).__setstate__(state)
            self.failed_components = state.get('failed_components', set())
            self.config_file = os.path.expandvars(get_config_option('simulator', 'system_def_file', ""))
            if self.config_file:
                self.logger.log(1, "restart: loading machine configuration")
                self.configure(self.config_file)
                self.logger.log(1, "restart: restoring partition state")
                self._restore_partition_state(state)
                self.logger.log(1, "restart: recomputing partition state")
                self._recompute_partition_state()
        except:
            self.logger.error("A fatal error occurred while restarting the system component", exc_info=True)
            sys.exit(1)
        self._gen_node_to_queue()

    def save_me(self):
        Component.save(self)
    save_me = automatic(save_me, float(get_config_option('bgsystem', 'save_me_interval', 10)))

    def configure(self, config_file):
        """
        Configure simulated partitions.

        Arguments:
        config_file -- xml configuration file
        """

        self.logger.log(1, "configure: opening machine configuration file")

        try:
            system_doc = ElementTree.parse(config_file)
        except IOError:
            self.logger.error("unable to open file: %r", config_file)
            self.logger.error("exiting...")
            sys.exit(1)
        except:
            self.logger.error("problem loading data from file: %r", config_file, exc_info=True)
            self.logger.error("exiting...")
            sys.exit(1)

        system_def = system_doc.getroot()
        if system_def.tag != "CraySystem":
            self.logger.error("unexpected root element in %r: %r", config_file, system_def.tag)
            self.logger.error("exiting...")
            sys.exit(1)

        # this is going to hold partition objects from the bridge (not our own Partition)
        self.logger.log(1, "configure: acquiring machine information and creating partition objects")
        self.nodes.clear()
        for node_def in system_def.iterfind('Node'):

            node = CrayNode(node_def.attrib)
            node.queues = [queue.attrib['name'] for queue in node_def.iterfind('Queue')]
            node.managed = True
            self.nodes.update({node.node_id: node})

        return

    def _ALPS_reserve_resources(self, job, new_time, node_id_list):
        '''stub to allow for simulated allocation to happen'''
        return node_id_list[:int(job['nodes'])]

    @automatic
    def update_partition_state(self):
        '''We've overriden the bridge, ultimately we can probably make this use the "Trestle"'''
        with self._node_lock:
            try:
                # first determine if the partition and associate node cards are in use
                now = time.time()
                for node in self.nodes.values():
                    # since we don't have the bridge, a partition which isn't busy
                    # should be set to idle and then blocked states can be derived
                    if node.status not in ["busy", "allocated"]:
                        node.status = "idle"
                    elif node.reserved:
                        if now > node.reserved_until:
                            self.reserve_resources_until(node.node_id, None, node.reserved_jobid)

                # then set parition states based on that usage as well as failed hardware, resource reservations, etc.
                self._recompute_partition_state()
            except:
                self.logger.error("error in update_partition_state", exc_info=True)
        return

    def _recompute_partition_state(self):
        '''mark nodes down if we've got hardware down events'''
        with self._node_lock:
            for node in self.nodes.values():
                if (node.status in ['idle', 'cleanup-pending', 'cleanup'] and
                    node in self.failed_components):
                    node.status = 'down'

    def _mark_partition_for_cleaning(self, pname, jobid):
        pass

    @exposed
    def reserve_partition(self, name, size=None):
        """Reserve a partition and block all related partitions.

        Arguments:
        name -- name of the partition to reserve
        size -- size of the process group reserving the partition (optional)
        """

        with self._node_lock:
            try:
                node = self.nodes[name]
            except KeyError:
                self.logger.error("reserve_partition(%r, %r) [does not exist]", name, size)
                return False
            if node.state != "allocated":
                self.logger.error("reserve_partition(%r, %r) [%s]", name, size, node.state)
                return False
            node.state = "busy"
        # explicitly call this, since the above "busy" is instantaneously available
        self.update_partition_state()
        self.logger.info("reserve_partition(%r, %r)", name, size)
        return True

    @exposed
    def release_partition(self, name):
        """Release a reserved partition.

        Arguments:
        name -- name of the partition to release
        """
        with self._node_lock:
            try:
                node = self.nodes[name]
            except KeyError:
                self.logger.error("release_partition(%r) [already free]", name)
                return False
            if not node.state == "busy":
                self.logger.info("release_partition(%r) [not busy]", name)
                return False

            if node.used_by is not None:
                node.state = "allocated"
            else:
                node.state = "idle"
        self.update_partition_state()
        self.logger.info("release_partition(%r)", name)
        return True

    @exposed
    def add_failed_components(self, component_names):
        '''take a list of locations to mark offline'''
        success = []
        for name in component_names:
            if self.nodes.get(name, None) is not None:
                self.failed_components.add(name)
                success.append(name)
                self.nodes[name].status = 'DOWN'
        return success

    @exposed
    def del_failed_components(self, component_names):
        '''restore locations to service'''
        success = []
        for name in component_names:
            try:
                self.failed_components.remove(name)
                success.append(name)
                self.nodes[name].status = 'UP'
            except KeyError:
                pass
        return success

    @exposed
    def list_failed_components(self, component_names=None):
        '''remote fetch of out of service components'''
        if component_names is None:
            return list(self.failed_components)
        return [component for component in self.failed_components if component in component_names]
