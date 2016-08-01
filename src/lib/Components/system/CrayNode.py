"""Cray-specific node information"""

import logging
from Cobalt.Components.system.ClusterNode import ClusterNode

_logger = logging.getLogger(__name__)

class CrayNode(ClusterNode):
    '''Extension of ClusterNodes for Cray specific systems.  The first system
    targeted for this node type is Cray's XC-40 KNL nodes.


    Intended for use with ALPS.  Not Native mode.
    '''

    CRAY_STATE_MAP = {'UP': 'idle', 'DOWN':'down', 'UNAVAILABLE':'down',
            'ROUTING':'down', 'SUSPECT':'down', 'ADMIN':'down',
            'UNKNOWN':'down', 'UNAVAIL': 'down', 'SWDOWN': 'down',
            'REBOOTQ':'down', 'ADMINDOWN':'down'}

    def __init__(self, spec):
        super(CrayNode, self).__init__(spec)
        self._status = self.CRAY_STATE_MAP[spec['state'].upper()]
        self.node_id = spec['node_id']
        self.role = spec['role'].upper()
        self.attributes['architecture'] = spec['architecture']
        self.segment_details = spec['SocketArray']
        self.ALPS_status = 'UNKNOWN' #Assume unknown state.
        CrayNode.RESOURCE_STATUSES.append('alps-interactive')

    def to_dict(self):
        return self.__dict__

    def __str__(self):
        return str(self.to_dict())

    def reset_info(self, node):
        '''reset node information on restart from a stored node object'''
        super(CrayNode, self).reset_info(node)
        self.status = node.status

    @property
    def status(self):
        return super(CrayNode, self).status

    @status.setter
    def status(self, new_status):
        '''set status using cray states, as well as internal state.
        also, coerce to allocated if we are used by something, but still marked
        idle.

        '''
        #admin down wins.  If an admin says it's down, it's down.
        if self.admin_down:
            self._status = 'down'
            return
        if new_status.upper() in self.CRAY_STATE_MAP.keys():
            self._status = self.CRAY_STATE_MAP[new_status.upper()]
            self.ALPS_status = new_status
        elif new_status in CrayNode.RESOURCE_STATUSES:
            self._status = new_status
        else:
            raise KeyError('%s is not a valid state for Cray Nodes.' % new_status)
        if self._status == 'idle' and self.reserved:
            self.status == 'allocated'

