"""Fake Bridge for communicating and fetching system state in ALPS.

This is a mock-up for simulation purposes.  This never actually calls ALPS, nor
is ALPS a requirement on the system you run this on.

"""

# calls in here are assumed to be blocking for now.  Non-blocking is being
# considered for later optimization

import logging
import xml.etree
import sys
import os.path
from xml.etree import ElementTree
import pprint
from cray_messaging import InvalidBasilMethodError, BasilRequest
from cray_messaging import parse_response, ALPSError
from Cobalt.Proxy import ComponentProxy
from Cobalt.Data import IncrID
from Cobalt.Util import sleep
from Cobalt.Util import init_cobalt_config, get_config_option
from Cobalt.Util import compact_num_list, expand_num_list


_logger = logging.getLogger()
init_cobalt_config()

DEFAULT_DEPTH = int(get_config_option('alps', 'default_depth', 72))

ALPS_RESERVATIONS = {}
ALPS_RES_ID_GEN = IncrID()
NODES = {}

class BridgeError(Exception):
    '''Exception class so that we may easily recognize bridge-specific errors.'''
    pass

def init_bridge():
    '''Read in XML for system specification

    '''
    alps_system_file = get_config_option('simulator', 'system_xml', None)
    if alps_system_file is None:
        _logger.critical('No simulator:system_xml file specified.  Aborting.')
        sys.exit(1)
    try:
        alps_system_xml = ElementTree.parse(os.path.expandvars(alps_system_file))
    except IOError:
        _logger.critical('Error reading %s', alps_system_file, exc_info=True)
        sys.exit(1)
    alps_root = alps_system_xml.getroot()
    for node in alps_root:
        NODES.update({int(node.attrib['node_id']): node.attrib})
        NODES[int(node.attrib['node_id'])]['status'] = 'UP'
        NODES[int(node.attrib['node_id'])]['allocated'] = False
    return

def _set_fake_res(params):
    pass

def _unset_fake_res(params):
    pass

def reserve(user, jobid, nodecount, attributes=None, node_id_list=None):

    '''reserve a set of nodes in ALPS'''
    if attributes is None:
        attributes = {}
    params = {}
    param_attrs = {}

    params['user_name'] = user
    params['batch_id'] = jobid
    params['width'] = attributes.get('width', nodecount * DEFAULT_DEPTH)
    params['depth'] = attributes.get('depth', None)
    params['nppn'] = attributes.get('nppn', DEFAULT_DEPTH)
    params['npps'] = attributes.get('nnps', None)
    params['nspn'] = attributes.get('nspn', None)
    params['reservation_mode'] = attributes.get('reservation_mode',
                                                     'EXCLUSIVE')
    params['nppcu'] = attributes.get('nppcu', None)
    params['p-state'] = attributes.get('p-state', None)
    params['p-govenor'] = attributes.get('p-govenor', None)
    if node_id_list is not None:
        params['node_list'] = [int(i) for i in node_id_list]
    return 'fake_reservation'

def release(alps_res_id):
    '''release a set of nodes in an ALPS reservation.  May be used on a
    reservation with running jobs.  If that occurs, the reservation will be
    released when the jobs exit/are terminated.

    Input:
        alps_res_id - id of the ALPS reservation to release.

    Returns:
        True if relese was successful

    Side Effects:
        ALPS reservation will be released.  New aprun attempts agianst
        reservation will fail.

    Exceptions:
        None Yet

    '''
    params = {'reservation_id': alps_res_id}
    retval = _call_sys_forker(BASIL_PATH, str(BasilRequest('RELEASE',
            params=params)))
    return retval

def confirm(alps_res_id, pg_id):
    '''confirm an ALPS reservation.  Call this after we have the process group
    id of the user job that we are reserving nodes for.

    Input:
        alps_res_id - The id of the reservation that is being confirmed.
        pg_id - process group id to bind the reservation to.

    Return:
        True if the reservation is confirmed.  False otherwise.

    Side effects:
        None

    Exceptions:
        None Yet.
    '''
    params = {'pagg_id': pg_id,
              'reservation_id': alps_res_id}
    retval = _call_sys_forker(BASIL_PATH, str(BasilRequest('CONFIRM',
            params=params)))
    return retval

def system():
    '''fetch system information using the SYSTEM query.  Provides memory
    information'''
    params = {}
    req = BasilRequest('QUERY', 'SYSTEM', params)
    return _call_sys_forker(BASIL_PATH, str(req))

def fetch_inventory(changecount=None, resinfo=False):
    '''fetch the inventory for the machine

        changecount -  changecount to send if we only want deltas past a certain
        point
        resinfo - also fetch information on current reservations

        return:
        dictionary of machine information parsed from XML response
    '''
    params = {}
    if changecount is not None:
        params['changecount'] = changecount
    if resinfo:
        params['resinfo'] = True
    #TODO: add a flag for systems with version <=1.4 of ALPS
    req = BasilRequest('QUERY', 'INVENTORY', params)
    if not resinfo:
        return NODES
    else:
        return NODES

def fetch_reservations():
    '''fetch reservation data.  This includes reservation metadata but not the
    reserved nodes.

    '''
    params = {'resinfo': True, 'nonodes' : True}
    req = BasilRequest('QUERY', 'INVENTORY', params)
    return ALPS_RESERVATIONS

def reserved_nodes():
    params = {}
    req = BasilRequest('QUERY', 'RESERVEDNODES', params)
    return _call_sys_forker(BASIL_PATH, str(req))

def fetch_aggretate_reservation_data():
    '''correlate node and reservation data to get which nodes are in which
    reservation.

    '''
    pass

def extract_system_node_data(node_data):
    ret_nodeinfo = {}
    for node_info in node_data['nodes']:
        #extract nodeids, construct from bulk data block...
        for node_id in expand_num_list(node_info['node_ids']):
            node = {}
            node['node_id'] = node_id
            node['state'] = node_info['state']
            node['role'] = node_info['role']
            node['attrs'] = node_info
            ret_nodeinfo[str(node_id)] = node
        del node_info['state']
        del node_info['node_ids']
        del node_info['role']
    return ret_nodeinfo

def print_node_names(spec):
    '''Debugging utility to print nodes returned by ALPS'''
    print spec['reservations']
    print spec['nodes'][0]
    for node in spec['nodes']:
        print node['name']

if __name__ == '__main__':
    _logger.addHandler(logging.StreamHandler())
    init_bridge()
    pprint.pprint(fetch_inventory())
    sys.exit(0)
