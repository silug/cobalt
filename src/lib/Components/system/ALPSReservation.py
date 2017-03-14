"""ALPS system reservation implementation for XC40 Rhine/Redwood bridge"""

import logging
import Cobalt.Components.system.AlpsBridge as ALPSBridge
from Cobalt.Components.system.ALPSBaseReservation import ALPSBaseReservation
_logger = logging.getLogger(__name__)

class ALPSReservation(ALPSBaseReservation):
    '''XC40 Rhine/Redwood implementation of ALPS reservation information
    container.  Built from an ALPSBridge reposnse dictionary.

    '''
    def __init__(self, *args, **kwargs):
        super(ALPSReservation, self).__init__(*args, **kwargs)

    def release(self):
        '''Release an underlying ALPS reservation.

        Note:
        A reservation may remain if there are still active claims.  When all
        claims are gone

        Returns a list of apids and child_ids for the system script forker
        for any apids that are still cleaning.

        '''
        if self.dying:
            #release already issued.  Ignore
            return
        apids = []
        status = ALPSBridge.release(self.alps_res_id)
        if int(status['claims']) != 0:
            _logger.info('ALPS reservation: %s still has %s claims.',
                    self.alps_res_id, status['claims'])
            # fetch reservation information so that we can send kills to
            # interactive apruns.
            resinfo = ALPSBridge.fetch_reservations()
            apids = _find_non_batch_apids(resinfo['reservations'], self.alps_res_id)
        else:
            _logger.info('ALPS reservation: %s has no claims left.',
                self.alps_res_id)
        self.dying = True
        return apids

def _find_non_batch_apids(resinfo, alps_res_id):
    '''Extract apids from non-basil items.'''
    apids = []
    for alps_res in resinfo:
        if str(alps_res['reservation_id']) == str(alps_res_id):
            #wow, this is ugly. Traversing the XML from BASIL
            for applications in alps_res['ApplicationArray']:
                for application in applications.values():
                    for app_data in application:
                        # applicaiton id is at the app_data level.  Multiple
                        # commands don't normally happen.  Maybe in a MPMD job?
                        # All commands will have the same applicaiton id.
                        for commands in app_data['CommandArray']:
                            for command in commands.values():
                                # BASIL is the indicaiton of a apbasil
                                # reservation.  apruns with the application of
                                # BASIL would be an error.
                                if command[0]['cmd'] != 'BASIL':
                                    apids.append(app_data['application_id'])
    return apids

