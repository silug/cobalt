"""Base reservation class for ALPS backend reservations.


    release is intended to call bridge functions, so this needs to be handled in
    the concrete implementation and will vary if we change bridges.
"""
import logging

_logger = logging.getLogger(__name__)

class ALPSBaseReservation(object):
    '''Container for ALPS Reservation information.  Can be used to update
    reservations and also internally relases reservation.

    Should be built from an ALPS reservation response dict as returned by the
    bridge.

    '''

    def __init__(self, job, spec, nodes):
        '''spec should be the information returned from the Reservation Response
        object.

        '''
        self.jobid = int(job['jobid'])
        self.node_ids = [node_id for node_id in spec['reserved_nodes']]
        self.node_names = []
        for node_id in self.node_ids:
            self.node_names.append(nodes[node_id].name)
        self.pg_id = spec.get('pagg_id', None) #process group of executing script
        if self.pg_id is not None:
            self.pg_id = int(self.pg_id)
        self.alps_res_id = int(spec['reservation_id'])
        #self.app_info = spec['ApplicationArray']
        self.user = job['user']
        #self.gid = spec['account_name'] #appears to be gid.
        self.dying = False
        self.dead = False #System no longer has this alps reservation
        _logger.info('ALPS Reservation %s registered for job %s',
                self.alps_res_id, self.jobid)

    def __str__(self):
        return ", ".join([str(self.jobid), str(self.node_ids),
            str(self.node_names), str(self.pg_id), str(self.alps_res_id),
            str(self.user)])

    @property
    def confirmed(self):
        '''Has this reservation been confirmed?  If not, it's got a 2 minute
        lifetime.

        '''
        # An associated process aggregate id (pagg_id) implies confirmation
        return self.pg_id is not None

    def confirm(self, pagg_id):
        '''Mark a reservation as confirmed.  This must be passed back from the
        forker that confirmed the reservation and is the process group id of the
        child process forked for the job.

        Args:
            pagg_id - process aggregate id that has been used for confirmation

        Notes:
            pagg_id used may change based on type of job (interactive vs
            non-interactive).  This also serves as the authentication token for
            any apruns against the reservation.

        '''
        self.pg_id = pagg_id
        _logger.info('ALPS Reservation %s for job %s confirmed',
               self.alps_res_id, self.jobid)

    def release(self):
        '''Release an underlying ALPS reservation.

        Note:
        A reservation may remain if there are still active claims.  When all
        claims are gone

        Returns a list of apids and child_ids for the system script forker
        for any apids that are still cleaning.

        '''
        raise NotImplementedError
