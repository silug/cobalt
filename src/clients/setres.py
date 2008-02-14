#!/usr/bin/env python

'''Setup reservations in the scheduler'''
__revision__ = '$Id$'
__version__ = '$Version$'

import getopt, pwd, sys, time
import xmlrpclib
import Cobalt.Util
from Cobalt.Proxy import ComponentProxy, ComponentLookupError

helpmsg = '''Usage: setres.py [--version] [-m] -n name -s <starttime> -d <duration> 
                  -c <cycle time> -p <partition> -q <queue name> 
                  -u <user> [-f] [partion1] .. [partionN]
starttime is in format: YYYY_MM_DD-HH:MM
duration may be in minutes or HH:MM:SS
cycle time may be in minutes or DD:HH:MM:SS
queue name is only needed to specify a name other than the default
cycle time, queue name, and user are optional'''

if __name__ == '__main__':
    if '--version' in sys.argv:
        print "setres %s" % __revision__
        print "cobalt %s" % __version__
        raise SystemExit, 0
    if '-h' in sys.argv or '--help' in sys.argv or len(sys.argv) == 1:
        print helpmsg
        raise SystemExit, 0
    try:
        scheduler = ComponentProxy("scheduler", defer=False)
    except ComponentLookupError:
        print "Failed to connect to scheduler"
        raise SystemExit, 1
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'c:s:d:mn:p:q:u:axC:', [])
    except getopt.GetoptError, msg:
        print msg
        print helpmsg
        raise SystemExit, 1
    try:
        partitions = [opt[1] for opt in opts if opt[0] == '-p'] + args
    except ValueError:
        if args:
            partitions = args
        else:
            print "Must supply either -p with value or partitions as arguments"
            print helpmsg
            raise SystemExit, 1
        

    if '-f' not in sys.argv:
        # we best check that the partitions are valid
        pspec = [{'name':p} for p in partitions]
        try:
            system = ComponentProxy("system", defer=False)
        except ComponentLookupError:
            print "Failed to contact system component for partition check"
            raise SystemExit, 1
        for p in partitions:
            test_parts = system.get_partitions(pspec)
            if len(test_parts) != len(pspec):
                missing = [p for p in partitions if {'name':p} not in test_parts]
                print "Missing partitions: %s" % (" ".join(missing))
                raise SystemExit, 1
    
    try:
        [start] = [opt[1] for opt in opts if opt[0] == '-s']
    except ValueError:
        if '-m' in sys.argv[1:]:
            start = None
        else:
            print "Must supply a start time for the reservation with -s"
            raise SystemExit, 1
    try:
        [duration] = [opt[1] for opt in opts if opt[0] == '-d']
    except ValueError:
        if '-m' in sys.argv[1:]:
            duration = None
        else:
            print "Must supply a duration for the reservation with -d"
            raise SystemExit, 1
    
    if duration:
        try:
            if duration.count(':') == 0:
                dsec = int(duration) * 60
            else:
                units = duration.split(':')
                units.reverse()
                totaltime = 0
                mults = [1, 60, 3600]
                if len(units) > 3:
                    print "time too large"
                    raise SystemExit, 1
                dsec = sum([mults[index] * float(units[index]) for index in range(len(units))])
        except ValueError:
            print "Error: duration '%s' is invalid" % duration
            print "duration may be in minutes or HH:MM:SS"
            raise SystemExit, 1
    if start:
        try:
            (day, rtime) = start.split('-')
            (syear, smonth, sday) = [int(field) for field in day.split('_')]
            (shour, smin) = [int(field) for field in rtime.split(':')]
            starttime = time.mktime((syear, smonth, sday, shour, smin, 0, 0, 0, -1))
            print "Got starttime %s" % (time.strftime('%c', time.localtime(starttime)))
        except ValueError:
            print "Error: start time '%s' is invalid" % start
            print "start time is expected to be in the format: YYYY_MM_DD-HH:MM"
            raise SystemExit, 1
    if '-u' in sys.argv[1:]:
        user = [opt[1] for opt in opts if opt[0] == '-u'][0]
        for usr in user.split(':'):
            try:
                pwd.getpwnam(usr)
            except KeyError:
                print "User %s does not exist" % (usr)
    else:
        user = None
    
    if '-n' in sys.argv[1:]:
        [nameinfo] = [val for (opt, val) in opts if opt == '-n']
    else:
        nameinfo = 'system'
    
    if '-c' in sys.argv[1:]:
        cycle_time = [opt[1] for opt in opts if opt[0] == '-c'][0]
    else:
        cycle_time = None
    
    if cycle_time:    
        try:
            if cycle_time.count(':') == 0:
                cycle_time = int(cycle_time) * 60
            else:
                units = cycle_time.split(':')
                units.reverse()
                totaltime = 0
                mults = [1, 60, 3600, 86400]
                if len(units) > 4:
                    print "time too large"
                    raise SystemExit, 1
                cycle_time = sum([mults[index] * float(units[index]) for index in range(len(units))])
        except ValueError:
            print "Error: cycle time '%s' is invalid" % duration
            print "cycle time may be in minutes or DD:HH:MM:SS"
            raise SystemExit, 1

        

    # modify the existing reservation instead of creating a new one
    if '-m' in sys.argv[1:]:
        if '-n' not in sys.argv[1:]:
            print "-m must by called with -n <reservation name>"
            raise SystemExit
        rname = [arg for (opt, arg) in opts if opt == '-n'][0]
        res_list = scheduler.get_reservations([{'name':rname}])
        if not res_list:
            print "cannot find reservation named '%s'" % rname
            raise SystemExit, 1
        updates = {}
        if user:
            updates['users'] = user
        if start:
            updates['start'] = starttime
        if duration:
            updates['duration'] = dsec
        if cycle_time:
            updates['cycle'] = cycle_time
        if partitions:
            updates['partitions'] = ":".join(partitions)
                
        scheduler.set_reservations([{'name':rname}], updates)
        raise SystemExit, 0

    spec = { 'partitions': ":".join(partitions), 'name': nameinfo, 'users': user, 'start': starttime, 'duration': dsec, 'cycle': cycle_time }
    if '-q' in sys.argv:
        spec['queue'] = [opt[1] for opt in opts if opt[0] == '-q'][0]
    try:
        print scheduler.add_reservations([spec])
    except xmlrpclib.Fault, flt:
        if flt.faultCode==ComponentLookupError.fault_code:
            print "Couldn't contact the queue manager"
            sys.exit(1)
        else:
            print flt.faultString
            sys.exit(1)
    except:
        print "Couldn't contact the scheduler"
        raise
        
