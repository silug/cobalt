'''Utility funtions for Cobalt programs'''
__revision__ = '$Revision$'

import os
import types
import smtplib
import socket
import sys
import time
import ConfigParser
import popen2
from datetime import date, datetime
from getopt import getopt, GetoptError
from Cobalt.Exceptions import TimeFormatError, TimerException
import logging

import Cobalt

logger = logging.getLogger('Util')

def get_time(date_string):
    '''Parse a time string that may be specified as minutes, HH:MM, HH:MM:SS, or DD:HH:MM:SS, and return the total number of minutes.  Raise an exception for bad values.'''
    time_info = {}

    units = date_string.split(":")

    if len(units) == 1:
        time_info["minutes"] = units[0]
    elif len(units) == 2:
        time_info["hours"] = units[0]
        time_info["minutes"] = units[1]
    elif len(units) == 3:
        time_info["hours"] = units[0]
        time_info["minutes"] = units[1]
        time_info["seconds"] = units[2]
    elif len(units) == 4:
        time_info["days"] = units[0]
        time_info["hours"] = units[1]
        time_info["minutes"] = units[2]
        time_info["seconds"] = units[3]
    else:
        raise TimeFormatError, "time may be specified as minutes, HH:MM, HH:MM:SS, or DD:HH:MM:SS"

    for key in time_info:
        try:
            time_info[key] = int(time_info[key])
        except ValueError:
            raise TimeFormatError, "illegal value '%s' for %s" % (time_info[key], key)
            
    if time_info.has_key("seconds"):
        if time_info["seconds"] < 0 or time_info["seconds"] > 59:
            raise TimeFormatError, "seconds value '%s' outside range [0, 59]" % time_info["seconds"]

    if time_info.has_key("minutes"):
        if len(time_info) == 1:
            if time_info["minutes"] < 0:
                raise TimeFormatError, "minutes value must not be negative"
        else:
            if time_info["minutes"] < 0 or time_info["minutes"] > 59:
                raise TimeFormatError, "minutes value '%s' outside range [0, 59]" % time_info["minutes"]

    if time_info.has_key("hours"):
        if len(time_info) < 4:
            if time_info["hours"] < 0:
                raise TimeFormatError, "hours value must not be negative"
        else:
            if time_info["hours"] < 0 or time_info["hours"] > 23:
                raise TimeFormatError, "hours value '%s' outside range [0, 23]" % time_info["hours"]

    if time_info.has_key("days"):
        if time_info["days"] < 0:
            raise TimeFormatError, "days value must not be negative"

    minutes = time_info.get("minutes", 0)
    minutes += 60 * time_info.get("hours", 0)
    minutes += 1440 * time_info.get("days", 0)

    return minutes


def dgetopt(arglist, opt, vopt, msg):
    '''parse options into a dictionary'''
    ret = {}
    for optname in opt.values() + vopt.values():
        ret[optname] = False
    gstr = "".join(opt.keys()) + "".join([longopt+':' for longopt in vopt.keys()])
    gstr += 'C:'
    try:
        (opts, args) = getopt(arglist, gstr)
    except GetoptError, gerr:
        print gerr
        print msg
        raise SystemExit, 1
    for (gopt, garg) in opts:
        option = gopt[1:]
        if opt.has_key(option):
            ret[opt[option]] = True
        else:
            if option == 'C':
                if 'C' not in opt.keys() + vopt.keys():
                    continue
            ret[vopt[option]] = garg
    return ret, list(args)

def dgetopt_long(arglist, opt, vopt, msg):
    '''parse options into a dictionary, long and short options supported'''
    ret = {}
    for optname in opt.values() + vopt.values():
        ret[optname] = False
    long_opts = []
    gstr = ''

    # options that don't require args
    for o in opt.keys():
        if len(o) > 1:
            long_opts.append(o)
        else:
            gstr = gstr + o
    # options that require args
    for o in vopt.keys():
        if len(o) > 1:
            long_opts.append(o + '=')
        else:
            gstr = gstr + o + ':'

    gstr += 'C:'
    try:
        (opts, args) = getopt(arglist, gstr, long_opts)
    except GetoptError, gerr:
        print gerr
        print msg
        raise SystemExit, 1

    for (gopt, garg) in opts:
        option = gopt.split('-')[-1]
        if opt.has_key(option):
            ret[opt[option]] = True
        else:
            if option == 'C' and 'C' not in opt.keys() + vopt.keys():
                continue
            ret[vopt[option]] = garg

    return ret, list(args)

def print_vertical(rows):
    '''print data in horizontal format'''
    hmax = max([len(str(x)) for x in rows[0]])
    hformat = '    %%-%ds: %s' % (hmax+1, '%s')
    for row in rows[1:]:
        for x in range(len(row)):
            if x == 0:
                print "%s: %s" % (rows[0][x], row[x])
            else:
                print hformat % (rows[0][x], row[x])
        print

def print_tabular(rows):
    '''print data in tabular format'''
    cmax = tuple([-1 * max([len(str(row[index])) for row in rows]) for index in xrange(len(rows[0]))])
    fstring = ("%%%ss  " * len(cmax)) % cmax
    print fstring % rows[0]
    print ((-1 * sum(cmax))  + (len(cmax) * 2)) * '='
    for row in rows[1:]:
        try:
            print fstring % row
        except IOError:
            return

def printTabular(rows, centered = []):
    '''print data in tabular format'''
    for row in rows:
        for index in xrange(len(row)):
            if isinstance(row[index], types.BooleanType):
                if row[index]:
                    row[index] = 'X'
                else:
                    row[index] = ''
    total = 0
    for column in xrange(len(rows[0])):
        width = max([len(str(row[column])) for row in rows])
        for row in rows:
            if column in centered:
                row[column] = row[column].center(width)
            else:
                row[column] = str(row[column]).ljust(width)
        total += width + 2
    try:
        print '  '.join(rows[0])
        print total * '='
        for row in rows[1:]:
            print '  '.join(row)
    except IOError:
        return

def print_dtab(dtab, fields = []):
    '''print dictionary data in tabular format'''
    if not fields:
        fields = dtab[0].keys()
    fieldlen = [(field, max([len(str(drow[field])) for drow in dtab] + [len(field)])) for field in fields]
    fstring = ''
    for key, value in fieldlen:
        fstring += '%%(%s)%ss  ' % (key, (-1 * value))
    header = "".join([("%" + str(value) + "s  ")%(key) for key, value in fieldlen])
    print header
    print (sum([value for key, value in fieldlen]) + (2 * len(fieldlen))) * '='
    for drow in dtab:
        print fstring % drow

def buildRackTopology(partlist):
    '''Build a dict of partition -> (parents, children)'''
    partinfo = {}
    partport = {}
    for part in partlist:
        partport[part['name']] = part
        partinfo[part['name']] = ([], [])
    for part in partlist:
        parents = [ppart['name'] for ppart in partlist if part['name'] in ppart['deps']]
        while parents:
            next = parents.pop()
            partinfo[part['name']][0].append(next)
            ndp = [ppart['name'] for ppart in partlist if next in ppart['deps']]
            parents += ndp
        children = part['deps'][:]  # copy because popping this below would
                                    # clear the deps list, and all children
                                    # are not added properly
        while children:
            next = children.pop()
            partinfo[part['name']][1].append(next)
            children += partport[next]['deps']
    return partinfo

def sendemail(toaddr, subj, msg, smtpserver = 'localhost'):
    '''Sends an email'''
    msgstr = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % ('cobalt@%s' % socket.getfqdn(), (',').join(toaddr), subj))
    try:
        server = smtplib.SMTP(smtpserver)
    except Exception, e:
        print "Problem sending mail", e
        return
    try:
        server.sendmail('cobalt@%s' % socket.getfqdn(), toaddr, msgstr + msg)
    except Exception, msg:
        print 'Problem sending mail', msg
    server.quit()
    
def runcommand(cmd):
    '''Execute command, returning rc, stdout, stderr'''
    cmdp = popen2.Popen3(cmd, True)
    out = []
    err = []
    status = cmdp.wait()
    out += cmdp.fromchild.readlines()
    err += cmdp.childerr.readlines()
    return (status, out, err)

class AccountingLog:
    def __init__(self, name):
        CP = ConfigParser.ConfigParser()
        CP.read(Cobalt.CONFIG_FILES)
        try:
            self.logdir = CP.get('cqm', 'log_dir')
        except ConfigParser.NoOptionError:
            self.logdir = '/var/log/cobalt-accounting'
        self.date = None
        self.logfile = open('/dev/null', 'w+')
        self.name = name
    def RotateLog(self):
        if self.date != time.localtime()[:3]:
            self.date = time.localtime()[:3]
            date_string = "%s_%02d_%02d" % self.date
            logfile = "%s/%s-%s.log" % (self.logdir, self.name, date_string)
            try:
                self.logfile = open(logfile, 'a+')
            except IOError:
                self.logfile = open("/dev/null", 'a+')
    def LogMessage(self, message):
        self.RotateLog()
        timenow = time.strftime("%Y-%m-%d %T", time.localtime())
        
        try:
            self.logfile.write("%s %s\n" % (timenow, message))
            self.logfile.flush()
        except IOError, e:
            logger.error("AccountingLog failure : %s" % e)

class FailureMode(object):
    '''FailureModes are used to report (and supress) errors appropriately
    call Pass() on success and Fail() on error'''
    def __init__(self, name):
        self.name = name
        self.status = True

    def Pass(self):
        '''Check if status was previously failure and report OK status if needed'''
        if not self.status:
            logger.error("Failure %s cleared" % (self.name))
            self.status = True

    def Fail(self):
        '''Check if status was previously success and report failed status if needed'''
        if self.status:
            logger.error("Failure %s occured" % (self.name))
            self.status = False


def processfilter(cmdstr, jobdict):
    '''Run a filter on the job, passing in all job args and processing all output'''
    extra = []
    for key, value in jobdict.iteritems():
        if isinstance(value, list):
            extra.append('%s="%s"' % (key, ':'.join(value)))
        elif isinstance(value, dict):
            extra.append('%s="{%s}"' % (key, str(value)))
        else:
            extra.append('%s="%s"' % (key, value))
    rc, out, err = Cobalt.Util.runcommand(" ".join([cmdstr] + extra))
    if err:
        # strip \n from last line of stderr to make sure only
        # one \n is print'ed 
        err[-1] = err[-1].strip()
        # the lines in err already end in \n from readlines()
        print >> sys.stderr, ''.join(err)
    if rc != 0:
        print >> sys.stderr, "Filter %s failed" % (cmdstr)
        sys.exit(1)
    if out:
        for line in out:
            key, value = line.strip().split('=', 1)
            if key not in jobdict.keys():
                jobdict[key] = value
            elif isinstance(jobdict[key], list):
                jobdict[key] = value.split(':')
            elif isinstance(jobdict[key], dict):
                jobdict[key].update(eval(value))
            else:
                jobdict[key] = value


class Timer (object):
    '''the timer object keeps track of start, stop and elapsed times'''
    def __init__(self, max_time = None):
        self.__start_times = []
        self.__stop_times = []
        if max_time != None and max_time < 0:
            raise TimerException, "maximum time may not be negative (max_time=%s)" % (max_time,)
        self.__max_time = max_time
        self.__elapsed_time = 0.0
    
    def start(self):
        '''(re)start time tracking'''
        if self.is_active:
            raise TimerException, "timer already started"
        self.__start_times.append(time.time())
        
    def stop(self):
        '''stop time tracking'''
        if not self.is_active:
            raise TimerException, "timer not active"
        self.__stop_times.append(time.time())
        self.__elapsed_time += self.__stop_times[-1] - self.__start_times[-1]

    def __get_is_active(self):
        '''determine if the timer is currently running'''
        return len(self.__start_times) > len(self.__stop_times)

    is_active = property(__get_is_active, doc = "flag indicating if the time is currently active")

    def __get_elapsed_time(self):
        '''get the time elapsed while the timer has been active, including any current activity'''
        if not self.is_active:
            return self.__elapsed_time
        else:
            return self.__elapsed_time + time.time() - self.__start_times[-1]

    elapsed_time = property(__get_elapsed_time, doc = "time elapsed while the has been active, including any current activity")

    def __get_has_expired(self):
        '''determine if the timer has expired'''
        if self.__max_time != None:
            return self.elapsed_time > self.__max_time
        else:
            # raise TimerException, "timer does not have a maximum time associated with it"
            return False

    has_expired = property(__get_has_expired, doc = "flag indicating if the timer has expired")

    def __get_start_times(self):
        '''create and return a duplicate list of the start times'''
        return list(self.__start_times)

    start_times = property(__get_start_times, doc = "list of start times")

    def __get_stop_times(self):
        '''create and return a duplicate list of the stop times'''
        return list(self.__stop_times)

    stop_times = property(__get_stop_times, doc = "list of end times")
