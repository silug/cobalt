#!/usr/bin/env python

'''Cobalt Queue Simulator (for Blue Gene systems) library'''

import ConfigParser
import copy
import logging
import math
import os
import os.path
import random
import signal
import sys
import time

from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError
from datetime import datetime

import Cobalt
import Cobalt.Cqparse
import Cobalt.Util

from Cobalt.Components.base import exposed, query, automatic, locking
from Cobalt.Components.cqm import QueueDict, Queue
from Cobalt.Components.simulator import Simulator
from Cobalt.Data import Data, DataList
from Cobalt.Exceptions import ComponentLookupError
from Cobalt.Proxy import ComponentProxy, local_components
from Cobalt.Server import XMLRPCServer, find_intended_location


MACHINE_ID = 0
MACHINE_NAME = "Intrepid"
MAXINT = 2021072587
MIDPLANE_SIZE = 512

logging.basicConfig()
logger = logging.getLogger('Qsim')
TOTAL_NODES = 40960

OPT_RULE = "A1"  # A0, A1, A2, A3, A4, NORMAL, EVEN
RECOVERYOPT = 2 # by default, the failed job is sent back to the rear of the queue
CHECKPOINT = False  #not used in this version
MTTR = 3600   #time to repair partition(in sec), a failed partition will be available again in MTTR seconds,
FRACTION = 1  #factor to tune workload, the times between job arrival will be multipled with FRACTION.(1 means no change.) 
SET_event = set(['I', 'Q', 'S', 'E', 'F', 'R'])

PRINT_SCREEN = True

REDS = '\033[95m'
YELLOWS = '\033[93m'
BLUES = '\033[94m'
GREENS = '\033[92m'
ENDC = '\033[0m'

config = ConfigParser.ConfigParser()
config.read(Cobalt.CONFIG_FILES)
if not config.has_section('cqm'):
    print '''"cqm" section missing from cobalt config file'''
    sys.exit(1)

def get_histm_config(option, default):
    try:
        value = config.get('histm', option)
    except ConfigParser.NoOptionError:
        value = default
    return value

prediction_scheme = get_histm_config("prediction_scheme", "paired")  # ["project", "user", "combine        # *AdjEst*

def parseline(line):
    '''parse a line in work load file, return a temp
    dictionary with parsed fields in the line'''
    temp = {}
    firstparse = line.split(';')
    temp['EventType'] = firstparse[1]
    if temp['EventType'] == 'Q':
        temp['submittime'] = firstparse[0]
    temp['jobid'] = firstparse[2]
    substr = firstparse.pop()
    if len(substr) > 0:
        secondparse = substr.split(' ')
        for item in secondparse:
            tup = item.partition('=')
            if not temp.has_key(tup[0]):
                temp[tup[0]] = tup[2]
    return temp

def parse_work_load(filename):
    '''parse the whole work load file, return a raw job dictionary''' 
    temp = {'jobid':'*', 'submittime':'*', 'queue':'*', 
            'Resource_List.walltime':'*','nodes':'*', 'runtime':'*'}
    # raw_job_dict = { '<jobid>':temp, '<jobid2>':temp2, ...}
    raw_job_dict = {}
    wlf = open(filename, 'r')
    for line in wlf:
        if line[0].isdigit():
            line = line.strip('\n')
            line = line.strip('\r')
            temp = parseline(line)
            jobid = temp['jobid']
            #new job id encountered, add a new entry for this job
            if not raw_job_dict.has_key(jobid):
                raw_job_dict[jobid] = temp
            else:  #not a new job id, update the existing entry
                raw_job_dict[jobid].update(temp)
    return raw_job_dict

def tune_workload(specs, frac):
    '''tune workload heavier or lighter'''
    
    print "inside tune_workload"
    
    def _subtimecmp(spec1, spec2):
        return cmp(spec1.get('submittime'), spec2.get('submittime'))
    
    specs.sort(_subtimecmp)
        
    #calc mtbs
    lastsubtime = 0
    for spec in specs:
        if (lastsubtime==0):
            interval = 0
        else:
            interval = spec['submittime'] - lastsubtime
        lastsubtime =  spec['submittime']
        spec['interval'] = interval
    
     #tune workload heavy or light
    
    last_newsubtime = specs[0].get('submittime')
    
    for spec in specs:
        interval = spec['interval']
        newsubtime = last_newsubtime + frac* interval
        spec['submittime'] = newsubtime
        spec['interval'] = frac* interval
        last_newsubtime = newsubtime    
    
    print "in adjust: last submit job=", specs[len(specs)-1].get('submittime')

def sec_to_date(sec, dateformat="%m/%d/%Y %H:%M:%S"):
    tmp = datetime.fromtimestamp(sec)
    fmtdate = tmp.strftime(dateformat)
    return fmtdate    
                      
def date_to_sec(fmtdate, dateformat="%m/%d/%Y %H:%M:%S"):
    t_tuple = time.strptime(fmtdate, dateformat)
    sec = time.mktime(t_tuple)
    return sec

def qsim_quit():
    print "pid=", os.getpid()
    os.kill(os.getpid(), signal.SIGINT)
    
def get_bgsched_config(option, default):
    try:
        value = config.get('bgsched', option)
    except ConfigParser.NoOptionError:
        value = default
    return value

class Job (Data):
    '''Job for simulation
    Job attribute description and type:
    jobid: int
    submittime:  unix second, float
    queue: queue name, string
    walltime: estimate runtime, minutes, string
    nodes: node number, string
    runtime: seconds, string
    remain_time: seconds, float
    start_time: unix second, float
    end_time: unix second, float
    failure_time: unix second, float
    location: list of string(partition name)
    state: ['invisible', 'running', 'queued', 'ended', 'pending']  string
    is_visible: true/false
    first_subtime: unix second, float, the time that the job sumibitted for the first time
    enque_time: the time the job start waiting in queue, used by scheduler?
    recovering: indicating that the job was failed in the process of recovering
    '''
    
    fields = Data.fields + ["jobid", "submittime", "queue", "walltime",
                            "nodes","runtime", "start_time", "end_time",
                            "failure_time", "location", "state", "is_visible", 
                            "args",
                            "user",
                            "system_state",
                            "starttime",
                            "project",
                            "is_runnable",
                            "is_active",
                            "has_resources",
                            "attr",
                            "score",
                            "remain_time",    
                            ]    

    def __init__(self, spec):
        Data.__init__(self, spec)
        self.tag = 'job'
        #following fields are initialized at beginning of simulation
        self.jobid = int(spec.get("jobid"))
        self.queue = spec.get("queue", "default")
        #self.queue = "default"
                
        self.submittime = spec.get("submittime")   #in seconds
        
        self.walltime = spec.get("walltime")   #in minutes
        self.walltime_p = spec.get("walltime_p") #  *AdjEst* 
        self.user = spec.get("user", "unknown")
        self.project = spec.get("project", "unknown")
        self.nodes = spec.get("nodes", 0)
        self.runtime = spec.get("runtime", 0)
        self.remain_time = float(self.runtime)       
        self.start_time = spec.get('start_time', '0')
        self.end_time = spec.get('end_time', '0')
        self.state = spec.get("state", "invisible")
        self.system_state = ''
        self.starttime = 0
        #self.arrival_time = 0
        #self.failure_time = 0
        self.has_resources = False
        self.is_runnable = spec.get("is_runnable", False)
        self.is_visible = False
        self.score = float(spec.get("score", 0.0))
        self.attrs = spec.get("attrs", {})
        self.args = []
        self.progress = 0
        #self.checkpoint = 1
        self.recovering = False
        self.location = []

class JobList(DataList):
    '''the list of job objects'''
    item_cls = Job
    
    def __init__(self, _queue):
        self.queue = _queue
        
class SimQueue (Queue):
    '''SimQueue object, extended from cqm.Queue, 
     the attribute jobs is qsim.JobList'''
    
    def __init__(self, spec):
        Queue.__init__(self, spec)
        self.jobs = JobList(self)
        self.state = 'running'
        self.tag = 'queue'
        
    def get_joblist(self):
        '''return the job list'''
        return self.jobs
  
class SimQueueDict(QueueDict):
    '''Queue Dict class for simulating, extended from cqm.QueueDict'''
    item_cls = SimQueue
    key = "name"

    def __init__(self, policy):
        QueueDict.__init__(self)
        self.policy = policy
        #create default queue
        self.add_queues([{"name":"default", "policy":"default"}])         
 
    def add_jobs(self, specs, callback=None, cargs={}):
        '''add jobs to queues, if specified queue not exist, create one''' 
        queue_names = self.keys()
        
        for spec in specs:
            if spec['queue'] not in queue_names:
                spec['queue'] = "default"
   
#               self.add_queues([{"name":spec['queue'], "policy":self.policy}])
#               queue_names.append(spec['queue'])
               
        results = []
         # add the jobs to the appropriate JobList
        for spec in specs:
            results += self[spec['queue']].jobs.q_add([spec], callback, cargs)
            
        return results

class PBSlogger:
    '''Logger to generate PBS-style event log'''

    def __init__(self, name):
        #get log directory
        CP = ConfigParser.ConfigParser()
        CP.read(Cobalt.CONFIG_FILES)
        try:
            self.logdir = os.path.expandvars(CP.get('cqm', 'log_dir'))
        except ConfigParser.NoOptionError:
            self.logdir = '.'
            
        if not os.path.isdir(self.logdir):
            os.system('mkdir %s' % self.logdir)        
            
        #determine log filename
        if name:
            filename = "%s/%s-qsim-%s.log" % (self.logdir, MACHINE_NAME, name)
        else:
            self.date = time.localtime()[:3]
            date_string = "%s_%02d_%02d" % self.date
            filename = "%s/%s-qsim-%s.log" % (self.logdir, MACHINE_NAME, date_string)    
        
        self.logfile = open(filename, 'w')
        self.name = name

    def closeLog(self):
        self.logfile.close()

    def LogMessage(self, message):
        '''log message into pbs-style log'''
        try:
            self.logfile.write("%s\n" % (message))
            self.logfile.flush()
        except IOError, e:
            logger.error("PBSlogger failure : %s" % e)
            
    
class BGQsim(Simulator):
    '''Cobalt Queue Simulator for cluster systems'''
    
    implementation = "bqsim"
    name = "queue-manager"
    alias = "system"
    logger = logging.getLogger(__name__)

    def __init__(self, *args, **kwargs):
        
        Simulator.__init__(self, *args, **kwargs)
         
        #initialize partitions
        self.interval = kwargs.get("interval", 0)
        
        partnames = self._partitions.keys()
        self.init_partition(partnames)
        self.inhibit_small_partitions()
        self.part_size_list = []
        #print all partitions
        for part in self._partitions.itervalues():
            if part.scheduled == True:
                print "%s" % (part.name)
     
        for part in self.partitions.itervalues():
            if int(part.size) not in self.part_size_list:
                if part.size >= MIDPLANE_SIZE:
                    self.part_size_list.append(int(part.size))
        self.part_size_list.sort()
        print self.part_size_list
        
        self.workload_file =  kwargs.get("bgjob")
        self.output_log = kwargs.get("outputlog")
        
        self.event_manager = ComponentProxy("event-manager")
      
        walltime_prediction = get_histm_config("walltime_prediction", False)   # *AdjEst*
        print "walltime_prediction=", walltime_prediction
        if walltime_prediction in ["True", "true"]:
            self.walltime_prediction = True
        else:
            self.walltime_prediction = False
        
        #print "walltime_prediction =", self.walltime_prediction   
        histm_alive = False
        try:
            histm_alive = ComponentProxy("history-manager").is_alive()
        except:
            #self.logger.error("failed to connect to histm component", exc_info=True)
            histm_alive = False
        
        if histm_alive:
            self.history_manager = ComponentProxy("history-manager")
        else:
            self.walltime_prediction = False
            
        print "walltime_prediction=", self.walltime_prediction   
        
        self.time_stamps = [('I', '0', 0, {})]
        self.cur_time_index = 0
        self.queues = SimQueueDict(policy=None)
        
        self.invisible_job_dict = {}   # for jobs not submitted, {jobid:job_instance}
        self.invisible_spec_dict = {}   #{jobid: jobspec}

        self.num_running = 0
        self.num_waiting = 0
        self.num_busy = 0
        self.num_end = 0
        self.total_job = 0
        
        self.init_queues()

        #initialize PBS-style logger
        self.pbslog = PBSlogger(self.output_log)
        
        #initialize debug logger
        #if self.output_log:
        #    self.dbglog = PBSlogger(self.output_log+"-debug")
        #else:
        #    self.dbglog = PBSlogger(".debug")
        
        #finish tag
        self.finished = False
        
        #register local alias "system" for this component
        local_components["system"] = self
        
        #initialize capacity loss
        self.capacity_loss = 0
                
        #starting job(id)s at current time stamp. used for calculating capacity loss
        self.starting_jobs = []
        
        self.user_utility_functions = {}
        self.builtin_utility_functions = {}
                        
        self.define_builtin_utility_functions()
        self.define_user_utility_functions()
    
        #a list of job pairs. each pair is stored in a tuple, bg job first, followed by cluster job
        self.mate_job_list = []
        
        self.rack_matrix = []
        self.reset_rack_matrix()
                                            
        print "Simulation starts:"
        
    def register_alias(self):
        '''register alternate name for the Qsimulator, by registering in slp
        with another name for the same location. in this case 'system' is the
        alternate name'''
        try:
            slp = Cobalt.Proxy.ComponentProxy("service-location", defer=False)
        except ComponentLookupError:
            print >> sys.stderr, "unable to find service-location"
            qsim_quit()
        svc_location = slp.locate(self.name)
        if svc_location:
            slp.register(self.alias, svc_location)
    register_alias = automatic(register_alias, 30)
    
    def is_finished(self):
        return self.finished
    is_finished = exposed(is_finished)
    
    def get_current_time(self):
        '''this function overrid the get_current_time in bgsched, bg_base_system, and cluster_base_system'''
        return  self.event_manager.get_current_time()
    
    def get_current_time_sec(self):
        return  self.event_manager.get_current_time()
    
    def get_current_time_date(self):
        return self.event_manager.get_current_date_time()

    def time_increment(self):
        '''the current time stamp increments by 1'''
        self.event_manager.clock_increment()
       
    def insert_time_stamp(self, timestamp, type, info):
        '''insert time stamps in the same order'''
        if type not in SET_event:
            print "invalid event type,", type
            return
        
        evspec = {}
        evspec['jobid'] = info.get('jobid', 0)
        evspec['type'] = type
        evspec['datetime'] = sec_to_date(timestamp)
        evspec['unixtime'] = timestamp
        evspec['machine'] = MACHINE_ID
        
        self.event_manager.add_event(evspec)
        
    def init_partition(self, namelist):
        '''add all paritions and apply activate and enable'''
        func = self.add_partitions
        args = ([{'tag':'partition', 'name':partname, 'size':"*",
                  'functional':False, 'scheduled':False, 'queue':"*",
                  'deps':[]} for partname in namelist],)
        apply(func, args)
        
        func = self.set_partitions
        args = ([{'tag':'partition', 'name':partname} for partname in namelist],
                {'scheduled':True, 'functional': True})
        apply(func, args)
    
    def inhibit_small_partitions(self):
        '''set all partition less than 512 nodes not schedulable and functional'''
        namelist = []
        for partition in self._partitions.itervalues():
            if partition.size < MIDPLANE_SIZE:
                namelist.append(partition.name)
        func = self.set_partitions
        args = ([{'tag':'partition', 'name':partname} for partname in namelist],
                {'scheduled':False})
        apply(func, args)

    def init_queues(self):
        '''parses the work load log file, initializes queues and sorted time 
        stamp list'''
        
        print "Initializing jobs, one moment please..."
        
        raw_jobs = parse_work_load(self.workload_file)
        specs = []
        
        tag = 0
        for key in raw_jobs:
            spec = {}
            tmp = raw_jobs[key]
            
            spec['jobid'] = tmp.get('jobid')
            spec['queue'] = tmp.get('queue')
            
            #convert submittime from "%m/%d/%Y %H:%M:%S" to Unix time sec
            format_sub_time = tmp.get('submittime')
            if format_sub_time:
                spec['submittime'] = date_to_sec(format_sub_time)
                #spec['submittime'] = float(tmp.get('qtime'))
                spec['first_subtime'] = spec['submittime']  #set the first submit time                
            else:
                continue
                
            spec['user'] = tmp.get('user')
            spec['project'] = tmp.get('account')
                
            #convert walltime from 'hh:mm:ss' to float of minutes
            format_walltime = tmp.get('Resource_List.walltime')
            spec['walltime'] = 0
            if format_walltime:
                segs = format_walltime.split(':')
                walltime_minuntes = int(segs[0])*60 + int(segs[1])
                spec['walltime'] = str(int(segs[0])*60 + int(segs[1]))
            else:  #invalid job entry, discard
                continue
            
            if tmp.get('start') and tmp.get('end'):
                act_run_time = float(tmp.get('end')) - float(tmp.get('start'))
                if act_run_time / (float(spec['walltime'])*60) > 1.1:
                    act_run_time = float(spec['walltime'])*60
                spec['runtime'] = str(round(act_run_time, 1))
            else:
                continue
                
            if tmp.get('Resource_List.nodect'):
                spec['nodes'] = tmp.get('Resource_List.nodect')
                if int(spec['nodes']) == 40960:
                    continue                    
            else:  #invalid job entry, discard
                continue
            
            if self.walltime_prediction: #*AdjEst*  
                ap = self.get_walltime_Ap(spec)
                spec['walltime_p'] = int(spec['walltime']) * ap
            else:
                spec['walltime_p'] = int(spec['walltime'])
             
            spec['state'] = 'invisible'
            spec['start_time'] = '0'
            spec['end_time'] = '0'
            spec['queue'] = "default"
            spec['has_resource'] = False
            spec['is_runnable'] = False
            
            #add the job spec to the spec list            
            specs.append(spec)
                
        #adjust workload density
        if FRACTION != 1:
            tune_workload(specs, FRACTION)
            print "workload adjusted: last submit job=", specs[len(specs)-1].get('submittime')
        
        self.total_job = len(specs)
        print "total job number:", self.total_job
        
        #self.add_jobs(specs)
       
        self.invisible_spec_dict = self.init_invisible_dict(specs)
        
                        
        self.event_manager.add_init_events(specs, MACHINE_ID)

        return 0
    
    def init_invisible_dict(self, specs):
        #jobdict = {}
        specdict = {}
        for spec in specs:
            jobid = str(spec['jobid'])
            #new_job = Job(spec)
            #jobdict[jobid] = new_job
            specdict[jobid] = spec
        return specdict
    
    def get_walltime_Ap(self, spec):  #*AdjEst*
        '''get walltime adjusting parameter from history manager component'''
        
        projectname = spec.get('project')
        username = spec.get('user')
        if prediction_scheme == "paired":
            return self.history_manager.get_Ap_by_keypair(username, projectname)
        
        Ap_proj = self.history_manager.get_Ap('project', projectname)
        
        Ap_user = self.history_manager.get_Ap('user', username)
         
        if prediction_scheme == "project":
            return Ap_proj
        elif prediction_scheme == "user":
            print "Ap_user==========", Ap_user
            return Ap_user
        elif prediction_scheme == "combined":
            return (Ap_proj + Ap_user) / 2
        else:
            return self.history_manager.get_Ap_by_keypair(username, projectname)
            
    def log_job_event(self, eventtype, timestamp, spec):
        '''log job events(Queue,Start,End) to PBS-style log'''
        def len2 (_input):
            _input = str(_input)
            if len(_input) == 1:
                return "0" + _input
            else:
                return _input
        if eventtype == 'Q':  #submitted(queued) for the first time
            message = "%s;Q;%s;queue=%s" % (timestamp, spec['jobid'], spec['queue'])
        elif eventtype == 'R':  #resume running after failure recovery
            message = "%s;R;%s" % (timestamp, ":".join(spec['location']))
        else:
            wall_time = spec['walltime']
            walltime_minutes = len2(int(float(wall_time)) % 60)
            walltime_hours = len2(int(float(wall_time)) // 60)
            log_walltime = "%s:%s:00" % (walltime_hours, walltime_minutes)
            if eventtype == 'S':  #start running 
                message = "%s;S;%s;queue=%s qtime=%s Resource_List.nodect=%s Resource_List.walltime=%s start=%s exec_host=%s" % \
                (timestamp, spec['jobid'], spec['queue'], spec['submittime'], 
                 spec['nodes'], log_walltime, spec['start_time'], ":".join(spec['location']))
            elif eventtype == 'E':  #end
                message = "%s;E;%s;queue=%s qtime=%s Resource_List.nodect=%s Resource_List.walltime=%s start=%s end=%f exec_host=%s runtime=%s" % \
                (timestamp, spec['jobid'], spec['queue'], spec['submittime'], spec['nodes'], log_walltime, spec['start_time'], 
                 round(float(spec['end_time']), 1), ":".join(spec['location']),
                 spec['runtime'])
            else:
                print "invalid event type, type=", type
                return
        self.pbslog.LogMessage(message)
    
    def update_job_states(self, specs, updates):
        '''update the state of the jobs associated to the current time stamp'''
        
        jobid = self.event_manager.get_current_event_job()
        
        ids_str = str(self.event_manager.get_current_event_job())
        
        ids = ids_str.split(':')
        cur_event = self.event_manager.get_current_event_type()
        #print "current event=", cur_event, " ", ids
        for Id in ids:
            
            if cur_event == "Q":  # Job (Id) is submitted
                tempspec = self.invisible_spec_dict[Id]
                
                tempspec['state'] = "queued"   #invisible -> queued
                tempspec['is_runnable'] = True   #False -> True
                
                self.queues.add_jobs([tempspec])
                self.num_waiting += 1
                del self.invisible_spec_dict[Id]

            elif cur_event=="E":  # Job (Id) is completed
                joblist = self.queues.get_jobs([{'jobid':int(Id)}])
                
                if joblist:
                    completed_job = joblist[0]
                else:
                    return 0
                
                #release partition
                for partition in completed_job.location:
                    self.release_partition(partition)
                
                partsize = int(self._partitions[partition].size)
                self.num_busy -= partsize
                                
                #log the job end event
                jobspec = completed_job.to_rx()
                #print "end jobspec=", jobspec
                if jobspec['end_time']:
                    end = float(jobspec['end_time'])
                else:
                    end = 0
                end_datetime = sec_to_date(end)   
                self.log_job_event("E", end_datetime, jobspec)
                
                #delete the job instance from self.queues
                self.queues.del_jobs([{'jobid':int(Id)}])
                self.num_running -= 1
                self.num_end += 1
        
        self.print_screen(cur_event)
                
        return 0
    
    def run_job_updates(self, jobspec, newattr):
        ''' return the state updates (including state queued -> running, 
        setting the start_time, end_time)'''
        updates = {}
        
        #print "enter run_job_updates, jobspec=", jobspec
        
        start = self.get_current_time_sec()
        updates['start_time'] = start
        updates['starttime'] = start

        updates['state'] = 'running'
        updates['system_state'] = 'running'
        updates['is_runnable'] = False
        updates['has_resources'] = True

        #print self.get_current_time_date(), "run job state change, job", jobspec['jobid'], \
        #     ":", jobspec['state'], "->", updates['state']
             
        #determine whether the job is going to fail before completion
        location = newattr['location']
        duration = jobspec['remain_time']
        
        end = start + duration
        updates['end_time'] = end
        self.insert_time_stamp(end, "E", {'jobid':jobspec['jobid']})
        
        updates.update(newattr)
    
        return updates
     
    def add_jobs(self, specs):
        '''Add a job'''
        response = self.queues.add_jobs(specs)
        return response
    add_jobs = exposed(query(add_jobs))
        
    def current_idle_node(self):
        '''number of idle nodes'''
        idle_nodes = 0
        midplanes = self.get_all_idle_midplanes()
        idle_nodes = 512 * len(midplanes)
        return idle_nodes
    
    def current_cycle_capacity_loss(self):
        loss  = 0
        current_time = self.get_current_time_sec()
        next_time = self.get_next_time_sec()
        print "current_time=", current_time
        print "next_time=", next_time
        time_length = next_time - current_time
        idle_node = self.current_idle_node()
        loss = time_length * idle_node
        return loss
    
    def total_capacity_loss_rate(self):
        last_stamp = len(self.time_stamps) - 1
        total_period_sec = self.time_stamps[last_stamp] [2] - self.time_stamps[1][2]
        total_NH = TOTAL_NODES *  (total_period_sec / 3600)
            
        print "total_nodehours=", total_NH
        print "total loss capcity (node*hour)=", self.capacity_loss / 3600
        
        loss_rate = self.capacity_loss /  (total_NH * 3600)
        
        print "capacity loss rate=", loss_rate
        return loss_rate        
    
    def get_jobs(self, specs):
        '''get a list of jobs, each time triggers time stamp increment and job
        states update'''

        jobs = []
        
        if self.event_manager.get_go_next():
            self.update_job_states(specs, {})
            
            self.compute_utility_scores()

        self.event_manager.set_go_next(True)
        
        jobs = self.queues.get_jobs([{'tag':"job"}])

        return jobs
    get_jobs = exposed(query(get_jobs))
    
    def _get_job_by_id(self, jobid):
        jobs = self.queues.get_jobs([{'jobid':jobid}])
        if len(jobs) == 1:
            return jobs[0]
        else:
            return None
   
    def add_queues(self, specs):
        '''add queues'''
        return self.queues.add_queues(specs)
    add_queues = exposed(query(add_queues))
    
    def get_queues(self, specs):
        '''get queues'''
        return self.queues.get_queues(specs)
    get_queues = exposed(query(get_queues))
   
    def run_jobs(self, specs, nodelist):
        '''run a queued job, by updating the job state, start_time and
        end_time, invoked by bgsched'''
        #print "run job ", specs, " on nodes", nodelist
        if specs:
            self.start_job(specs, {'location': nodelist})
            self.print_screen()
            
            #set tag false, enable scheduling another job at the same time
            self.event_manager.set_go_next(False)
            
        return len(specs)
    run_jobs = exposed(run_jobs)
    
    def start_job(self, specs, updates):
        '''update the job state and start_time and end_time when cqadm --run
        is issued to a group of jobs'''
        
        partitions = updates['location']
        for partition in partitions:
            self.reserve_partition(partition)
            partsize = int(self._partitions[partition].size)
            self.num_busy += partsize
            
        self.num_running += 1
        self.num_waiting -= 1
                    
        def _start_job(job, newattr):
            '''callback function to update job start/end time'''
            temp = job.to_rx()
            newattr = self.run_job_updates(temp, newattr)
            temp.update(newattr)
            job.update(newattr)
            self.log_job_event('S', self.get_current_time_date(), temp)
        
        return self.queues.get_jobs(specs, _start_job, updates)
    
    def compute_utility_scores (self):
        utility_scores = []
        current_time = time.time()
            
        for job in self.queues.get_jobs([{'is_runnable':True}]):    
            utility_name = self.queues[job.queue].policy
            args = {'queued_time':current_time - float(job.submittime), 
                    'wall_time': 60*float(job.walltime_p),    #  *AdjEst*
                    'size': float(job.nodes),
                    'user_name': job.user,
                    'project': job.project,
                    'queue_priority': int(self.queues[job.queue].priority),
                    #'machine_size': max_nodes,
                    'jobid': int(job.jobid),
                    'score': job.score,
                    'recovering': job.recovering,
                    'state': job.state,
                    }
            try:
                if utility_name in self.builtin_utility_functions:
                    utility_func = self.builtin_utility_functions[utility_name]
                else:
                    utility_func = self.user_utility_functions[utility_name]
                utility_func.func_globals.update(args)
                score = utility_func()
            except KeyError:
                # do something sensible when the requested utility function doesn't exist
                # probably go back to the "default" one
                
                # and if we get here, try to fix it and throw away this scheduling iteration
                self.logger.error("cannot find utility function '%s' named by queue '%s'" % (utility_name, job.queue))
                self.user_utility_functions[utility_name] = self.builtin_utility_functions["default"]
                self.logger.error("falling back to 'default' policy to replace '%s'" % utility_name)
                return
            except:
                # do something sensible when the requested utility function explodes
                # probably go back to the "default" one
                # and if we get here, try to fix it and throw away this scheduling iteration
                self.logger.error("error while executing utility function '%s' named by queue '%s'" % (utility_name, job.queue), \
                    exc_info=True)
                self.user_utility_functions[utility_name] = self.builtin_utility_functions["default"]
                self.logger.error("falling back to 'default' policy to replace '%s'" % utility_name)
                return
            
            try:
                job.score += score
            except:
                self.logger.error("utility function '%s' named by queue '%s' returned a non-number" % (utility_name, job.queue), \
                    exc_info=True)
                self.user_utility_functions[utility_name] = self.builtin_utility_functions["default"]
                self.logger.error("falling back to 'default' policy to replace '%s'" % utility_name)
                return
    
    def define_user_utility_functions(self):
        self.logger.info("building user utility functions")
        self.user_utility_functions.clear()
        filename = os.path.expandvars(get_bgsched_config("utility_file", ""))
        try:
            f = open(filename)
        except:
            self.logger.error("Can't read utility function definitions from file %s" % get_bgsched_config("utility_file", ""))
            return
        
        str = f.read()
        
        try:
            code = compile(str, filename, 'exec')
        except:
            self.logger.error("Problem compiling utility function definitions.", exc_info=True)
            return
        
        globals = {'math':math, 'time':time}
        locals = {}
        try:
            exec code in globals, locals
        except:
            self.logger.error("Problem executing utility function definitions.", exc_info=True)
            
        for thing in locals.values():
            if type(thing) is types.FunctionType:
                if thing.func_name in self.builtin_utility_functions:
                    self.logger.error("Attempting to overwrite builtin utility function '%s'.  User version discarded." % \
                        thing.func_name)
                else:
                    self.user_utility_functions[thing.func_name] = thing
    define_user_utility_functions = exposed(define_user_utility_functions)
    
    def define_builtin_utility_functions(self):
        self.logger.info("building builtin utility functions")
        self.builtin_utility_functions.clear()
        
        # I think this duplicates cobalt's old scheduling policy
        # higher queue priorities win, with jobid being the tie breaker
        def default0():
            val = queue_priority + 0.1
            return val
        
        def default1():
            '''FCFS'''
            val = queued_time
            return val
        
        def default():
            '''WFP, supporting coordinated job recovery'''
                        
            wall_time_sec = wall_time*60
                            
            val = ( queued_time / wall_time_sec)**3 * (size/64.0)
            
            return val
    
        def high_prio():
            val = 1.0
            return val
    
        self.builtin_utility_functions["default"] = default
        self.builtin_utility_functions["high_prio"] = high_prio
        
    def _find_job_location(self, args, drain_partitions=set(), backfilling=False):
        jobid = args['jobid']
        nodes = args['nodes']
        queue = args['queue']
        utility_score = args['utility_score']
        walltime = args['walltime']
        walltime_p = args['walltime_p']  #*AdjEst* 
        forbidden = args.get("forbidden", [])
        required = args.get("required", [])
        
        best_score = sys.maxint
        best_partition = None
        
        available_partitions = set()
        
        requested_location = None
        if args['attrs'].has_key("location"):
            requested_location = args['attrs']['location']
                
        if required:
            # whittle down the list of required partitions to the ones of the proper size
            # this is a lot like the stuff in _build_locations_cache, but unfortunately, 
            # reservation queues aren't assigned like real queues, so that code doesn't find
            # these
            for p_name in required:
                available_partitions.add(self.cached_partitions[p_name])
                available_partitions.update(self.cached_partitions[p_name]._children)

            possible = set()
            for p in available_partitions:            
                possible.add(p.size)
                
            desired_size = 64
            job_nodes = int(nodes)
            for psize in sorted(possible):
                if psize >= job_nodes:
                    desired_size = psize
                    break
            
            for p in available_partitions.copy():
                if p.size != desired_size:
                    available_partitions.remove(p)
                elif p.name in self._not_functional_set:
                    available_partitions.remove(p)
                elif requested_location and p.name != requested_location:
                    available_partitions.remove(p)
        else:
            for p in self.possible_locations(nodes, queue):
                skip = False
                for bad_name in forbidden:
                    if p.name==bad_name or bad_name in p.children or bad_name in p.parents:
                        skip = True
                        break
                if not skip:
                    if (not requested_location) or (p.name == requested_location):
                        available_partitions.add(p)
        
        available_partitions -= drain_partitions
        now = self.get_current_time()
        
        for partition in available_partitions:
            # if the job needs more time than the partition currently has available, look elsewhere    
            
            runtime_estimate = float(walltime_p)   # *Adj_Est*
            
            if backfilling:
                if 60*runtime_estimate > (partition.backfill_time - now):
                    continue
                
            if partition.state == "idle":
                # let's check the impact on partitions that would become blocked
                score = 0
                for p in partition.parents:
                    if self.cached_partitions[p].state == "idle" and self.cached_partitions[p].scheduled:
                        score += 1
                
                # the lower the score, the fewer new partitions will be blocked by this selection
                if score < best_score:
                    best_score = score
                    best_partition = partition        

        if best_partition:
            return {jobid: [best_partition.name]}

    def find_job_location(self, arg_list, end_times):
        best_partition_dict = {}
        
        if self.bridge_in_error:
            return {}
        
        self.cached_partitions = self.partitions

        # build the cached_partitions structure first
        self._build_locations_cache()

        # first, figure out backfilling cutoffs per partition (which we'll also use for picking which partition to drain)
        job_end_times = {}
        for item in end_times:
            job_end_times[item[0][0]] = item[1]
            
        now = self.get_current_time()
        for p in self.cached_partitions.itervalues():
            if p.state == "idle":
                p.backfill_time = now
            else:
                p.backfill_time = now + 5*60
            p.draining = False
        
        for p in self.cached_partitions.itervalues():    
            if p.name in job_end_times:
                if job_end_times[p.name] > p.backfill_time:
                    p.backfill_time = job_end_times[p.name]
                
                for parent_name in p.parents:
                    parent_partition = self.cached_partitions[parent_name]
                    if p.backfill_time > parent_partition.backfill_time:
                        parent_partition.backfill_time = p.backfill_time
        
        for p in self.cached_partitions.itervalues():
            if p.backfill_time == now:
                continue
            
            for child_name in p.children:
                child_partition = self.cached_partitions[child_name]
                if child_partition.backfill_time == now or child_partition.backfill_time > p.backfill_time:
                    child_partition.backfill_time = p.backfill_time

        
        # first time through, try for starting jobs based on utility scores
        drain_partitions = set()
        
        for job in arg_list:
            partition_name = self._find_job_location(job, drain_partitions)
            if partition_name:
                best_partition_dict.update(partition_name)
                break
            
            location = self._find_drain_partition(job)
            if location is not None:
                for p_name in location.parents:
                    drain_partitions.add(self.cached_partitions[p_name])
                for p_name in location.children:
                    drain_partitions.add(self.cached_partitions[p_name])
                    self.cached_partitions[p_name].draining = True
                drain_partitions.add(location)
                #self.logger.info("job %s is draining %s" % (winning_job['jobid'], location.name))
                location.draining = True
        
        # the next time through, try to backfill, but only if we couldn't find anything to start
        if not best_partition_dict:
            
            # arg_list.sort(self._walltimecmp)

            for args in arg_list:
                partition_name = self._find_job_location(args, backfilling=True)
                if partition_name:
                    self.logger.info("backfilling job %s" % args['jobid'])
                    best_partition_dict.update(partition_name)
                    break

        # reserve the stuff in the best_partition_dict, as those partitions are allegedly going to 
        # be running jobs very soon
        #
        # also, this is the only part of finding a job location where we need to lock anything
        self._partitions_lock.acquire()
        try:
            for p in self.partitions.itervalues():
                # push the backfilling info from the local cache back to the real objects
                p.draining = self.cached_partitions[p.name].draining
                p.backfill_time = self.cached_partitions[p.name].backfill_time
                
            for jobid, partition_list in best_partition_dict.iteritems():
                part = self.partitions[partition_list[0]]
                part.used_by = int(jobid)
                part.reserved_until = self.get_current_time() + 5*60
                part.state = "allocated"
                for p in part._parents:
                    if p.state == "idle":
                        p.state = "blocked (%s)" % (part.name,)
                for p in part._children:
                    if p.state == "idle":
                        p.state = "blocked (%s)" % (part.name,)
        except:
            self.logger.error("error in find_job_location", exc_info=True)
        self._partitions_lock.release()
        
        return best_partition_dict
    find_job_location = locking(exposed(find_job_location))

    #display stuff
    
    def get_midplanes_by_state(self, status):
        idle_midplane_list = []
                
        for partition in self._partitions.itervalues():
            if partition.size == MIDPLANE_SIZE:
                if partition.state == status:
                    idle_midplane_list.append(partition.name)
                
        return idle_midplane_list        

    def show_resource(self):
        '''print rack_matrix'''
        
        self.mark_matrix()
        
        for row in self.rack_matrix:
            for rack in row:
                if rack[0] == 1:
                    print "*",
                elif rack[0] == 0:
                    print GREENS+'X'+ENDC,
            print '\r'
            for rack in row:
                if rack[1] == 1:
                    print "*",
                elif rack[1] == 0:
                    print GREENS+'X'+ENDC,
            print '\r'
             
    def mark_matrix(self):
        idle_midplanes = self.get_midplanes_by_state('idle')
        self.reset_rack_matrix()
        for name in idle_midplanes:  #sample name for a midplane:  ANL-R15-M0-512
            row = int(name[5])
            col = int(name[6])
            M = int(name[9])
            self.rack_matrix[row][col][M] = 1
            
    def reset_rack_matrix(self):
        self.rack_matrix = [
                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
            ]
    
    def print_screen(self, cur_event=""):
        '''print screen, show number of waiting jobs, running jobs, busy_nodes%'''
        
        os.system('clear')
        
        if PRINT_SCREEN == False:
            print "simulation in progress, please wait"
            return            
        
        current_datetime = self.event_manager.get_current_date_time()
        print "%s %s" % (current_datetime, cur_event)
        
        self.show_resource()
         
        print "number of waiting jobs: ", self.num_waiting
        
        waiting_job_bar = REDS
        for i in range(self.num_waiting):
            waiting_job_bar += "*"
        waiting_job_bar += ENDC
            
        print waiting_job_bar
        
        print "number of running jobs: ", self.num_running
        
        running_job_bar = BLUES
        for i in range(self.num_running):
            running_job_bar += "+"
        running_job_bar += ENDC
        print running_job_bar
        
        midplanes = self.num_busy / 512
        print "number of busy midplanes: ", midplanes
        print "system utilization: ", float(self.num_busy) / 40960.0
        busy_midplane_bar = GREENS
        
        i = 0
        while i < midplanes:
            busy_midplane_bar += "x"
            i += 1
        for j in range(i, 80):
            busy_midplane_bar += "-"
        busy_midplane_bar += ENDC
        busy_midplane_bar += REDS
        busy_midplane_bar += "|"
        busy_midplane_bar += ENDC
        print busy_midplane_bar
        print "completed jobs/total jobs:  %s/%s" % (self.num_end, self.total_job)
        
        progress = 100 * self.num_end / self.total_job
        
        progress_bar = ""
        i = 0
        while i < progress:
            progress_bar += "="
            i += 1
        for j in range(i, 100):
            progress_bar += "-"
        progress_bar += "|"
        print progress_bar
        if self.interval:
            time.sleep(self.interval)
        