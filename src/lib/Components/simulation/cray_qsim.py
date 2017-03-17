#!/usr/bin/env python

'''Cobalt Queue Simulator (for Blue Gene systems) library'''

import logging
import math
import os
import os.path
import sys
import time
import types

from Cobalt.Components.qsim_base import *
from Cobalt.Components.base import exposed, query
from Cobalt.Util import init_cobalt_config, get_config_option
from Cobalt.Util import expand_num_list
from Cobalt.Components.simulation.cray_simulator import CraySimulator
from Cobalt.Components.system.CrayBaseSystem import chain_loc_list
from Cobalt.Proxy import ComponentProxy, local_components
from Cobalt.Exceptions import ComponentLookupError

_logger = logging.getLogger(__name__)
init_cobalt_config()

MACHINE_ID = 3 #This is a cray-type system
REMOTE_QUEUE_MANAGER = get_config_option('simulator', 'remote_queue_manager', 'cluster-queue-manager')
WALLTIME_AWARE_CONS = get_config_option('simulator', 'walltime_aware_cons', False)
MACHINE_NAME = get_config_option('simulator', 'machine_name', 'Zinc')
DEFAULT_MAX_HOLDING_SYS_UTIL = float(get_config_option('simulator', 'default_max_holding_sys_until', 0.6))
SELF_UNHOLD_INTERVAL = int(get_config_option('simulator', 'self_unhold_interval', 0))
AT_LEAST_HOLD = int(get_config_option('simulator', 'at_least_hold', 600))
TOTAL_NODES = int(get_config_option('system', 'size', 96))
YIELD_THRESHOLD = int(get_config_option('simulator', 'yield_threshold', 0))
BESTFIT_BACKFILL = bool(get_config_option('simulator', 'bestfit_backfill', False))
SJF_BACKFILL = bool(get_config_option('simulator', 'sjf_backfill', True))
MIN_WALLTIME = int(get_config_option('simulator', 'min_walltime', 60))
MAX_WALLTIME = int(get_config_option('simulator', 'max_walltime', 259200)) #MIRA -- 72 Hours
BALANCE_FACTOR = int(get_config_option('simulator', 'balance_factor', 1))

class CrayQsim(CraySimulator):
    '''Cobalt Queue Simulator for Blue Gene systems'''

    implementation = "qsim"
    name = "queue-manager"
    alias = "system"
    logger = _logger

    def __init__(self, *args, **kwargs):

        super(CrayQsim, self).__init__(*args, **kwargs)

        #initialize partitions
        self.sleep_interval = kwargs.get("sleep_interval", 0)

        self.fraction = kwargs.get("BG_Fraction", 1)
        self.sim_start = kwargs.get("bg_trace_start", 0)
        self.sim_end = kwargs.get("bg_trace_end", sys.maxint)
        self.anchor = kwargs.get("Anchor", 0)
        self.backfill = kwargs.get("backfill", "ff")
###--------Partition related
###-------Job related
        self.workload_file = kwargs.get("cray_job")
        self.output_log = MACHINE_NAME + "-" + kwargs.get("outputlog", "")

        self.event_manager = ComponentProxy("event-manager")

        self.time_stamps = [('I', '0', 0, {})]
        self.cur_time_index = 0
        self.queues = SimQueueDict(policy=None)

        self.unsubmitted_job_spec_dict = {}   #{jobid_stringtype: jobspec}

        self.num_running = 0
        self.num_waiting = 0
        self.num_busy = 0
        self.num_end = 0
        self.total_job = 0

####------Walltime prediction
        self.predict_scheme = kwargs.get("predict", False)

        if self.predict_scheme:
            self.walltime_prediction = True
            self.predict_queue = bool(int(self.predict_scheme[0]))
            self.predict_backfill = bool(int(self.predict_scheme[1]))
            self.predict_running = bool(int(self.predict_scheme[2]))
        else:
            self.walltime_prediction = False
            self.predict_queue = False
            self.predict_backfill = False
            self.predict_running = False

        histm_alive = False
        try:
            histm_alive = ComponentProxy("history-manager").is_alive()
        except ComponentLookupError:
            #self.logger.error("failed to connect to histm component", exc_info=True)
            histm_alive = False

        if histm_alive:
            self.history_manager = ComponentProxy("history-manager")
        else:
            self.walltime_prediction = False

#####init jobs (should be after walltime prediction initializing stuff)
        self.init_queues()

#####------walltime-aware spatial scheduling
        self.walltime_aware_cons = False
        self.walltime_aware_aggr = False
        self.wass_scheme = kwargs.get("wass", None)

        if self.wass_scheme == "both":
            self.walltime_aware_cons = True
            self.walltime_aware_aggr = True
        elif self.wass_scheme == "cons":
            self.walltime_aware_cons = True
        elif self.wass_scheme == "aggr":
            self.walltime_aware_aggr = True

###-------CoScheduling start###
        self.cosched_scheme_tup = kwargs.get("coscheduling", (0,0))

        self.mate_vicinity = kwargs.get("vicinity", 0)

        self.cosched_scheme = self.cosched_scheme_tup[0]
        self.cosched_scheme_remote = self.cosched_scheme_tup[1]

        valid_cosched_schemes = ["hold", "yield"]

        if self.cosched_scheme in valid_cosched_schemes and self.cosched_scheme_remote in valid_cosched_schemes:
            self.coscheduling = True
        else:
            self.coscheduling = False

        #key=local job id, value=remote mated job id
        self.mate_job_dict = {}
        #key = jobid, value = nodelist  ['part-or-node-name','part-or-node-name' ]
        self.job_hold_dict = {}

        #record holding job's holding time   jobid:first hold (sec)
        self.first_hold_time_dict = {}

        #record yield jobs's first yielding time, for calculating the extra waiting time
        self.first_yield_hold_time_dict = {}

        #record yield job ids. update dynamically
        self.yielding_job_list = []

        self.cluster_job_trace = kwargs.get("cjob", None)
        if not self.cluster_job_trace:
            self.coscheduling = False

        self.jobid_qtime_pairs = []

        if self.coscheduling:
            self.init_jobid_qtime_pairs()
            # 'disable' coscheduling for a while until cqsim triggers the remote function
            # to initialize mate job dice successfully
            self.coscheduling = False

        self.max_holding_sys_util = DEFAULT_MAX_HOLDING_SYS_UTIL

####----reservation related
        self.reservations = {}
        self.reserve_ratio = kwargs.get("reserve_ratio", 0)
        if self.reserve_ratio > 0:
            self.init_jobid_qtime_pairs()
            self.init_reservations_by_ratio(self.reserve_ratio)

####----log and other
        #initialize PBS-style logger
        self.pbslog = PBSlogger(self.output_log)

        #initialize debug logger
        if self.output_log:
            self.dbglog = PBSlogger(self.output_log+"-debug")
        else:
            self.dbglog = PBSlogger(".debug")

        #finish tag
        self.finished = False

        #register local alias "system" for this component
        local_components["system"] = self

        #initialize capacity loss
        self.capacity_loss = 0

        self.user_utility_functions = {}
        self.builtin_utility_functions = {}

        self.define_builtin_utility_functions()
        self.define_user_utility_functions()

        self.rack_matrix = []

        self.batch = kwargs.get("batch", False)

######adaptive metric-aware cheduling
        self.metric_aware = kwargs.get("metrica", False)
        self.balance_factor = float(kwargs.get("balance_factor"))
        self.window_size = kwargs.get("window_size", 1)

        self.history_wait = {}
        self.history_slowdown = {}
        self.history_utilization = {}

        self.delivered_node_hour = 0
        self.delivered_node_hour2 = 0
        self.jobcount = 0
        self.counted_jobs = []
        self.num_started = 0
        self.started_job_dict = {}
        self.queue_depth_data = []
        self.adaptive = kwargs.get("adaptive", False)
        if self.adaptive:
            print "adaptive scheme=", self.adaptive

####----print some configuration
        if self.wass_scheme:
            print "walltime aware job allocation enabled, scheme = ", self.wass_scheme

        if self.walltime_prediction:
            print "walltime prediction enabled, scheme = ", self.predict_scheme

        if self.fraction != 1:
            print "job arrival intervals adjusted, fraction = ", self.fraction

        if not self.cluster_job_trace:
            #Var = raw_input("press any Enter to continue...")
            pass

##### simulation related
    def get_current_time(self):
        '''this function overrides get_current_time() in bgsched, bg_base_system, and cluster_base_system'''
        return  self.event_manager.get_current_time()

    def get_current_time_sec(self):
        return  self.event_manager.get_current_time()

    def get_current_time_date(self):
        return self.event_manager.get_current_date_time()

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
        evspec['location'] = info.get('location', [])

        self.event_manager.add_event(evspec)

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
                #dbgmsg = "%s:Start:%s:%s" % (timestamp, spec['jobid'], ":".join(spec['location']))
                #self.dbglog.LogMessage(dbgmsg)
            elif eventtype == 'H':  #hold some resources
                message = "%s;H;%s;queue=%s qtime=%s Resource_List.nodect=%s Resource_List.walltime=%s exec_host=%s" % \
                (timestamp, spec['jobid'], spec['queue'], spec['submittime'],
                 spec['nodes'], log_walltime, ":".join(spec['location']))
            elif eventtype == "U":  #unhold some resources
                message = "%s;U;%s;host=%s" % \
                (timestamp, spec['jobid'], ":".join(spec['location']))
            elif eventtype == 'E':  #end
                first_yield_hold = self.first_yield_hold_time_dict.get(int(spec['jobid']), 0)
                if first_yield_hold > 0:
                    overhead = spec['start_time'] - first_yield_hold
                else:
                    overhead = 0
                message = "%s;E;%s;queue=%s qtime=%s Resource_List.nodect=%s Resource_List.walltime=%s start=%s end=%f exec_host=%s runtime=%s hold=%s overhead=%s" % \
                (timestamp, spec['jobid'], spec['queue'], spec['submittime'], spec['nodes'], log_walltime, spec['start_time'],
                 round(float(spec['end_time']), 1), ":".join(spec['location']),
                 spec['runtime'], spec['hold_time'], overhead)
            else:
                print "invalid event type, type=", eventtype
                return
        self.pbslog.LogMessage(message)


 ####reservation related

    def init_starttime_jobid_pairs(self):
        '''used for initializing reservations'''
        pair_list = []

        for id, spec in self.unsubmitted_job_spec_dict.iteritems():
            start = spec['start_time']
            pair_list.append((float(start), int(id)))

        def _stimecmp(tup1, tup2):
            return cmp(tup1[0], tup2[0])

        pair_list.sort(_stimecmp)

        return pair_list

    def init_reservations_by_ratio(self, ratio):
        '''init self.reservations dictionary'''

        if ratio <= 0.5:
            step = int(1.0 / ratio)
            reverse_step = 1
        else:
            step = 1
            reverse_step = int(1.0/(1-ratio))

        i = 0
        temp_dict = {}
        start_time_pairs = self.init_starttime_jobid_pairs()
        for item in start_time_pairs:
            #remote_item = self.remote_jobid_qtime_pairs[i]
            i += 1

            if step > 1 and i % step != 0:
                continue

            if reverse_step > 1 and i % reverse_step == 0:
                continue

            jobid = item[1]
            reserved_time = item[0]
            jobspec = self.unsubmitted_job_spec_dict[str(jobid)]

            nodes = int(jobspec['nodes'])
            if nodes < 512 or nodes> 16384:
                continue

            reserved_location = jobspec['location']
            self.reservations[jobid] = (reserved_time, reserved_location)

            self.insert_time_stamp(reserved_time, "S", {'jobid':jobid})

        print "totally reserved jobs: ", len(self.reservations.keys())

    def reservation_violated(self, expect_end, location):
        '''test if placing a job with current expected end time (expect_end)
        on partition (location) will violate any reservation'''
        violated = False
        for resrv in self.reservations.values():
            start = resrv[0]
            if expect_end < start:
                continue

            reserved_partition = resrv[1]
            if self.location_conflict(location, reserved_partition):
                #print "location conflict:", location, reserved_partition
                violated = True

        return violated

    def location_conflict(self, partname1, partname2):
        '''test if partition 1 is parent or children or same of partition2 '''
        conflict = False

        p = self._partitions[partname2]
        #print partname1, partname2, p.children, p.parents
        if partname1==partname2 or partname1 in p.parents or partname1 in p.parents:
            conflict = True
        return conflict

##### job/queue related
    def _get_queuing_jobs(self):
        jobs = [job for job in self.queues.get_jobs([{'is_runnable':True}])]
        return jobs
    queuing_jobs = property(_get_queuing_jobs)

    def _get_running_jobs(self):
        return [job for job in self.queues.get_jobs([{'has_resources':True}])]
    running_jobs = property(_get_running_jobs)

    def init_queues(self):
        '''parses the work load log file, initializes queues and sorted time
        stamp list'''

        print "Initializing BG jobs, one moment please..."

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
                qtime = date_to_sec(format_sub_time)
                if qtime < self.sim_start or qtime > self.sim_end:
                    continue
                spec['submittime'] = qtime
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

            if tmp.get('runtime'):
                spec['runtime'] = tmp.get('runtime')
            elif tmp.get('start') and tmp.get('end'):
                act_run_time = float(tmp.get('end')) - float(tmp.get('start'))
                if act_run_time <= 0:
                    continue
                if act_run_time / (float(spec['walltime'])*60) > 1.1:
                    act_run_time = float(spec['walltime'])*60
                spec['runtime'] = str(round(act_run_time, 1))
            else:
                continue

            if tmp.get('Resource_List.nodect'):
                spec['nodes'] = tmp.get('Resource_List.nodect')
                if int(spec['nodes']) == TOTAL_NODES:
                    continue
            else:  #invalid job entry, discard
                continue

            if self.walltime_prediction: #*AdjEst*
                if tmp.has_key('walltime_p'):
                    spec['walltime_p'] = int(tmp.get('walltime_p')) / 60 #convert from sec (in log) to min, in line with walltime
                else:
                    ap = self.get_walltime_Ap(spec)
                    spec['walltime_p'] = int(spec['walltime']) * ap
            else:
                spec['walltime_p'] = int(spec['walltime'])

            spec['state'] = 'invisible'
            spec['start_time'] = '0'
            spec['end_time'] = '0'
            spec['queue'] = "default"
            spec['has_resources'] = False
            spec['is_runnable'] = False
            spec['location'] =tmp.get('exec_host', '')  #used for reservation jobs only
            spec['start_time'] = tmp.get('start', 0)  #used for reservation jobs only

            #add the job spec to the spec list
            specs.append(spec)

        specs.sort(subtimecmp)

        #adjust workload density and simulation start time
        if self.fraction != 1 or self.anchor !=0 :
            tune_workload(specs, self.fraction, self.anchor)

        print "simulation time span:"
        print "first job submitted:", sec_to_date(specs[0].get('submittime'))
        print "last job submitted:", sec_to_date(specs[len(specs)-1].get('submittime'))

        self.total_job = len(specs)
        print "total job number:", self.total_job

        #self.add_jobs(specs)

        self.unsubmitted_job_spec_dict = self.init_unsubmitted_dict(specs)

        self.event_manager.add_init_events(specs, MACHINE_ID)

        return 0

    def add_queues(self, specs):
        '''add queues'''
        return self.queues.add_queues(specs)
    add_queues = exposed(query(add_queues))

    def get_queues(self, specs):
        '''get queues'''
        return self.queues.get_queues(specs)
    get_queues = exposed(query(get_queues))

    def init_unsubmitted_dict(self, specs):
        #jobdict = {}
        specdict = {}
        for spec in specs:
            jobid = str(spec['jobid'])
            #new_job = Job(spec)
            #jobdict[jobid] = new_job
            specdict[jobid] = spec
        return specdict

    def get_live_job_by_id(self, jobid):
        '''get waiting or running job instance by jobid'''
        job = None
        joblist = self.queues.get_jobs([{'jobid':int(jobid)}])
        if joblist:
            job = joblist[0]
        return job

    def add_jobs(self, specs):
        '''Add a job'''
        response = self.queues.add_jobs(specs)
        return response
    add_jobs = exposed(query(add_jobs))

    def get_jobs(self, specs):
        '''get a list of jobs, each time triggers time stamp increment and job
        states update'''

        jobs = []

        if self.event_manager.get_go_next():
            #enter a scheduling iteration

            #clear yielding job list
            del self.yielding_job_list[:]

            cur_event = self.event_manager.get_current_event_type()
            cur_event_job = self.event_manager.get_current_event_job

            if cur_event == "S":
                #start reserved job at this time point
                self.run_reserved_jobs()

            if cur_event in ["Q", "E"]:
                #scheduling related events
                self.update_job_states(specs, {}, cur_event)

            self.compute_utility_scores()

            #unhold holding job. MUST be after compute_utility_scores()
            if cur_event == "U":
                cur_job = self.event_manager.get_current_event_job()
                if cur_job in self.job_hold_dict.keys():
                    self.unhold_job(cur_job)
                else:
                    #if the job not in job_hold_dict, do nothing. the job should have already started
                    return []

            if cur_event == "C":
                 if self.job_hold_dict.keys():
                    self.unhold_all()

        self.event_manager.set_go_next(True)

        jobs = self.queues.get_jobs([{'tag':"job"}])

        if self.yielding_job_list:
            jobs = [job for job in jobs if job.jobid not in self.yielding_job_list]

        #before handling the jobs to scheduler, rule out the jobs already having reservations
        if self.reservations:
            jobs = [job for job in jobs if job.jobid not in self.reservations.keys()]

        return jobs
    get_jobs = exposed(query(get_jobs))

    def update_job_states(self, specs, updates, cur_event):
        '''update the state of the jobs associated to the current time stamp'''

        ids_str = str(self.event_manager.get_current_event_job())

        ids = ids_str.split(':')
        #print "current event=", cur_event, " ", ids
        for Id in ids:


            if cur_event == "Q":  # Job (Id) is submitted

                tempspec = self.unsubmitted_job_spec_dict.get(Id, None)

                if tempspec == None:
                    continue

                tempspec['state'] = "queued"   #invisible -> queued
                tempspec['is_runnable'] = True   #False -> True

                self.queues.add_jobs([tempspec])
                self.num_waiting += 1

                self.log_job_event("Q", self.get_current_time_date(), tempspec)


                #del self.unsubmitted_job_spec_dict[Id]



            elif cur_event=="E":  # Job (Id) is completed
                completed_job = self.get_live_job_by_id(Id)

                if completed_job == None:
                    continue

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

        if not self.cluster_job_trace and not self.batch:
            os.system('clear')
            self.print_screen(cur_event)

        return 0

    def run_reserved_jobs(self):
        #handle reserved job (first priority)
        jobid = int(self.event_manager.get_current_event_job())

        if jobid in self.reservations.keys():
            reserved_location = self.reservations.get(jobid)[1]
            self.start_reserved_job(jobid, [reserved_location])

    def start_reserved_job(self, jobid, nodelist):
       # print "%s: start reserved job %s at %s" % (self.get_current_time_date(), jobid, nodelist)
        self.start_job([{'jobid':int(jobid)}], {'location': nodelist})
        del self.reservations[jobid]

    def run_jobs(self, specs, nodelist, user_name=None, resid=None, walltime=None):
        '''run a queued job, by updating the job state, start_time and
        end_time, invoked by bgsched'''
        #print "run job ", specs, " on nodes", nodelist
        if specs == None:
            return 0

        for spec in specs:

            action = "start"
            dbgmsg = ""

            if self.coscheduling:
                local_job_id = spec.get('jobid') #int
                #check whether there is a mate job

                mate_job_id = self.mate_job_dict.get(local_job_id, 0)

                #if mate job exists, get the status of the mate job
                if mate_job_id > 0:
                    remote_status = self.get_mate_jobs_status_local(mate_job_id).get('status', "unknown!")
                    dbgmsg += "local=%s;mate=%s;mate_status=%s" % (local_job_id, mate_job_id, remote_status)

                    if remote_status in ["queuing", "unsubmitted"]:
                        if self.cosched_scheme == "hold": # hold resource if mate cannot run, favoring job
                            action = "start_both_or_hold"
                        if self.cosched_scheme == "yield": # give up if mate cannot run, favoring sys utilization
                            action = "start_both_or_yield"
                    if remote_status == "holding":
                        action = "start_both"

                #to be inserted co-scheduling handling code
                else:
                    pass

            if action == "start":
                #print "BQSIM-normal start job %s on nodes %s" % (spec['jobid'], nodelist)
                self.start_job([spec], {'location': nodelist})
            elif action == "start_both_or_hold":
                #print "try to hold job %s on location %s" % (local_job_id, nodelist)
                mate_job_can_run = False

                #try to invoke a scheduling iteration to see if remote yielding job can run now
                try:
                    mate_job_can_run = ComponentProxy(REMOTE_QUEUE_MANAGER).try_to_run_mate_job(mate_job_id)
                except:
                    self.logger.error("failed to connect to remote queue-manager component!")

                if mate_job_can_run:
                    #now that mate has been started, start local job
                    self.start_job([spec], {'location': nodelist})
                    dbgmsg += " ###start both"
                else:
                    self.hold_job(spec, {'location': nodelist})
            elif action == "start_both":
                #print "start both mated jobs %s and %s" % (local_job_id, mate_job_id)
                self.start_job([spec], {'location': nodelist})
                ComponentProxy(REMOTE_QUEUE_MANAGER).run_holding_job([{'jobid':mate_job_id}])
            elif action == "start_both_or_yield":
                #print "BQSIM: In order to run local job %s, try to run mate job %s" % (local_job_id, mate_job_id)
                mate_job_can_run = False

                #try to invoke a scheduling iteration to see if remote yielding job can run now
                try:
                    mate_job_can_run = ComponentProxy(REMOTE_QUEUE_MANAGER).try_to_run_mate_job(mate_job_id)
                except:
                    self.logger.error("failed to connect to remote queue-manager component!")

                if mate_job_can_run:
                    #now that mate has been started, start local job
                    self.start_job([spec], {'location': nodelist})
                    dbgmsg += " ###start both"
                else:
                    #mate job cannot run, give up the turn. mark the job as yielding.
                    job_id = spec.get('jobid')
                    self.yielding_job_list.append(job_id)  #int
                    #record the first time this job yields
                    if not self.first_yield_hold_time_dict.has_key(job_id):
                        self.first_yield_hold_time_dict[job_id] = self.get_current_time_sec()
                        #self.dbglog.LogMessage("%s: job %s first yield" % (self.get_current_time_date(), job_id))

                    #self.release_allocated_nodes(nodelist)
            if len(dbgmsg) > 0:
                #self.dbglog.LogMessage(dbgmsg)
                pass

#            if self.walltime_aware_aggr:
#                self.run_matched_job(spec['jobid'], nodelist[0])

        #set tag false, enable scheduling another job at the same time
        self.event_manager.set_go_next(False)
        #self.print_screen()

        return len(specs)
    run_jobs = exposed(run_jobs)

    def start_job(self, specs, updates):
        '''update the job state and start_time and end_time when cqadm --run
        is issued to a group of jobs'''
        start_holding = False
        for spec in specs:
            if self.job_hold_dict.has_key(spec['jobid']):
                start_holding = True
        print updates['location']
        partitions = chain_loc_list(updates['location'])
        for partition in partitions:
            if not start_holding:
                self.reserve_partition(partition)
        self.num_busy += len(partitions)

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
        if jobspec['last_hold'] > 0:
            updates['hold_time'] = jobspec['hold_time'] + self.get_current_time_sec() - jobspec['last_hold']

        #determine whether the job is going to fail before completion
        location = newattr['location']
        duration = jobspec['remain_time']

        end = start + duration
        updates['end_time'] = end
        self.insert_time_stamp(end, "E", {'jobid':jobspec['jobid']})

        updates.update(newattr)



        #self.update_jobdict(str(jobid), 'start_time', start)
        #self.update_jobdict(str(jobid), 'end_time', end)
        #self.update_jobdict(str(jobid), 'location', location)
        self.num_started += 1
        #print "start job %s" % self.num_started
        partsize = int(location[0].split('-')[-1])
        #print "now=%s, jobid=%s, start=%s, end=%s, partsize=%s" % (self.get_current_time_date(), jobspec['jobid'], sec_to_date(start), sec_to_date(end), partsize)

        started_job_spec = {'jobid':str(jobspec['jobid']), 'submittime': jobspec['submittime'], 'start_time': start, 'end_time': end, 'location': location, 'partsize': partsize}
        self.started_job_dict[str(jobspec['jobid'])] = started_job_spec

        self.delivered_node_hour2 += (end-start)* partsize / 3600.0

        return updates

    def update_jobdict(self, jobid, _key, _value):
        '''update self.unsubmitted_jobdict'''
        self.unsubmitted_job_spec_dict[jobid][_key] = _value
        if jobid == '280641':
            print "update job %s=, _key=%s, _value=%s, afterupdate=%s" % (jobid, _key, _value, self.unsubmitted_job_spec_dict[jobid][_key])

##### system related

#####--------utility functions

    # order the jobs with biggest utility first
    def utilitycmp(self, job1, job2):
        return -cmp(job1.score, job2.score)

    def compute_utility_scores (self):
        utility_scores = []
        current_time = self.get_current_time_sec()

        #for balanced utility computing
        if self.metric_aware:
            max_wait, avg_wait = self.get_current_max_avg_queue_time()
            max_walltime, min_walltime = self.get_current_max_min_walltime()

        for job in self.queues.get_jobs([{'is_runnable':True}]):
            utility_name = self.queues[job.queue].policy

            args = {'queued_time':current_time - float(job.submittime),
                    'wall_time': 60*float(job.walltime),
                    'wall_time_p':  60*float(job.walltime_p), ##  *AdjEst*
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

                if self.metric_aware:
                    utility_func = self.comput_utility_score_balanced

                utility_func.func_globals.update(args)

                if self.metric_aware:
                    score = utility_func(self.balance_factor, max_wait, max_walltime, min_walltime)
                else:
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
                job.score = score #in trunk it is job.score += score, (coscheduling need to temperally change score)
                #print "job id=%s, score=%s" % (job.jobid, job.score)
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
            #self.logger.error("Can't read utility function definitions from file %s" % get_bgsched_config("utility_file", ""))
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

        def default():
            '''FCFS'''
            val = queued_time
            return val

        def default1():
            '''WFP'''
            if self.predict_queue:
                wall_time_sched = wall_time_p
            else:
                wall_time_sched = wall_time

            val = ( queued_time / wall_time_sched)**3 * size

            return val

        def high_prio():
            val = 1.0
            return val

        self.builtin_utility_functions["default"] = default
        self.builtin_utility_functions["high_prio"] = high_prio


#####----waltime prediction stuff
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


#####---- Walltime-aware Spatial Scheduling part

    def calc_loss_of_capacity(self):
        '''calculate loss of capacity for one iteration'''

        if self.num_waiting > 0:
            idle_nodes = TOTAL_NODES - self.num_busy
            has_loss = False
            for job in self.queuing_jobs:
                if (int(job.nodes)) < idle_nodes:
                    has_loss = True
                    break
            if has_loss:
                loss = self.current_cycle_capacity_loss()
                self.capacity_loss += loss
    calc_loss_of_capacity = exposed(calc_loss_of_capacity)

    def current_cycle_capacity_loss(self):
        loss  = 0
        current_time = self.get_current_time_sec()
        next_time = self.event_manager.get_next_event_time_sec()

        return loss

    def total_capacity_loss_rate(self):
        timespan_sec = self.event_manager.get_time_span()

        total_NH = TOTAL_NODES *  (timespan_sec / 3600)

        #print "total_nodehours=", total_NH
        #print "total loss capcity (node*hour)=", self.capacity_loss / 3600

        loss_rate = self.capacity_loss /  (total_NH * 3600)

        print "capacity loss rate=", loss_rate
        return loss_rate

    @locking
    @exposed
    def find_job_location(self, *args, **kwargs):
        print args, kwargs
        return super(CrayQsim, self).find_job_location(*args, **kwargs)

#####--begin--CoScheduling stuff
    def init_jobid_qtime_pairs(self):
        '''initialize mate job dict'''
        self.jobid_qtime_pairs = []

        for id, spec in self.unsubmitted_job_spec_dict.iteritems():
            qtime = spec['submittime']
            self.jobid_qtime_pairs.append((qtime, int(id)))

        def _qtimecmp(tup1, tup2):
            return cmp(tup1[0], tup2[0])

        self.jobid_qtime_pairs.sort(_qtimecmp)

    def get_jobid_qtime_pairs(self):
        '''get jobid_qtime_pairs list, remote function'''
        return self.jobid_qtime_pairs
    get_jobid_qtime_pairs = exposed(get_jobid_qtime_pairs)

    def set_mate_job_dict(self, remote_mate_job_dict):
        '''set self.mate_job_dict, remote function'''
        self.mate_job_dict = remote_mate_job_dict
        matejobs = len(self.mate_job_dict.keys())
        proportion = float(matejobs) / self.total_job

        self.coscheduling = True

        print "Co-scheduling enabled, blue gene scheme=%s, cluster scheme=%s" % (self.cosched_scheme, self.cosched_scheme_remote)

        print "Number of mate job pairs: %s, proportion in blue gene jobs: %s%%"\
             % (len(self.mate_job_dict.keys()), round(proportion *100, 1))
        self.generate_mate_job_log()

    set_mate_job_dict = exposed(set_mate_job_dict)

    def try_to_run_mate_job(self, _jobid):
        '''try to run mate job, start all the jobs that can run. If the started
        jobs include the given mate job, return True else return False.  _jobid : int
        '''
        #if the job is not yielding, do not continue; no other job is possibly to be scheduled
        if _jobid not in self.yielding_job_list:
            return False

        mate_job_started = False

        #start all the jobs that can run
        while True:
            running_jobs = [job for job in self.queues.get_jobs([{'has_resources':True}])]

            end_times = []

            now = self.get_current_time_sec()

            for job in running_jobs:
                end_time = max(float(job.starttime) + 60 * float(job.walltime), now + 5*60)
                end_times.append([job.location, end_time])

            active_jobs = [job for job in self.queues.get_jobs([{'is_runnable':True}])] #waiting jobs
            active_jobs.sort(self.utilitycmp)

            job_location_args = []
            for job in active_jobs:
                if not job.jobid == _jobid and self.mate_job_dict.get(job.jobid, 0) > 0:
                    #if a job other than given job (_jobid) has mate, skip it.
                    continue

                job_location_args.append({'jobid': str(job.jobid),
                                          'nodes': job.nodes,
                                          'queue': job.queue,
                                          'forbidden': [],
                                          'utility_score': job.score,
                                          'walltime': job.walltime,
                                          'walltime_p': job.walltime_p,  #*AdjEst*
                                          'attrs': job.attrs,
                 } )

            if len(job_location_args) == 0:
                break

            #print "queue order=", [item['jobid'] for item in job_location_args]

            best_partition_dict = self.find_job_location(job_location_args, end_times)

            if best_partition_dict:
                #print "best_partition_dict=", best_partition_dict

                for canrun_jobid in best_partition_dict:
                    nodelist = best_partition_dict[canrun_jobid]

                    if str(_jobid) == canrun_jobid:
                        mate_job_started = True

                    self.start_job([{'tag':"job", 'jobid':int(canrun_jobid)}], {'location':nodelist})
                    #print "bqsim.try_to_run_mate, start job jobid ", canrun_jobid
            else:
                break

        return mate_job_started
    try_to_run_mate_job = exposed(try_to_run_mate_job)

    def run_holding_job(self, specs):
        '''start holding job'''
        for spec in specs:
            jobid = spec.get('jobid')
            nodelist = self.job_hold_dict.get(jobid, None)
            if nodelist == None:
                #print "cannot find holding resources"
                return
            #print "start holding job %s on location %s" % (spec['jobid'], nodelist)
            self.start_job([spec], {'location':nodelist})
            del self.job_hold_dict[jobid]

    run_holding_job = exposed(run_holding_job)

    def hold_job(self, spec, updates):
        '''hold a job. a holding job is not started but hold some resources that can run itself in the future
        once its mate job in a remote system can be started immediatly. Note, one time hold only one job'''

        def _hold_job(job, newattr):
            '''callback function to update job start/end time'''
            temp = job.to_rx()
            newattr = self.hold_job_updates(temp, newattr)
            temp.update(newattr)
            job.update(newattr)
            self.log_job_event("H", self.get_current_time_date(), temp)

        current_holden_nodes = 0
        for partlist in self.job_hold_dict.values():
            host = partlist[0]
            nodes = int(host.split("-")[-1])
            current_holden_nodes += nodes

        nodelist = updates['location']

        partsize = 0
        for partname in nodelist:
            partsize += int(partname.split("-")[-1])

        job_id = spec['jobid']
        if current_holden_nodes + partsize < self.max_holding_sys_util * TOTAL_NODES:
            self.job_hold_dict[spec['jobid']] = nodelist

            if not self.first_hold_time_dict.has_key(job_id):
                self.first_hold_time_dict[job_id] = self.get_current_time_sec()

            for partname in nodelist:
                self.reserve_partition(partname)

            if not self.first_yield_hold_time_dict.has_key(job_id):
                self.first_yield_hold_time_dict[job_id] = self.get_current_time_sec()

            return self.queues.get_jobs([spec], _hold_job, updates)
        else:
            #if execeeding the maximum limite of holding nodes, the job will not hold but yield
            self.yielding_job_list.append(job_id)  #int
            #record the first time this job yields
            if not self.first_yield_hold_time_dict.has_key(job_id):
                self.first_yield_hold_time_dict[job_id] = self.get_current_time_sec()
                self.dbglog.LogMessage("%s: job %s first yield" % (self.get_current_time_date(), job_id))
            return 0

    def hold_job_updates(self, jobspec, newattr):
        '''Return the state updates (including state queued -> running,
        setting the start_time, end_time)'''
        updates = {}

        updates['is_runnable'] = False
        updates['has_resources'] = False
        updates['state'] = "holding"
        updates['last_hold'] = self.get_current_time_sec()

        updates.update(newattr)

        if SELF_UNHOLD_INTERVAL > 0:
            release_time = self.get_current_time_sec() + SELF_UNHOLD_INTERVAL
            self.insert_time_stamp(release_time, "U", {'jobid':jobspec['jobid'], 'location':newattr['location']})

        return updates

    def unhold_job(self, jobid):
        '''if a job holds a partition longer than MAX_HOLD threshold, the job will release the partition and starts yielding'''
        nodelist = self.job_hold_dict.get(jobid)

        #release holden partitions
        if nodelist:
            for partname in nodelist:
                self.release_partition(partname)
        else:
            print "holding job %s not found in job_hold_dict: " % jobid
            return 0

        def _unholding_job(job, newattr):
            '''callback function'''
            temp = job.to_rx()
            newattr = self.unholding_job_updates(temp, newattr)
            temp.update(newattr)
            job.update(newattr)
            self.log_job_event("U", self.get_current_time_date(), temp)

            del self.job_hold_dict[jobid]

        return self.queues.get_jobs([{'jobid':jobid}], _unholding_job, {'location':self.job_hold_dict.get(jobid, ["N"])})

    def unholding_job_updates(self, jobspec, newattr):
        '''unhold job once the job has consumed SELF_UNHOLD_INTERVAL or system-wide unhold_all'''
        updates = {}

        updates['is_runnable'] = True
        updates['has_resources'] = False
        updates['state'] = "queued"
        #set the job to lowest priority at this scheduling point.
        #if no other job gets the nodes it released, the unholden job can hold those nodes again
        updates['score'] = 0
        #accumulate hold_time, adding last hold time to total hold_time
        updates['hold_time'] = jobspec['hold_time'] + self.get_current_time_sec() - jobspec['last_hold']
        updates['last_hold'] = 0

        updates.update(newattr)

        return updates

    def unhold_all(self):
        '''unhold all jobs. periodically invoked to prevent deadlock'''
        for jobid in self.job_hold_dict.keys():
            job_hold_time = self.get_current_time_sec() - self.first_hold_time_dict[jobid]
            #if a job has holden at least 10 minutes, then periodically unhold it
            if job_hold_time >  AT_LEAST_HOLD:
                self.unhold_job(jobid)

    def get_mate_job_status(self, jobid):
        '''return mate job status, remote function, invoked by remote component'''
        #local_job = self.get_live_job_by_id(jobid)
        ret_dict = {'jobid':jobid}
        ret_dict['status'] = self.get_coschedule_status(jobid)
        return ret_dict
    get_mate_job_status = exposed(get_mate_job_status)

    def get_mate_jobs_status_local(self, remote_jobid):
        '''return mate job status, invoked by local functions'''
        status_dict = {}
        try:
            status_dict = ComponentProxy(REMOTE_QUEUE_MANAGER).get_mate_job_status(remote_jobid)
        except:
            self.logger.error("failed to connect to remote cluster queue-manager component!")
            self.dbglog.LogMessage("failed to connect to remote cluster queue-manager component!")
        return status_dict

    def get_coschedule_status(self, jobid):
        '''return job status regarding coscheduling,
           input: jobid
           output: listed as follows:
            1. "queuing"
            2. "holding"
            3. "unsubmitted"
            4. "running"
            5. "ended"
        '''
        ret_status = "unknown"
        job = self.get_live_job_by_id(jobid)
        if job:  #queuing or running
            has_resources = job.has_resources
            is_runnable = job.is_runnable
            if is_runnable and not has_resources:
                ret_status = "queuing"
            if not is_runnable and has_resources:
                ret_status = "running"
            if not is_runnable and not has_resources:
                ret_status = "holding"
        else:  #unsubmitted or ended
            if self.unsubmitted_job_spec_dict.has_key(str(jobid)):
                ret_status = "unsubmitted"
            else:
                ret_status = "unknown"  #ended or no such job
                del self.mate_job_dict[jobid]
        return ret_status

    def generate_mate_job_log(self):
        '''output a file with mate jobs one pair per line'''

        #initialize debug logger
        if self.output_log:
            matelog = PBSlogger(self.output_log+"-mates")
        else:
            matelog = PBSlogger(".mates")

        for k, v in self.mate_job_dict.iteritems():
            msg = "%s:%s" % (k, v)
            matelog.LogMessage(msg)
        matelog.closeLog()

#####--end--CoScheduling stuff


#####----------display stuff

#    def get_midplanes_by_state(self, status):
#        idle_midplane_list = []
#
#        for partition in self._partitions.itervalues():
#            if partition.size == MIDPLANE_SIZE:
#                if partition.state == status:
#                    idle_midplane_list.append(partition.name)
#
#        return idle_midplane_list
#
#    def show_resource(self):
#        '''print rack_matrix'''
#
#        self.mark_matrix()
#
#        for row in self.rack_matrix:
#            for rack in row:
#                if rack[0] == 1:
#                    print "*",
#                elif rack[0] == 0:
#                    print GREENS + 'X' + ENDC,
#                elif rack[0] == 2:
#                    print YELLOWS + '+' + ENDC,
#                else:
#                    print rack[0],
#            print '\r'
#            for rack in row:
#                if rack[1] == 1:
#                    print "*",
#                elif rack[1] == 0:
#                    print GREENS + 'X' + ENDC,
#                elif rack[1] == 2:
#                    print YELLOWS + '+' + ENDC,
#                else:
#                    print rack[1],
#            print '\r'
#
#    def get_holden_midplanes(self):
#        '''return a list of name of 512-size partitions that are in the job_hold_list'''
#        midplanes = []
#        for partlist in self.job_hold_dict.values():
#            partname = partlist[0]
#            midplanes.extend(self.get_midplanes(partname))
#        return midplanes
#
#    def get_midplanes(self, partname):
#        '''return a list of sub-partitions each contains 512-nodes(midplane)'''
#        midplane_list = []
#        partition = self._partitions[partname]
#
#        if partition.size == MIDPLANE_SIZE:
#            midplane_list.append(partname)
#        elif partition.size > MIDPLANE_SIZE:
#            children = partition.children
#            for part in children:
#                if self._partitions[part].size == MIDPLANE_SIZE:
#                    midplane_list.append(part)
#        else:
#            parents = partition.parents
#            for part in parents:
#                if self._partitions[part].size == MIDPLANE_SIZE:
#                    midplane_list.append(part)
#
#        return midplane_list
#
#    def mark_matrix(self):
#        idle_midplanes = self.get_midplanes_by_state('idle')
#        self.reset_rack_matrix()
#        for name in idle_midplanes:  #sample name for a midplane:  ANL-R15-M0-512
#            print name
#            row = int(name[5])
#            col = int(name[6])
#            M = int(name[9])
#            self.rack_matrix[row][col][M] = 1
#        holden_midplanes = self.get_holden_midplanes()
#        if self.coscheduling and self.cosched_scheme == "hold":
#            for name in holden_midplanes:
#                row = int(name[5])
#                col = int(name[6])
#                M = int(name[9])
#                self.rack_matrix[row][col][M] = 2
#
#    def reset_rack_matrix(self):
#        self.rack_matrix = [
#                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
#                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
#                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
#                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
#                [[0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0], [0,0]],
#            ]
#        #self.rack_matrix = [[[0,0] for i in range(8)] for j in range(5)]

    def print_screen(self, cur_event=""):
        '''print screen, show number of waiting jobs, running jobs, busy_nodes%'''

        #os.system('clear')

#        print "Blue Gene"
#
#        if PRINT_SCREEN == False:
#            print "simulation in progress, please wait"
#            return
#
#        current_datetime = self.event_manager.get_current_date_time()
#        print "%s %s" % (current_datetime, cur_event)
#
#        self.show_resource()
#
##        print "number of waiting jobs: ", self.num_waiting
#
#
##        max_wait, avg_wait = self.get_current_max_avg_queue_time()
#
#        #print "maxium waiting time (min): ", int(max_wait / 60.0)
#        #print "average waiting time (min): ", int(avg_wait / 60.0)
#
#        waiting_job_bar = REDS
#        for i in range(self.num_waiting):
#            waiting_job_bar += "*"
#        waiting_job_bar += ENDC
#
#        print waiting_job_bar
#
#        holding_jobs = len(self.job_hold_dict.keys())
#        holding_midplanes = 0
#        hold_partitions = []
#        for partlist in self.job_hold_dict.values():
#            host = partlist[0]
#            hold_partitions.append(host)
#            nodes = int(host.split("-")[-1])
#            holding_midplanes += nodes / MIDPLANE_SIZE
#
#        print "number of running jobs: ", self.num_running
#        running_job_bar = BLUES
#        for i in range(self.num_running):
#            running_job_bar += "+"
#        running_job_bar += ENDC
#        print running_job_bar
#
#        print "number of holding jobs: ", holding_jobs
#
#        print "number of holden midplanes: ", holding_midplanes
#        #print "holden partitions: ", hold_partitions
#
#        midplanes = self.num_busy / MIDPLANE_SIZE
#        print "number of busy midplanes: ", midplanes
#        print "system utilization: ", float(self.num_busy) / TOTAL_NODES
#
#        busy_midplane_bar = GREENS
#
#        i = 0
#        while i < midplanes:
#            busy_midplane_bar += "x"
#            i += 1
#        j = 0
#        busy_midplane_bar += ENDC
#        busy_midplane_bar += YELLOWS
#        while j < holding_midplanes:
#            busy_midplane_bar += "+"
#            j += 1
#            i += 1
#        busy_midplane_bar += ENDC
#        for k in range(i, self.total_midplane):
#            busy_midplane_bar += "-"
#        busy_midplane_bar += REDS
#        busy_midplane_bar += "|"
#        busy_midplane_bar += ENDC
#        print busy_midplane_bar
#        print "completed jobs/total jobs:  %s/%s" % (self.num_end, self.total_job)
#
#        progress = 100 * self.num_end / self.total_job
#
#        progress_bar = ""
#        i = 0
#        while i < progress:
#            progress_bar += "="
#            i += 1
#        for j in range(i, 100):
#            progress_bar += "-"
#        progress_bar += "|"
#        print progress_bar
#
#        #if self.get_current_time_sec() > 1275393600:
#         #   time.sleep(1)
#
#        if self.sleep_interval:
#            time.sleep(self.sleep_interval)
#
#        print "waiting jobs:", [(job.jobid, job.nodes) for job in self.queuing_jobs]
#
##        wait_jobs = [job for job in self.queues.get_jobs([{'is_runnable':True}])]
##
##        if wait_jobs:
##            wait_jobs.sort(self.utilitycmp)
##            top_jobs = wait_jobs[0:5]
##        else:
##            top_jobs = []
##
##        if top_jobs:
##            print "high priority waiting jobs: ", [(job.jobid, job.nodes) for job in top_jobs]
##        else:
##            print "hig priority waiting jobs:"
#
#        #print "holding jobs: ", [(k,v[0].split("-")[-1]) for k, v in self.job_hold_dict.iteritems()]
#        print "\n\n"

    def post_simulation_handling(self):
        '''post screen after simulation completes'''
        #print self.first_yield_hold_time_dict
        capacity_loss_rate = self.total_capacity_loss_rate()
        msg  = "capacity_loss:%f" % capacity_loss_rate
        self.dbglog.LogMessage(msg)

        #calculate avgwait
        total_wait = 0
        count = 0
        for job in self.started_job_dict.itervalues():
            total_wait += job.get('start_time') - job.get('submittime')
            count += 1
        if len(self.started_job_dict) > 0:
            print "average waiting=", (total_wait / count)/60.0
        else:
            _logger.warning('No Jobs Started!')

        if self.adaptive:
            avg_qd = 'N/A'
            for qd in self.queue_depth_data:
                avg_qd = sum(self.queue_depth_data) / len(self.queue_depth_data)
            print "average queue depth=", avg_qd
    post_simulation_handling = exposed(post_simulation_handling)

#############metric-aware###

    def get_current_max_avg_queue_time(self):
        '''return the average waiting time of jobs in the current queue'''
        current_time = self.get_current_time_sec()
        queued_times =[current_time - float(job.submittime) for job in self.queuing_jobs]
        if len(queued_times) > 0:
            max_wait = max(queued_times)
            avg_wait = sum(queued_times) / len(queued_times)
        else:
            max_wait = 0
            avg_wait = 0
        return max_wait, avg_wait

    def get_current_max_min_walltime(self):
        '''return the max and min walltime in the current queue (in seconds)'''
        current_time = self.get_current_time_sec()
        wall_times =[60*float(job.walltime) for job in self.queuing_jobs]
        if len(wall_times) > 0:
            max_walltime = max(wall_times)
            min_walltime = min(wall_times)
        else:
            max_walltime = 0
            min_walltime = 0
        return max_walltime, min_walltime

    def comput_utility_score_balanced(self, balance_factor, max_wait, max_walltime, min_walltime):
        '''compute utility score balancing FCFS and SJF using a balance factor [0, 1]'''
        if max_wait == 0:
            wait_score = 0
        else:
            wait_score = 100.0 * queued_time / max_wait

        if max_walltime == min_walltime:
            length_score = 0
        else:
            length_score = 100.0 * (max_walltime - wall_time)/(max_walltime - min_walltime)

        balanced_score = wait_score * balance_factor + length_score * (1.0 - balance_factor)

        #print "wait=%s, max_wait=%s" % (queued_time, max_wait)
        #print "walltime=%s, MAX_WALLTIME=%s, MIN_WALLTIME=%s" % (wall_time, max_walltime, min_walltime)
        #print "wait_score=%s, length_score=%s, balanced_score=%s" % (wait_score, length_score, balanced_score)

        return balanced_score

    def monitor_metrics(self):
        '''main function of metrics monitoring'''

        self.monitor_metrics_util()
        self.monitor_metrics_wait()

    monitor_metrics = exposed(monitor_metrics)


    def monitor_metrics_wait(self):
        '''main function of metrics monitoring activities for wait'''
    #    print self.get_current_time_date(), " metrics monitor invoked"
        #self.get_utilization_rate(3600*24)
        #current_avg_wait = self.get_avg_wait_last_period(0)
        aggr_wait = self.get_aggr_wait_last_period(0)

        if self.adaptive in ["10", "11"]:
            if aggr_wait > 1000:
                self.balance_factor = 0.5
            else:
                self.balance_factor = 1
                print aggr_wait / 60

        self.queue_depth_data.append(aggr_wait / 60)
#        if self.balance_factor != before:
#            print "balance_factor changed to:", self.balance_factor

    def monitor_metrics_util(self):
        '''main function of metrics monitoring actitivies for utilization'''
        util_instant = self.get_utilization_rate(0)
        util_1h = self.get_utilization_rate(3600)
        util_10h = self.get_utilization_rate(3600*10)
        util_24h = self.get_utilization_rate(3600*24)

        #print util_instant, util_1h, util_10h, util_24h

        if self.adaptive in ["01", "11"]:
            if util_10h > util_24h:
                self.window_size = 1
            else:
                self.window_size = 4

    def get_utilization_rate(self, period):
        '''get the average utilization rate in the last 'period' of time'''

        now = self.get_current_time_sec()

        utilization = 0
        if period==0:
            utilization = float(self.num_busy) / TOTAL_NODES
            return utilization
        elif period > 0:
            start_point = now - period
            total_busy_node_sec = 0

            for k, v in self.started_job_dict.iteritems():
                jobid = k
                if jobid != v.get('jobid'):
                    print "jobid=", jobid, "valueid=", v.get('jobid')
                jobstart = float(v.get("start_time"))
                jobend = float(v.get("end_time"))
                partitions = v.get("location")
                partsize = int(partitions[0].split('-')[-1])

                #jobs totally within the period
                if jobstart > start_point and jobend < now:
                    node_sec =  (jobend - jobstart) * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "1 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960*3600))

                #jobs starting in the period but not ended yet
                if jobstart > start_point and jobstart < now and jobend >= now:
                    node_sec = (now - jobstart) * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "2 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960*3600))

                #jobs started before the period start but ended in the period
                if jobstart <= start_point and jobend > start_point and jobend < now:
                    node_sec = (jobend - start_point) * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "3 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960*3600))

                #jobs started before the period start but ended after the period end
                if jobstart <= start_point and jobend >= now:
                    node_sec = period * partsize
                    total_busy_node_sec += node_sec
                    self.delivered_node_hour += node_sec / 3600
                    #print "4 now=%s, jobid=%s, start=%s, end=%s, partsize=%s, nodehour=%s" % (sec_to_date(now), jobid, sec_to_date(jobstart), sec_to_date(jobend), partsize, node_sec /(40960.0*3600))

            avg_utilization = float(total_busy_node_sec) / (period*TOTAL_NODES)

            return avg_utilization

    def get_avg_wait_last_period(self, period):
        '''get the average waiting in the last 'period' of time'''

        total_wait = 0
        now = self.get_current_time_sec()

        if period==0: #calculate the average waiting of current queuing jobs
            count = 0
            for job in self.queuing_jobs:
                submittime = job.submittime
                wait = now - submittime
                total_wait += wait
                count += 1

            if count > 0:
                avg_wait = total_wait / count
            else:
                avg_wait = 0

        elif period > 0:  #calculate the average waiting of jobs *started* within last period winodw
            start_point = now - period
            count = 0

            for k, v in self.started_job_dict.iteritems():
                jobid = k
                jobsubmit = float(v.get("submittime"))
                jobstart = float(v.get("start_time"))

                #jobs started within the period
                if jobstart > start_point and jobstart < now:
                    jobwait = jobstart - jobsubmit
                    total_wait += jobwait
                    count += 1

            if count > 0:
                avg_wait = total_wait / count
            else:
                avg_wait = 0

        print avg_wait
        return avg_wait

    def get_aggr_wait_last_period(self, period=0):
        '''get the queue depth (aggregate waiting) in the last 'period' of time (in minutes)'''

        total_wait = 0
        now = self.get_current_time_sec()

        if period==0: #calculate the aggr waiting of current queuing jobs
            count = 0
            for job in self.queuing_jobs:
                submittime = job.submittime
                wait = now - submittime
                total_wait += wait
                count += 1

            #agg_wait = total_wait

        elif period > 0:  #calculate the aggr waiting of jobs *started* within last period winodw
            start_point = now - period
            count = 0

            for k, v in self.started_job_dict.iteritems():
                jobid = k
                jobsubmit = float(v.get("submittime"))
                jobstart = float(v.get("start_time"))

                #jobs started within the period
                if jobstart > start_point and jobstart < now:
                    jobwait = jobstart - jobsubmit
                    total_wait += jobwait
                    count += 1

            if count > 0:
                avg_wait = total_wait / count
            else:
                avg_wait = 0

        #print total_wait / 60
        return total_wait / 60

