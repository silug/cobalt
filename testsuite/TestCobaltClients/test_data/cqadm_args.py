"""
This module defines the test argument information list for cqadm.py and will 
dynamically be imported by testutils.py to generate the tests for cqadm.py.

Refer to the TESTUTILS_README.txt for more information about the usage of this module and testutils.py

test_argslist - is a list of dictionaries, each dictionary has all the necessary info for a test.

"""

test_argslist = [
    { "tc_name" : "getq_option_1", "args" : """--getq""", "new_only": True},
    { "tc_name" : "getq_option_2", "args" : """-d --getq""", "new_only": True},
    { "tc_name" : "getq_option_3", "args" : """-f --getq""", "new_only": True},
    { "tc_name" : "preempt_job_1", "args" : """-d --preempt 1 2 3""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "preempt_job_2", "args" : """-f --preempt 1 2 3""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "kill_job_1", "args" : """-d -f --kill 1 2 3""", "old_args" : """--delete 1 2 3""", },
    { "tc_name" : "kill_job_2", "args" : """--kill 1 2 3""", },
    { "tc_name" : "kill_job_3", "args" : """-f --kill 1 2 3""", "old_args" : """-d --delete 1 2 3""", },
    { "tc_name" : "kill_job_4", "args" : """-d --kill 1 2 3""", },
    { "tc_name" : "addq_option_1", "args" : """--addq""", "new_only" : True, },
    { "tc_name" : "addq_option_2", "args" : """--addq myq1 myq2 myq3""", "new_only": True},
    { "tc_name" : "delq_option_1", "args" : """--delq""", "new_only" : True, },
    { "tc_name" : "delq_option_2", "args" : """--delq myq1 myq2 myq3""", },
    { "tc_name" : "stopq_option_1", "args" : """--stopq""", "new_only" : True, },
    { "tc_name" : "stopq_option_2", "args" : """--stopq myq1 myq2 myq3""", },
    { "tc_name" : "startq_option_1", "args" : """--startq""", "new_only" : True, },
    { "tc_name" : "startq_option_2", "args" : """--startq myq1 myq2 myq3""", },
    { "tc_name" : "drainq_option_1", "args" : """--drainq""", "new_only" : True, },
    { "tc_name" : "drainq_option_2", "args" : """--drainq myq1 myq2 myq3""", },
    { "tc_name" : "killq_option_1", "args" : """--killq""", "new_only" : True, },
    { "tc_name" : "killq_option_2", "args" : """--killq myq1 myq2 myq3""", },
    { "tc_name" : "policy_option_1", "args" : """--policy""", 'new_only' : True, },
    { "tc_name" : "policy_option_2", "args" : """--policy 'mypolicy'""", "new_only" : True, },
    { "tc_name" : "policy_option_3", "args" : """--policy 'mypolicy' myq1 myq2""", },
    { "tc_name" : "setq_option_1", "args" : """--setq""", 'new_only' : True, },
    { "tc_name" : "setq_option_2", "args" : """--setq 'a=b b=c a=c'""", "new_only" : True, },
    { "tc_name" : "setq_option_3", "args" : """--setq 'a=b b=c a=c' myq1 myq2""", },
    { "tc_name" : "unsetq_option_1", "args" : """--unsetq""", 'new_only' : True,  },
    { "tc_name" : "unsetq_option_2", "args" : """--unsetq 'a b a'""", "new_only" : True, },
    { "tc_name" : "unsetq_option_3", "args" : """--unsetq 'a b a' myq1 myq2""", },
    { "tc_name" : "setjobid_option_1", "args" : """-j""", 'new_only' : True,  },
    { "tc_name" : "setjobid_option_2", "args" : """--setjobid""", 'new_only' : True, },
    { "tc_name" : "setjobid_option_3", "args" : """-j 1""", },
    { "tc_name" : "setjobid_option_4", "args" : """--setjobid 1""", },
    { "tc_name" : "setjobid_option_5", "args" : """-j 1 --setjobid 2""", },
    { "tc_name" : "run_option_1", "args" : """--run""", 'new_only' : True, },
    { "tc_name" : "run_option_2", "args" : """--run mayaguez""", "new_only" : True, },
    { "tc_name" : "run_option_3", "args" : """--run mayaguez 1 2 3""", 'skip_list' : ['not_bsim'], "new_only": True },
    { "tc_name" : "hold_option_1", "args" : """--hold""", 'new_only' : True, },
    { "tc_name" : "hold_option_2", "args" : """--hold 1 2 3""", },
    { "tc_name" : "hold_option_3", "args" : """-d --hold  1 2 3""", },
    { "tc_name" : "hold_option_4", "args" : """-f --hold  1 2 3""", },
    { "tc_name" : "release_option_1", "args" : """--release""", 'new_only' : True, },
    { "tc_name" : "release_option_2", "args" : """--release 1 2 3""", },
    { "tc_name" : "release_option_3", "args" : """-d --release 1 2 3""", },
    { "tc_name" : "release_option_4", "args" : """-f --release 1 2 3""", },
    { "tc_name" : "release_and_hold", "args" : """--hold --release 1 2 3""", 'new_only' : True, },
    { "tc_name" : "queue_option_1", "args" : """--queue""", 'new_only' : True, },
    { "tc_name" : "queue_option_2", "args" : """--queue myq""", 'new_only' : True, },
    { "tc_name" : "queue_option_3", "args" : """--queue myq 1 2 3""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "time_option_1", "args" : """--time""", 'new_only' : True, },
    { "tc_name" : "time_option_2", "args" : """--time 50""", "new_only" : True, },
    { "tc_name" : "time_option_4", "args" : """--time 50 1 2 3""", },
    { "tc_name" : "update_all_1", "args" : """--hold --queue myq --time 50 4 5 6""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "update_all_2", "args" : """--release --queue myq --time 50 4 5 6""", 'skip_list' : ['not_bsim'], },
    { "tc_name" : "combine_getq_and_addq", "args" : """--getq --addq myq1 myq2 myq3""", "new_only" : True, },
    { "tc_name" : "combine_getq_and_setjobid", "args" : """--getq -j 1 123""", "new_only" : True, },
    { "tc_name" : "combine_time_and_getq", "args" : """--time 50 --getq""", "new_only" : True, },
    { "tc_name" : "combine_release_and_getq", "args" : """--release --getq 123""", "new_only" : True, },
    { "tc_name" : "combine_setq_with_queue", "args" : """--setq 'a=1 b=2' --queue q 1""", "new_only" : True, },
    { "tc_name" : "combine_addq_and_delq", "args" : """--addq --delq q1 q2""", "new_only" : True, },
    { "tc_name" : "combine_addq_and_stopq", "args" : """--stopq --addq q1 q2""", "new_only" : True, },
    { "tc_name" : "combine_addq_and_startq", "args" : """--startq --addq q1 q2""", "new_only" : True, },
    { "tc_name" : "user_hold_option_1", "args" : """--user-hold""", 'new_only' : True, },
    { "tc_name" : "user_hold_option_2", "args" : """--user-hold 1 2 3""", 'new_only' : True, },
    { "tc_name" : "user_hold_option_3", "args" : """-d --user-hold  1 2 3""",  'new_only' : True, },
    { "tc_name" : "user_hold_option_4", "args" : """-f --user-hold  1 2 3""",  'new_only' : True, },
    { "tc_name" : "user_release_option_1", "args" : """--user-release""", 'new_only' : True, },
    { "tc_name" : "user_release_option_2", "args" : """--user-release 1 2 3""", 'new_only' : True, },
    { "tc_name" : "user_release_option_3", "args" : """-d --user-release 1 2 3""", 'new_only' : True, },
    { "tc_name" : "user_release_option_4", "args" : """-f --user-release 1 2 3""", 'new_only' : True, },
    { "tc_name" : "admin_hold_option_1", "args" : """--admin-hold""", 'new_only' : True, },
    { "tc_name" : "admin_hold_option_2", "args" : """--admin-hold 1 2 3""", "old_args": """--hold 1 2 3""", },
    { "tc_name" : "admin_hold_option_3", "args" : """-d --admin-hold  1 2 3""", "old_args": """-d --hold 1 2 3""", },
    { "tc_name" : "admin_hold_option_4", "args" : """-f --admin-hold  1 2 3""", "old_args": """-f --hold 1 2 3""", },
    { "tc_name" : "admin_release_option_1", "args" : """--admin-release""", 'new_only' : True, },
    { "tc_name" : "admin_release_option_2", "args" : """--admin-release 1 2 3""", "old_args": """--release 1 2 3""", },
    { "tc_name" : "admin_release_option_3", "args" : """-d --admin-release 1 2 3""", "old_args": """-d --release 1 2 3""", },
    { "tc_name" : "admin_release_option_4", "args" : """-f --admin-release 1 2 3""", "old_args": """-f --release 1 2 3""", },
    ]
