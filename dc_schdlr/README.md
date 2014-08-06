# DcSchdlr

A Simple Data Center resource scheduler simulator. Two level scheduling:
1) user pool: DRF user pool level fairness
2) User specific scheduler within user pool

Accepts commands:

`state # print current jobs and resource statistics`

`submit <user-id> <num_tasks> <task-duration> <per-task-cpus> <per-task-memory> # to submit a job`

`step n # step the simulation by n time units`

## Installation/Build from source

Change to root dir: dc_schdlr
Execute: 

  $ rake build

This will build a gem: pkg/dc_schdlr-0.0.1.gem

  $ gem install --local pkg/pkg/dc_schdlr-0.0.1.gem

## Usage

dc_schdlr [--help]

Accepts following commands from stdin:

`state # print current jobs and resource statistics`

`submit <num_tasks> <task-duration> <per-task-cpus> <per-task-memory> # to submit a job`

`step n # step the simulation by n time units`

## Sample run
```
submit user-1 20 4 4 24
submit user-2 15 10 3 20
submit user-3 10 7 2 16
state
Time: 0, Total Memory Utilized: 940,Total CPUs utilized: 145.
total_jobs: 3, finished: 0,pending(whose even a single task hasn't been scheduled): 0, running_jobs: 3, UserSchedulerQ length = 0
utilized_cpus_vector [7, 5, 8, 5, 7, 7, 8, 7, 7, 7, 7, 8, 7, 7, 8, 8, 8, 8, 8, 8]
utilized_mem_vector [44, 36, 48, 36, 44, 44, 48, 52, 44, 44, 44, 48, 44, 44, 56, 48, 56, 56, 56, 48]

USER: user-1:
Time: 0, total_jobs: 1, finished: 0,pending: 0, running_jobs: 1

USER: user-2:
Time: 0, total_jobs: 1, finished: 0,pending: 0, running_jobs: 1

USER: user-3:
Time: 0, total_jobs: 1, finished: 0,pending: 0, running_jobs: 1
submit user-3 20 20 4 20
state
Time: 0, Total Memory Utilized: 940,Total CPUs utilized: 145.
total_jobs: 4, finished: 0,pending(whose even a single task hasn't been scheduled): 1, running_jobs: 3, UserSchedulerQ length = 1
utilized_cpus_vector [7, 5, 8, 5, 7, 7, 8, 7, 7, 7, 7, 8, 7, 7, 8, 8, 8, 8, 8, 8]
utilized_mem_vector [44, 36, 48, 36, 44, 44, 48, 52, 44, 44, 44, 48, 44, 44, 56, 48, 56, 56, 56, 48]

USER: user-1:
Time: 0, total_jobs: 1, finished: 0,pending: 0, running_jobs: 1

USER: user-2:
Time: 0, total_jobs: 1, finished: 0,pending: 0, running_jobs: 1

USER: user-3:
Time: 0, total_jobs: 2, finished: 0,pending: 1, running_jobs: 1
step 4
state
Time: 4, Total Memory Utilized: 860,Total CPUs utilized: 145.
total_jobs: 4, finished: 1,pending(whose even a single task hasn't been scheduled): 0, running_jobs: 3, UserSchedulerQ length = 0
utilized_cpus_vector [7, 5, 8, 5, 7, 7, 8, 7, 7, 7, 7, 8, 7, 7, 8, 8, 8, 8, 8, 8]
utilized_mem_vector [40, 36, 40, 36, 40, 40, 40, 52, 40, 40, 40, 40, 40, 40, 52, 40, 56, 52, 56, 40]

USER: user-1:
Time: 4, total_jobs: 1, finished: 1,pending: 0, running_jobs: 0

USER: user-2:
Time: 4, total_jobs: 1, finished: 0,pending: 0, running_jobs: 1

USER: user-3:
Time: 4, total_jobs: 2, finished: 0,pending: 0, running_jobs: 2
step 3
state
Time: 7, Total Memory Utilized: 700,Total CPUs utilized: 125.
total_jobs: 4, finished: 2,pending(whose even a single task hasn't been scheduled): 0, running_jobs: 2, UserSchedulerQ length = 0
utilized_cpus_vector [7, 3, 8, 3, 7, 7, 8, 3, 7, 7, 7, 8, 7, 7, 4, 8, 6, 4, 6, 8]
utilized_mem_vector [40, 20, 40, 20, 40, 40, 40, 20, 40, 40, 40, 40, 40, 40, 20, 40, 40, 20, 40, 40]

USER: user-1:
Time: 7, total_jobs: 1, finished: 1,pending: 0, running_jobs: 0

USER: user-2:
Time: 7, total_jobs: 1, finished: 0,pending: 0, running_jobs: 1

USER: user-3:
Time: 7, total_jobs: 2, finished: 1,pending: 0, running_jobs: 1
step 3
state
Time: 10, Total Memory Utilized: 400,Total CPUs utilized: 80.
total_jobs: 4, finished: 3,pending(whose even a single task hasn't been scheduled): 0, running_jobs: 1, UserSchedulerQ length = 0
utilized_cpus_vector [4, 0, 8, 0, 4, 4, 8, 0, 4, 4, 4, 8, 4, 4, 4, 8, 0, 4, 0, 8]
utilized_mem_vector [20, 0, 40, 0, 20, 20, 40, 0, 20, 20, 20, 40, 20, 20, 20, 40, 0, 20, 0, 40]

USER: user-1:
Time: 10, total_jobs: 1, finished: 1,pending: 0, running_jobs: 0

USER: user-2:
Time: 10, total_jobs: 1, finished: 1,pending: 0, running_jobs: 0

USER: user-3:
Time: 10, total_jobs: 2, finished: 1,pending: 0, running_jobs: 1
step 5
state
Time: 15, Total Memory Utilized: 400,Total CPUs utilized: 80.
total_jobs: 4, finished: 3,pending(whose even a single task hasn't been scheduled): 0, running_jobs: 1, UserSchedulerQ length = 0
utilized_cpus_vector [4, 0, 8, 0, 4, 4, 8, 0, 4, 4, 4, 8, 4, 4, 4, 8, 0, 4, 0, 8]
utilized_mem_vector [20, 0, 40, 0, 20, 20, 40, 0, 20, 20, 20, 40, 20, 20, 20, 40, 0, 20, 0, 40]

USER: user-1:
Time: 15, total_jobs: 1, finished: 1,pending: 0, running_jobs: 0

USER: user-2:
Time: 15, total_jobs: 1, finished: 1,pending: 0, running_jobs: 0

USER: user-3:
Time: 15, total_jobs: 2, finished: 1,pending: 0, running_jobs: 1
step 5
state
Time: 20, Total Memory Utilized: 400,Total CPUs utilized: 80.
total_jobs: 4, finished: 3,pending(whose even a single task hasn't been scheduled): 0, running_jobs: 1, UserSchedulerQ length = 0
utilized_cpus_vector [4, 0, 8, 0, 4, 4, 8, 0, 4, 4, 4, 8, 4, 4, 4, 8, 0, 4, 0, 8]
utilized_mem_vector [20, 0, 40, 0, 20, 20, 40, 0, 20, 20, 20, 40, 20, 20, 20, 40, 0, 20, 0, 40]

USER: user-1:
Time: 20, total_jobs: 1, finished: 1,pending: 0, running_jobs: 0

USER: user-2:
Time: 20, total_jobs: 1, finished: 1,pending: 0, running_jobs: 0

USER: user-3:
Time: 20, total_jobs: 2, finished: 1,pending: 0, running_jobs: 1
step 5
state
Time: 25, Total Memory Utilized: 0,Total CPUs utilized: 0.
total_jobs: 4, finished: 4,pending(whose even a single task hasn't been scheduled): 0, running_jobs: 0, UserSchedulerQ length = 0
utilized_cpus_vector [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
utilized_mem_vector [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

USER: user-1:
Time: 25, total_jobs: 1, finished: 1,pending: 0, running_jobs: 0

USER: user-2:
Time: 25, total_jobs: 1, finished: 1,pending: 0, running_jobs: 0

USER: user-3:
Time: 25, total_jobs: 2, finished: 2,pending: 0, running_jobs: 0
```

