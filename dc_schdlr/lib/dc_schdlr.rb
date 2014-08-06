# Defines classess like Resourcepool (Data center resources),
# Jobs, Scheduler and Simulator

require "dc_schdlr/version"
require "priority_queue"

module DcSchdlr
  
  class DCResourcePool
    CPUS_PER_MACHINE = 8
    MEMORY_PER_MACHINE = 64 # 64Gs
    NUM_MACHINES = 20

    attr_accessor :utilized_cpus_vector
    attr_accessor :utilized_mem_vector
    attr_accessor :total_cpus_utilized
    attr_accessor :total_memory_utilized
    attr_accessor :total_cpus
    attr_accessor :total_mem

    def initialize()
      @total_cpus = NUM_MACHINES*CPUS_PER_MACHINE
      @total_mem = NUM_MACHINES*MEMORY_PER_MACHINE
      @total_cpus_utilized = 0
      @total_memory_utilized = 0
      @utilized_mem_vector = Array.new(NUM_MACHINES,0)
      @utilized_cpus_vector = Array.new(NUM_MACHINES,0)
    end
  end

  class Job
    attr_reader :job_id
    attr_reader :num_tasks
    attr_reader :task_duration
    attr_reader :pertask_cpus
    attr_reader :pertask_mem
    attr_accessor :job_start_time

    attr_accessor :num_pending_tasks
    attr_accessor :task_starttime_array
    attr_accessor :job_submit_time
    attr_accessor :task_machineid_array

    def initialize(id, num_tasks, task_duration, pertask_cpus, pertask_mem, submit_time)
      @job_id = id
      @num_tasks = num_tasks
      @task_duration = task_duration
      @pertask_mem = pertask_mem
      @pertask_cpus = pertask_cpus
      @num_pending_tasks = num_tasks
      @task_starttime_array = Array.new(num_tasks, nil)
      @task_machineid_array = Array.new(num_tasks, nil)
      @job_start_time = nil # hasn't been scheduled
      @job_submit_time = submit_time
    end
  end

  class Scheduler
    attr_accessor :total_jobs_handled
    attr_accessor :pending_jobsQ
    attr_accessor :running_jobs
    attr_accessor :num_finished_jobs
    attr_accessor :simulator
    attr_accessor :mode

    def initialize(mode='single')
      @total_jobs_handled = 0
      @num_finished_jobs = 0
      instansiate_pending_jobsQ()
      @running_jobs = []
      @simulator = nil
      @mode = mode
    end

    def instansiate_pending_jobsQ()
      @pending_jobsQ = Queue.new
    end
        
    def submit_job(num_tasks, task_duration, pertask_cpus, pertask_mem)
      @total_jobs_handled += 1
      job = Job.new(@total_jobs_handled, num_tasks, task_duration, pertask_cpus, pertask_mem, @simulator.curr_time)
      add_to_jobQ(job)
      schedule_pending_jobs()
    end

    def add_to_jobQ(job)
      @pending_jobsQ.enq(job)
    end
    
    def schedule_pending_jobs()
      # This is not really FIFO, to avoid head of line blocking:
      # we try to schedule as much jobs as possible looking them in 
      # the queue order and enque them back in same order.
      # This might lead to starvation of heavy jobs if cluster 
      # always has lighter jobs
      
      tmp_list = [] # jobs that couldn't be scheduled in this attempt
      while (!@pending_jobsQ.empty?)
        job = @pending_jobsQ.deq
        num_pending_tasks_before = job.num_pending_tasks
        allocate_resources(job) # allocate to all tasks if possible
        if job.num_pending_tasks != 0  # if job was not fully scheduled
          tmp_list << job
        end
        if (num_pending_tasks_before == job.num_tasks) && (job.num_pending_tasks < job.num_tasks)
          running_jobs << job # first time this job is scheduled and started
          job.job_start_time = @simulator.curr_time
        end
      end
      tmp_list.each do |jb|
        @pending_jobsQ.enq(jb)
      end
    end

    def allocate_resources(job) # mode = 'all'(all possible tasks) or 'single'(single task)
      # Here we can apply many kinds of scheduling algorithms:
      # DRF since we have heterogeous resources, or simply random first fit, best fit, etc.

      # Lets begin with applying random first fit and FIFO
      # We can also support user pool level fairness and with user pool, different scheduling policy
      if (job.pertask_cpus > (@simulator.dc_resource_pool.total_cpus - @simulator.dc_resource_pool.total_cpus_utilized)) ||
         (job.pertask_mem > (@simulator.dc_resource_pool.total_mem - @simulator.dc_resource_pool.total_cpus_utilized))
         return false
      end

      machine_ids = (0...DCResourcePool::NUM_MACHINES).to_a.shuffle!
      i = 0
      num_tasks_allocated = 0
      while((job.num_pending_tasks > 0) && (i < machine_ids.size))
        available_mem = DCResourcePool::MEMORY_PER_MACHINE - @simulator.dc_resource_pool.utilized_mem_vector[machine_ids[i]]
        available_cpus = DCResourcePool::CPUS_PER_MACHINE - @simulator.dc_resource_pool.utilized_cpus_vector[machine_ids[i]]
        
        if (job.pertask_cpus <= available_cpus) && (job.pertask_mem <= available_mem) # valid, allocate
          job.num_pending_tasks -= 1
          @simulator.dc_resource_pool.utilized_cpus_vector[machine_ids[i]] += job.pertask_cpus
          @simulator.dc_resource_pool.utilized_mem_vector[machine_ids[i]] += job.pertask_mem
          @simulator.dc_resource_pool.total_cpus_utilized += job.pertask_cpus
          @simulator.dc_resource_pool.total_memory_utilized += job.pertask_mem

          if (job.num_tasks-job.num_pending_tasks-1 < 0)
            $stderr.puts "ERROR: job.num_tasks-job.num_pending_tasks-1 = #{job.num_tasks-job.num_pending_tasks-1} < 0"
          end
          job.task_starttime_array[job.num_tasks-job.num_pending_tasks-1] = @simulator.curr_time
          job.task_machineid_array[job.num_tasks-job.num_pending_tasks-1] = machine_ids[i] # this task is schduled on this machine
          
          num_tasks_allocated += 1
          if (@mode == 'single') && (num_tasks_allocated == 1)
            return true
          end
        else
          i += 1 # can't use this machine
        end
      end
    end

  end
  
  class MyPriorityQueue < PriorityQueue
    def size
      self.length
    end
  end
  
  class DRFScheduler < Scheduler
     
    def instansiate_pending_jobsQ
      @pending_jobsQ = MyPriorityQueue.new
    end
    
    def add_to_jobQ(job)
      @pending_jobsQ.push(job, 0) # The key is amount of dominant share of the job
                                  # For new job, = 0
    end
    
    def schedule_pending_jobs()
      # We will look at the min elem in priority queue. This has the lowest
      # dominant share. Schedule one of its task, update its share and push 
      # to priority queue again. We will stop if the min elem cannot be scheduled 
      # at this time. This scheme ensures fairness and gives turns to different jobs
      # as shares of jobs are updated with each scheduled task. (This online version is
      # similar to progressive filling algo for a max-min fairness policy)
      
      while (@pending_jobsQ.length != 0)   
        elem = @pending_jobsQ.delete_min
        job = elem[0]
        num_pending_tasks_before = job.num_pending_tasks
        allocate_resources(job) # allocate to a single task for job

        if num_pending_tasks_before == job.num_pending_tasks
          # can't schedule a task, try next time
          @pending_jobsQ.push(elem[0], elem[1])
          break 
        end
        
        $stderr.puts "#{num_pending_tasks_before - job.num_pending_tasks} - #{job.num_pending_tasks} != 1" if \
        num_pending_tasks_before - job.num_pending_tasks != 1
          
        if (num_pending_tasks_before == job.num_tasks) && (job.num_pending_tasks < job.num_tasks)
          running_jobs << job # first time this job is scheduled and started
          job.job_start_time = @simulator.curr_time
        end
        
        share = get_dominant_share(job)
        @pending_jobsQ.push(job, share) if job.num_pending_tasks != 0
      end
    end
    
    def get_dominant_share(job)
      mem_utilized = (job.task_starttime_array.count{|x| x!= nil})*job.pertask_mem
      cpus_utilized = (job.task_starttime_array.count{|x| x!= nil})*job.pertask_cpus
      
      shares = [mem_utilized.to_f/@simulator.dc_resource_pool.total_memory_utilized, \
      cpus_utilized.to_f/@simulator.dc_resource_pool.total_cpus_utilized ]
      shares.max
    end
    
  end
  
  class Simulator
    attr_reader :curr_time
    attr_accessor :dc_resource_pool
    attr_accessor :scheduler

    def initialize()
      @curr_time = 0
      @dc_resource_pool = DCResourcePool.new
      @scheduler = DRFScheduler.new('single')
      @scheduler.simulator = self
    end

    def state
      puts "Time: #{@curr_time}, Total Memory Utilized: #{@dc_resource_pool.total_memory_utilized}," \
           "Total CPUs utilized: #{@dc_resource_pool.total_cpus_utilized}. \n"\
           "total_jobs: #{@scheduler.total_jobs_handled}, finished: #{@scheduler.num_finished_jobs},"\
           "pending: #{@scheduler.pending_jobsQ.size}, running_jobs: #{@scheduler.running_jobs.size}\n"\
           "print #{@dc_resource_pool.utilized_cpus_vector}\n"\
           "print #{@dc_resource_pool.utilized_mem_vector}"
    end

    def submit_job(num_tasks, task_duration, pertask_cpus, pertask_mem)
      @scheduler.submit_job(num_tasks, task_duration, pertask_cpus, pertask_mem)
    end

    def step_time(n) #step the simulation by n time units. 
      for i in (1..n)
        step
      end
    end

    def step
      @curr_time += 1
      # After a time unit, we would release the resources if 
      # already running job tasks are finished
      finished_jobs = []
      @scheduler.running_jobs.each_with_index do |job, job_idx|
        job.task_starttime_array.each_with_index do |task_start_time, tsk_idx|
          # if task was running and finished now
          if task_start_time && (@curr_time == task_start_time + job.task_duration)
            # release resources
            @dc_resource_pool.utilized_cpus_vector[job.task_machineid_array[tsk_idx]] -= job.pertask_cpus
            @dc_resource_pool.utilized_mem_vector[job.task_machineid_array[tsk_idx]] -= job.pertask_mem
            @dc_resource_pool.total_cpus_utilized -= job.pertask_cpus
            @dc_resource_pool.total_memory_utilized -= job.pertask_mem

            job.task_machineid_array[tsk_idx] = nil
            job.task_starttime_array[tsk_idx] = nil
          end
        end
        if job.num_pending_tasks == 0 && job.task_starttime_array.count{|x| x!= nil} == 0
            @scheduler.num_finished_jobs += 1
            finished_jobs << job_idx
        end
      end
      finished_jobs.each do |job_idx|
        @scheduler.running_jobs.delete_at(job_idx)
      end
      # Since resource pool state changed, see if we can schedule pending jobs now
      @scheduler.schedule_pending_jobs()
    end
  end
end
