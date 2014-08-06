# Two level scheduling:
# 1) user pool: DRF user pool level fairness
# 2) User specific scheduler within user pool

# Basically extening definitions in dc_schdlr.rb
# to have user pool, 2-level scheduling

require 'dc_schdlr'

module DcSchdlr
  class UserJobScheduler < Scheduler
    def submit_job(job)
      @total_jobs_handled += 1
      add_to_jobQ(job)
    end
    
    def schedule_pending_jobs() #just schedule one task of one job
      tmp_list = [] # jobs that couldn't be scheduled in this attempt
      while (!@pending_jobsQ.empty?)
        job = @pending_jobsQ.deq
        num_pending_tasks_before = job.num_pending_tasks
        allocate_resources(job)
        if job.num_pending_tasks != 0  # if job was not fully scheduled
          tmp_list << job
        end
        if (num_pending_tasks_before == job.num_tasks) && (job.num_pending_tasks < job.num_tasks)
          running_jobs << job # first time this job is scheduled and started
          job.job_start_time = @simulator.curr_time
        end
        
        # successfully scheduled a task
        if (num_pending_tasks_before > job.num_pending_tasks)
          tmp_list.each do |jb|
            @pending_jobsQ.enq(jb)
          end
          return true
        end
      end
      
      # couldn't schedule a single task
      tmp_list.each do |jb|
        @pending_jobsQ.enq(jb)
      end
      return false
    end
        
    def get_my_dominant_share
      tmp = []
      mem_utilized = 0
      cpus_utilized = 0
      while (!@pending_jobsQ.empty?)
        job = @pending_jobsQ.deq
        num_running_job_tasks = job.task_starttime_array.count{|x| x!= nil}
        mem_utilized += num_running_job_tasks*job.pertask_mem
        cpus_utilized += num_running_job_tasks*job.pertask_cpus
        tmp << job  
      end
      
      tmp.each do |job|
        @pending_jobsQ.enq(job)
      end
      
      shares = [mem_utilized.to_f/@simulator.dc_resource_pool.total_memory_utilized, \
      cpus_utilized.to_f/@simulator.dc_resource_pool.total_cpus_utilized ]
      shares.max # return dominant share
    end
  end
  
  # This class handles scheduling of users based on 
  # their fair share, using DRF. Two level scheduling,
  # scheduling of user pool and then scheduling within a user
  # pool with user's choice schdeuler
  class UserScheduler < DRFScheduler
    # Each user can have its own scheduling policy for
    # its job. So, we give each user its own scheduler
    attr_accessor :user_scheduler_map
    attr_accessor :userQ
    attr_accessor :num_finished_jobs
    attr_accessor :num_running_jobs
    attr_accessor :num_pending_jobs
    
    def initialize()
      @total_jobs_handled = 0
      @num_finished_jobs = 0
      @num_running_jobs = 0
      @num_pending_jobs = 0
      @simulator = nil # registered simulator
      @userQ = PriorityQueue.new()
      @user_scheduler_map = {}     
    end
    
    def submit_job(userid, num_tasks, task_duration, pertask_cpus, pertask_mem)
      @total_jobs_handled += 1
      job = Job.new(@total_jobs_handled, num_tasks, task_duration, pertask_cpus, pertask_mem, @simulator.curr_time)
      
      # If this is the first job submitted by user, create a user scheduler
      # Add this job to the user scheduler
      if !user_scheduler_map.has_key?(userid)
        scheduler = UserJobScheduler.new
        # register the simulator for this scheduler
        scheduler.simulator = @simulator
        user_scheduler_map["#{userid}"] = scheduler
      end
      
      user_scheduler_map["#{userid}"].submit_job(job) # This doesn't
        # schedule the job, only submits to user's scheduler
      add_to_UserQ(userid)
      schedule_pending_jobs()
    end
    
    def add_to_UserQ(user)
      @userQ.push(user, 0) # The key is amount of dominant share of the user
                           # For new job, = 0
    end
        
    def schedule_pending_jobs
      # We first select the user based on their DR share
      while (@userQ.length != 0)
        elem = @userQ.delete_min
        userid = elem[0]
        user_scheduler = @user_scheduler_map[userid]
        if !user_scheduler.schedule_pending_jobs()
          # can't schedule a user task, try next time
          # Also, user's share didn't change
          @userQ.push(elem[0], elem[1])
          break 
        end
        user_new_share = user_scheduler.get_my_dominant_share
        # no need to push if user doesn't have any pending jobs,
        # ie we just scheduled user's last pending task
        @userQ.push(userid, user_new_share) if user_scheduler.pending_jobsQ.size != 0
      end
      
      # After every scheduling event , update the aggregate job counters
      @num_finished_jobs = 0
      @num_running_jobs = 0
      @num_pending_jobs = 0
      @user_scheduler_map.each do |user,schdlr|
        @num_finished_jobs += schdlr.num_finished_jobs
        @num_running_jobs += schdlr.running_jobs.size
        @num_pending_jobs += schdlr.pending_jobsQ.size
      end
    end
  end
  
  class UserSchldrSimulator < Simulator
    def initialize()
      @curr_time = 0
      @dc_resource_pool = DCResourcePool.new
      @scheduler = UserScheduler.new
      @scheduler.simulator = self
    end

    def state      
      puts "Time: #{@curr_time}, Total Memory Utilized: #{@dc_resource_pool.total_memory_utilized}," \
           "Total CPUs utilized: #{@dc_resource_pool.total_cpus_utilized}. \n"\
           "total_jobs: #{@scheduler.total_jobs_handled}, finished: #{@scheduler.num_finished_jobs},"\
           "pending(whose even a single task hasn't been scheduled): #{@scheduler.num_pending_jobs}, running_jobs: #{@scheduler.num_running_jobs},"\
           " UserSchedulerQ length = #{@scheduler.userQ.length}\n"\
           "utilized_cpus_vector #{@dc_resource_pool.utilized_cpus_vector}\n"\
           "utilized_mem_vector #{@dc_resource_pool.utilized_mem_vector}"
           
      @scheduler.user_scheduler_map.each do |user,schdlr|
        puts "\nUSER: #{user}:"
        puts "Time: #{@curr_time}, "\
                   "total_jobs: #{schdlr.total_jobs_handled}, finished: #{schdlr.num_finished_jobs},"\
                   "pending: #{schdlr.pending_jobsQ.size}, running_jobs: #{schdlr.running_jobs.size}\n"
      end
    end

    def submit_job(userid, num_tasks, task_duration, pertask_cpus, pertask_mem)
      @scheduler.submit_job(userid, num_tasks, task_duration, pertask_cpus, pertask_mem)
    end

    def step_time(n) #step the simulation by n time units. 
      for i in (1..n)
        @curr_time += 1
        @scheduler.user_scheduler_map.each do |user,schdlr|
          step(schdlr)
        end
        @scheduler.schedule_pending_jobs()
      end
    end
    
    def step(schdlr)
      # This just releases the resources held by schdlr's running 
      # tasks if they finished.
      # Also, updates relevant counters

      # After a time unit, we would release the resources if 
      # already running job tasks are finished
      finished_jobs = []
      schdlr.running_jobs.each_with_index do |job, job_idx|
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
        
        # If no pending and running tasks, the whole job finished now
        if job.num_pending_tasks == 0 && job.task_starttime_array.count{|x| x!= nil} == 0
          schdlr.num_finished_jobs += 1
          finished_jobs << job_idx
        end
      end
      # remove the job from the running jobs list
      finished_jobs.each do |job_idx|
        schdlr.running_jobs.delete_at(job_idx)
      end
    end
  end
end
