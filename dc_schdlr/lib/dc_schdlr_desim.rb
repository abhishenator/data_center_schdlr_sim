# Basically implementation of dc_schdlr.rb using
# Discrete event simulator

require "dc_schdlr/version"
require 'dc_schdlr'
require 'discrete_event_simulator'
require "priority_queue"

module DcSchdlr
  class Scheduler
    def release_resources(machine_id, job, tsk_idx)
      @simulator.dc_resource_pool.utilized_mem_vector[machine_id] -= job.pertask_mem
      @simulator.dc_resource_pool.utilized_cpus_vector[machine_id] -= job.pertask_cpus
      @simulator.dc_resource_pool.total_cpus_utilized -= job.pertask_cpus
      @simulator.dc_resource_pool.total_memory_utilized -= job.pertask_mem
      job.task_machineid_array[tsk_idx] = nil
      job.task_starttime_array[tsk_idx] = nil
      if job.num_pending_tasks == 0 && job.task_starttime_array.count{|x| x!= nil} == 0
        @num_finished_jobs += 1
        job_idx_to_rm = nil
        @running_jobs.each_with_index do |jb,i|
          if jb.job_id == job.job_id
            job_idx_to_rm = i
            break
          end
        end
        @running_jobs.delete_at(job_idx_to_rm)
      end
      # Since resource pool state changed, see if we can schedule pending jobs now
      @simulator.after_delay(0) do 
        schedule_pending_jobs()
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
          tsk_idx = job.num_tasks-job.num_pending_tasks-1
          job.task_starttime_array[tsk_idx] = @simulator.curr_time
          job.task_machineid_array[tsk_idx] = machine_ids[i] # this task is schduled on this machine
          
          num_tasks_allocated += 1
          @simulator.after_delay(job.task_duration) do
            release_resources(machine_ids[i], job, tsk_idx)
          end
          
          if (@mode == 'single') && (num_tasks_allocated == 1)  
            return true
          end
        else
          i += 1 # can't use this machine
        end
      end
    end    
  end
  
  class MySimulator < DESimulator
    attr_accessor :dc_resource_pool
    attr_accessor :scheduler

    def initialize()
      super
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
      after_delay(0) do
        @scheduler.submit_job(num_tasks, task_duration, pertask_cpus, pertask_mem)
      end
    end
  end
end
