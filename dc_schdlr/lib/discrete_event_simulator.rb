require 'priority_queue'

module DcSchdlr
  class DESimulator
    attr_reader :curr_time
    attr_reader :action_Q
    
    def initialize()
      @curr_time = 0
      @action_Q = PriorityQueue.new
      after_delay(0) do
        puts "Starting Simulation. Time = #{@curr_time}"
      end
    end
    
    def after_delay(delay, &action)
      @action_Q.push(action, @curr_time + delay)
      # We want to execute actions scheduled now to be able to
      # interactively see the state as we submit job
      # Basically, we can also execute action.call right now if
      # delay = 0. But following statement takes care of it.
      step(0)
    end
    
    def next_action
      elem = @action_Q.delete_min
      @curr_time = elem[1]
      elem[0].call
    end
    
    def step(time) # step this much time from now
      target_time = @curr_time + time
      while(@action_Q.length != 0 && @action_Q.min[1] <= target_time)
        next_action
      end
      
      # if there are no actions scheduled at target_time,
      # then next() will set the curr_time to be the last event's
      # time. So, set curr_time explicitly here
      @curr_time = target_time
    end
  end
end