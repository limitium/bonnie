require './bonnie/transit_worker'
require 'thread'
class ParallelWorker < TransitWorker

  def declareAll(channel)
    @channel = channel
    super channel
  end

  def parallel(response, items, iteration_queue)

    synchronize_queue=@channel.queue 'synchronize.'+Digest::MD5.hexdigest(Time.now.to_s), :exclusive => false, :auto_delete => true, :durable => false

    items.each do |item|
      send_to item, :routing_key => iteration_queue, :reply_to => synchronize_queue.name
    end

    response[:size] = items.size
    response[:synchronize_to] = synchronize_queue.name
  end
end