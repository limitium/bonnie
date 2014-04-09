require './bonnie/transit_worker'
require 'json'
class SynchronizeWorker < TransitWorker

  def declareAll(channel)
    @channel = channel
    super channel
  end

  def proceed_message(block, json, metadata)
    @metadata = metadata
    block.call metadata, json
  end

  def synchronize (synchronize_data, &block)
    connection = AMQP.connect(@url, :heartbeat => 30)
    channel = AMQP::Channel.new(connection, :prefetch => 1)
    synchronize_queue= channel.queue synchronize_data['synchronize_to'], :exclusive => false, :auto_delete => true, :durable => false

    synchronize_data[:error] = 0
    synchronize_data['items']=[]
    synchronize_queue.subscribe do |metadata, payload|
      begin
        iteration_data = JSON.parse(payload)
        synchronize_data['items'].push block.call iteration_data
        if iteration_data['error'] != 0
          synchronize_data[:error] = 3
        end
      rescue
        synchronize_data[:error] = 2
      end
      synchronize_data['size'] -= 1
      if synchronize_data['size'] == 0
        synchronize_queue.delete
        puts '. sdone '+synchronize_data['project_id'].to_s
        send_to synchronize_data, :routing_key => @backend_queue_name, :reply_to => @metadata.reply_to, :correlation_id => @metadata.correlation_id, :persistent => true
        @metadata.ack
      end
    end
  end
end