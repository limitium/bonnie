require 'rubygems'
require 'bunny'
require 'json'

class Worker
  def initialize(url)
    bunny = Bunny.new url
    bunny.start
    @channel = bunny.create_channel
    @channel.prefetch 1
  end

  def frontend(exchange_name, queue_name, routing_key = nil)
    @frontend_exchange = @channel.direct exchange_name, :auto_delete => false, :durable => true
    @frontend_queue = @channel.queue queue_name, :exclusive => false, :auto_delete => false, :durable => true

    @frontend_queue.bind @frontend_exchange, :routing_key => routing_key
  end

  def work(&block)
    @frontend_queue.subscribe :block => true, :manual_ack => true do |delivery_info, properties, msg|
      begin
        json = JSON.parse(msg)
      rescue
        json = {'error' => 'json_decode'}
      ensure
        on_message(block, json, delivery_info, properties)
        @channel.ack delivery_info.delivery_tag
      end
    end
  end

  def on_message(block, body, delivery_info, properties)
    block.call delivery_info, properties, body
  end
end