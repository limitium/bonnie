require 'rubygems'
require 'amqp'
require 'json'

class Worker
  def initialize(url)
    @url=url
  end

  def frontend(exchange_name, queue_name, routing_key = nil)
    @exchange_name = exchange_name
    @queue_name = queue_name
    @routing_key = routing_key
  end

  def declareAll(channel)
    frontend_exchange = channel.direct @exchange_name, :auto_delete => false, :durable => true
    frontend_queue = channel.queue @queue_name, :exclusive => false, :auto_delete => false, :durable => true

    frontend_queue.bind frontend_exchange, :routing_key => @routing_key
  end

  def subscribe(frontend_queue, block)
    frontend_queue.subscribe :ack => true do |metadata, payload|
      begin
        json = JSON.parse(payload)
      rescue
        json = {'error' => 'json_decode'}
      ensure
        proceed_message(block, json, metadata)
      end
    end
  end

  def proceed_message(block, json, metadata)
    on_message metadata, json, block
    metadata.ack
  end

  def work(&block)
    EventMachine.run do
      Signal.trap('INT') { EventMachine.stop }
      Signal.trap('TERM') { EventMachine.stop }

      connection = AMQP.connect(@url, :heartbeat => 30)
      connection.on_error do |ch, connection_close|
        puts '[connection failure] ...'
        raise connection_close.reply_text
      end
      connection.after_recovery do |conn, settings|
        subscribe(declareAll(conn.channel), block)
      end

      connection.on_tcp_connection_loss do |conn, settings|
        puts '[network failure] Trying to reconnect...'
        conn.reconnect
      end

      channel = AMQP::Channel.new(connection, :prefetch => 1)
      channel.on_error do |ch, channel_close|
        puts '[channel failure] ...'
        raise channel_close.reply_text
      end

      subscribe(declareAll(channel), block)
    end
  end

  def on_message(metadata, json, block)
    block.call metadata, json
  end
end