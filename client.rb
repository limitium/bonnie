require 'rubygems'
require 'amqp'
require 'json'

class Client
  def initialize(url, exchange_name)
    @url = url
    @exchange_name = exchange_name
  end

  def send(msg)
    EventMachine.run do
      Signal.trap('INT') { EventMachine.stop }
      Signal.trap('TERM') { EventMachine.stop }
      connection = AMQP.connect @url
      channel = AMQP::Channel.new connection
      channel.on_error do |ch, channel_close|
        puts 'Channel-level error: ' + channel_close.reply_text + ', shutting down...'
        connection.close { EventMachine.stop }
      end
      exchange = channel.direct @exchange_name, :auto_delete => false, :durable => true
      exchange.publish JSON.generate(msg) do
        connection.close { EventMachine.stop }
      end
    end
  end
end