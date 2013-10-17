require './bonnie/worker'
require 'json'
class ReplyWorker < Worker

  def initialize(url)
    super url
    @default_exchange = @channel.default_exchange
  end

  def on_message(block, body, delivery_info, properties)
    result = block.call delivery_info, properties, body
    send_to result, :routing_key => properties.reply_to, :correlation_id => properties.correlation_id, :expiration => 1000
  end

  def send_to(result, params)
    @default_exchange.publish JSON.generate(result), params
  end

end