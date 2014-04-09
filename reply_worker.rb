require './bonnie/worker'
require 'json'
class ReplyWorker < Worker

  def initialize(url)
    super url
  end

  def declareAll(channel)
    @default_exchange = channel.default_exchange
    super channel
  end

  def on_message(metadata, json, block)
    result = block.call metadata, json
    send_to result, :routing_key => metadata.reply_to, :correlation_id => metadata.correlation_id, :expiration => 1000000
  end

  def send_to(result, params)
    @default_exchange.publish JSON.generate(result), params
  end

end