require './bonnie/reply_worker'
class TransitWorker < ReplyWorker
  def backend(queue_name)
    @backend_queue_name = queue_name
  end

  def on_message(metadata, json, block)
    result = block.call metadata, json
    send_to result, :routing_key => @backend_queue_name, :reply_to => metadata.reply_to, :correlation_id => metadata.correlation_id, :persistent => true
  end
end