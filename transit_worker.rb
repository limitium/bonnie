require './bonnie/reply_worker'
class TransitWorker < ReplyWorker
  def backend(queue_name)
    @backend_queue_name = queue_name
  end

  def on_message(block, msg, delivery_info, properties)
    result = block.call delivery_info, properties, msg
    send_to result, :routing_key => @backend_queue_name, :reply_to => properties.reply_to, :correlation_id => properties.correlation_id, :persistent => true
  end
end