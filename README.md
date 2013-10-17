```
$stdout.sync = true

require 'rubygems'
require 'bonnie/reply_worker'

worker = ReplyWorker.new 'amqp://log:pass@host/vhost'

worker.frontend 'exchange_name', 'queue.name'


worker.work do |delivery_info, properties, in_data|
  {:worker_result=> in_data + 1 }
end
