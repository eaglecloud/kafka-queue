<?php

namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class KafkaQueue extends Queue implements QueueContract
{
    protected $producer, $consumer;

    public function __construct($producer, $consumer)
    {
        $this->producer = $producer;
        $this->consumer = $consumer;
    }

    public function size($queue = null)
    {
        // TODO: Implement size() method.
    }

    public function push($job, $data = '', $queue = null)
    {
        $topic = $this->producer->newTopic($queue ?? env('KAFKA_QUEUE'));
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(1000);
//        $this->producer->poll(0);
//        while ($this->producer->getOutQLen() > 0) {
//            $this->producer->poll(50);
//        }
//        echo "send success" . PHP_EOL;
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // TODO: Implement pushRaw() method.
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        // TODO: Implement later() method.
    }

    public function pop($queue = null)
    {
        $this->consumer->subscribe([$queue]);

        try {
            $message = $this->consumer->consume(30 * 1000);
            match ($message->err) {
                RD_KAFKA_RESP_ERR_NO_ERROR => unserialize($message->payload)->handle(),
                RD_KAFKA_RESP_ERR__PARTITION_EOF => var_dump("No more messages; will wait for more\n"),
                RD_KAFKA_RESP_ERR__TIMED_OUT => var_dump("Timed out\n"),
                default => throw new \Exception($message->errstr(), $message->err),
            };
        } catch (\Exception $e) {
            var_dump($e->getMessage());
        }
    }
}
