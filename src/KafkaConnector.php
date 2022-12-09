<?php

namespace Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{
    public function connect(array $config)
    {
        return new KafkaQueue($this->producer($config), $this->consumer($config));
    }

    protected function producer(array $config)
    {
        $conf = $this->getConf($config);
        $conf->set('acks', $config['acks']);
        $conf->set('retries', $config['retries']);
        $conf->set('retry.backoff.ms', $config['retry_backoff_ms']);
        $conf->set('socket.timeout.ms', $config['socket_timeout_ms']);
        $conf->set('reconnect.backoff.max.ms', $config['reconnect_backoff_max_ms']);
        // 注册发送消息的回调
//        $conf->setDrMsgCb(function ($kafka, $message) {
//            echo '【Producer】send：message=' . var_export($message, true) . "\n";
//        });
        // 注册发送消息错误的回调
//        $conf->setErrorCb(function ($kafka, $err, $reason) {
//            echo "【Producer】send error：err=$err reason=$reason \n";
//        });

        return new \RdKafka\Producer($conf);
    }

    protected function consumer(array $config)
    {
        $conf = $this->getConf($config);
        $conf->set('session.timeout.ms', $config['session_timeout_ms']);
        $conf->set('reconnect.backoff.max.ms', $config['reconnect_backoff_max_ms']);
        $conf->set('group.id', $config['group_id']);

        return new \RdKafka\KafkaConsumer($conf);
    }

    protected function getConf(array $config)
    {
        $conf = new \RdKafka\Conf();

        $conf->set('metadata.broker.list', $config['bootstrap_servers']);
        $conf->set('sasl.mechanisms', $config['sasl_mechanisms']);
        $conf->set('api.version.request', 'true');
        $conf->set('sasl.username', $config['sasl_username']);
        $conf->set('sasl.password', $config['sasl_password']);
        $conf->set('security.protocol', $config['security_protocol']);
        $conf->set('ssl.ca.location', __DIR__.'/mix-4096-ca-cert');
        $conf->set('ssl.ca.location', resource_path().'/kafka/mix-4096-ca-cert');

        return $conf;
    }
}
