<?php
/**
 * Wrapper for the PHP AMQP Lib as a means to abstract any protocol-specific methods
 * from all outer classes that need to work with those.
 *
 * User: emiliano
 * Date: 30/12/20
 * Time: 14:37
 */

namespace {{ params.packageName }}\Infrastructure;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use Ramsey\Uuid\Uuid;
use {{ params.packageName }}\Messages\MessageContract;
use {{ params.packageName }}\Handlers\AMQPRPCServerHandler;
use {{ params.packageName }}\Handlers\AMQPRPCClientHandler;
use {{ params.packageName }}\Handlers\HandlerContract;
use {{ params.packageName }}\Common\FactoryContract;

class AMQPBrokerClient implements BrokerClientContract
{
    /** @var AMQPStreamConnection $connection */
    private $connection;
    /** @var AMQPChannel $channel */
    private $channel;
    /** @var FactoryContract $factory */
    private $factory;

    /**
     * AMQPBrokerClient constructor.
     * @param AMQPStreamConnection|null $connection
     * @param FactoryContract|null $factory
     */
    public function __construct(
        AMQPStreamConnection $connection = null,
        FactoryContract $factory = null
    ) {
        $this->setConnection($connection);
        if (!is_null($factory)) {
            $this->setFactory($factory);
        }
    }

    /**
     * @param AMQPStreamConnection $connection
     * @return BrokerClientContract
     */
    public function setConnection($connection): BrokerClientContract
    {
        $this->connection = $connection;
        return $this;
    }

    /**
     * @return AMQPStreamConnection
     */
    public function getConnection(): AMQPStreamConnection
    {
        return $this->connection;
    }

    /**
     * @return AMQPChannel
     */
    public function connect()
    {
        $this->channel = $this->connection->channel();
        return $this->channel;
    }

    /**
     * @throws Exception
     */
    public function close()
    {
        $this->channel->close();
        $this->connection->close();
    }

    /**
     * @param FactoryContract $factory
     * @return BrokerClientContract
     */
    public function setFactory(FactoryContract $factory): BrokerClientContract
    {
        $this->factory = $factory;
        return $this;
    }

    /**
     * @return FactoryContract
     */
    public function getFactory(): FactoryContract
    {
        return $this->factory;
    }

    /**
     * @param MessageContract $message
     * @param array $config
     * @return bool
     * @throws Exception
     */
    public function basicPublish(MessageContract $message, array $config = []): bool
    {
        try {
            // Explicit assignments instead of extract()
            $exchangeName = $config['exchangeName'] ?? '';
            $exchangeType = $config['exchangeType'] ?? '';
            $bindingKey   = $config['bindingKey'] ?? '';
            $passive      = $config['passive'] ?? false;
            $durable      = $config['durable'] ?? false;
            $autoDelete   = $config['autoDelete'] ?? true;
            $internal     = $config['internal'] ?? false;
            $noWait       = $config['noWait'] ?? false;
            $arguments    = $config['arguments'] ?? [];
            $ticket       = $config['ticket'] ?? null;
            $mandatory    = $config['mandatory'] ?? false;
            $immediate    = $config['immediate'] ?? false;

            $this->connect();
            $this->channel->exchange_declare(
                $exchangeName,
                $exchangeType,
                $passive,
                $durable,
                $autoDelete,
                $internal,
                $noWait,
                $arguments,
                $ticket
            );
            /** @var AMQPMessage $amqpMessage */
            $amqpMessage = $message->getPayload();
            $this->channel->basic_publish(
                $amqpMessage,
                $exchangeName,
                $bindingKey,
                $mandatory,
                $immediate,
                $ticket
            );
            $this->close();
            return true;
        } catch (\Throwable $t) {
            // Log the error if needed
            return false;
        }
    }

    /**
     * Basic consume function will default to topic through exchange with binding keys.
     *
     * @param HandlerContract $handler
     * @param array $config
     * @return bool
     * @throws ErrorException
     */
    public function basicConsume(HandlerContract $handler, array $config = []): bool
    {
        try {
            // Explicit assignments instead of extract()
            $exchangeName    = $config['exchangeName'] ?? '';
            $exchangeType    = $config['exchangeType'] ?? '';
            $bindingKey      = $config['bindingKey'] ?? '';
            $exchangeDurable = $config['exchangeDurable'] ?? false;
            $queueDurable    = $config['queueDurable'] ?? false;
            $autoDelete      = $config['autoDelete'] ?? true;
            $noWait          = $config['noWait'] ?? false;
            $arguments       = $config['arguments'] ?? [];
            $ticket          = $config['ticket'] ?? null;
            $publisherTag    = $config['publisherTag'] ?? '';
            $noLocal         = $config['noLocal'] ?? false;
            $noAck           = $config['noAck'] ?? false;
            $exclusive       = $config['exclusive'] ?? false;

            $this->connect();
            $this->channel->exchange_declare(
                $exchangeName,
                $exchangeType,
                false,
                $exchangeDurable,
                $autoDelete,
                false,
                $noWait,
                $arguments,
                $ticket
            );
            list($queueName) = $this->channel->queue_declare(
                "",
                false,
                $queueDurable,
                $autoDelete,
                false
            );
            $this->channel->queue_bind($queueName, $exchangeName, $bindingKey);
            $this->channel->basic_consume(
                $queueName,
                $publisherTag,
                $noLocal,
                $noAck,
                $exclusive,
                $noWait,
                [$handler, 'handle'],
                $ticket,
                $arguments
            );

            while ($this->channel->is_consuming()) {
                $this->channel->wait();
            }

            $this->close();

            return true;
        } catch (\Throwable $t) {
            // Log error if needed
            return false;
        }
    }

    /**
     * @param MessageContract $message
     * @param array $config
     * @return AMQPMessage
     * @throws ErrorException
     */
    public function rpcPublish(MessageContract $message, array $config = []): AMQPMessage
    {
        // Explicit assignment instead of extract()
        $bindingKey = $config['bindingKey'] ?? '';

        $this->connect();
        /** @var AMQPRPCClientHandler $rpcClientHandler */
        $rpcClientHandler = $this->getFactory()->createHandler(AMQPRPCClientHandler::class);

        list($queue) = $this->channel->queue_declare(
            "",
            false,
            false,
            true,
            false
        );

        /** @var AMQPMessage $amqpMessage */
        $amqpMessage = $message->getPayload();
        $amqpMessage->set('reply_to', $queue);
        if (!$amqpMessage->has('correlation_id')) {
            $correlationId = Uuid::uuid4()->toString();
            $amqpMessage->set('correlation_id', $correlationId);
        }
        $rpcClientHandler->setCorrelationId($amqpMessage->get('correlation_id'));

        $this->channel->basic_consume(
            $queue,
            '',
            false,
            true,
            false,
            false,
            [$rpcClientHandler, 'handle']
        );

        $this->channel->basic_publish($amqpMessage, '', $bindingKey);

        while (!$amqpRPCMessage = $rpcClientHandler->getMessage()) {
            $this->channel->wait();
        }

        $this->close();

        return $amqpRPCMessage;
    }

    /**
     * @param AMQPRPCServerHandler $handler
     * @param array $config
     * @return bool
     */
    public function rpcConsume(AMQPRPCServerHandler $handler, array $config = []): bool
    {
        try {
            // Explicit assignment instead of extract()
            $queueName = $config['queueName'] ?? '';

            $this->connect();
            $this->channel->queue_declare(
                $queueName,
                false,
                false,
                false,
                false
            );

            $this->channel->basic_qos(null, 1, null);
            $this->channel->basic_consume(
                $queueName,
                '',
                false,
                false,
                false,
                false,
                [$handler, 'handle']
            );

            while ($this->channel->is_consuming()) {
                $this->channel->wait();
            }

            $this->close();

            return true;
        } catch (\Throwable $t) {
            // Log error if needed
            return false;
        }
    }
}
