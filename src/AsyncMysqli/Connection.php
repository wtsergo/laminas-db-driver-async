<?php

namespace Wtsergo\LaminasDbDriverAsync\AsyncMysqli;

use Laminas\Db\Adapter\Driver\Mysqli\Mysqli;
use Revolt\EventLoop;
use Laminas\Db\Adapter\Exception;

class Connection extends \Laminas\Db\Adapter\Driver\Mysqli\Connection
{
    protected PooledLink $pooledLink;
    protected \WeakReference $driverRef;

    public function __construct($connectionInfo = null)
    {
        if ($connectionInfo instanceof PooledLink) {
            $this->pooledLink = $connectionInfo;
        } else {
            throw new Exception\InvalidArgumentException(
                sprintf('$connection must be instance of %s', PooledLink::class)
            );
        }
    }
    public function getCurrentSchema()
    {
        if (! $this->isConnected()) {
            $this->connect();
        }

        //$result = $this->getResource()->query('SELECT DATABASE()');
        $result = $this->getResource()->query('SELECT DATABASE()', \MYSQLI_ASYNC);
        if ($result === false) {
            throw new Exception\RuntimeException(\mysqli_error($this->mysqli));
        }

        $suspension = EventLoop::getSuspension();

        EventLoop::onMysqli($this->getResource(), static function (string $callbackId, \mysqli $link) use ($suspension) {
            \Revolt\EventLoop::cancel($callbackId);
            $suspension->resume($link->reap_async_query());
        });

        $result = $suspension->suspend();

        if ($result === false) {
            throw new Exception\RuntimeException(\mysqli_error($this->getResource()));
        }

        $result = $result->fetch_row();

        return $result[0];
    }

    public function execute($sql)
    {
        if (! $this->isConnected()) {
            $this->connect();
        }

        if ($this->profiler) {
            $this->profiler->profilerStart($sql);
        }

        //$result = $this->getResource()->query($sql);
        $result = $this->getResource()->query($sql, \MYSQLI_ASYNC);

        if ($result === false) {
            throw new Exception\RuntimeException(\mysqli_error($this->getResource()));
        }

        $suspension = EventLoop::getSuspension();

        EventLoop::onMysqli($this->getResource(), static function (string $callbackId, \mysqli $link) use ($suspension) {
            \Revolt\EventLoop::cancel($callbackId);
            $suspension->resume($link->reap_async_query());
        });

        $result = $suspension->suspend();

        if ($result === false) {
            throw new Exception\RuntimeException(\mysqli_error($this->getResource()));
        }

        if ($this->profiler) {
            $this->profiler->profilerFinish($sql);
        }

        return $this->getDriver()->createResult($result === true ? $this->getResource() : $result);
    }

    public function getResource($pooled=false)
    {
        return $pooled ? $this->pooledLink : $this->pooledLink->link;
    }

    public function setResource(\mysqli $resource)
    {
        throw new Exception\RuntimeException(__METHOD__.' not supported');
    }

    public function beginTransaction()
    {
        if (! $this->isConnected()) {
            $this->connect();
        }

        $this->getResource()->autocommit(false);
        $this->inTransaction = true;

        return $this;
    }

    public function connect()
    {
        throw new Exception\RuntimeException(
            __METHOD__.' not supported. Connection should be provided from outside'
        );
    }

    public function commit()
    {
        if (! $this->isConnected()) {
            $this->connect();
        }

        $this->getResource()->commit();
        $this->inTransaction = false;
        $this->getResource()->autocommit(true);

        return $this;
    }

    public function getLastGeneratedValue($name = null)
    {
        return $this->getResource()->insert_id;
    }

    public function isConnected()
    {
        return $this->getResource() instanceof \mysqli;
    }

    /**
     * {@inheritDoc}
     */
    public function disconnect()
    {
        throw new Exception\RuntimeException(__METHOD__.' not supported');
    }

    public function setDriver(Mysqli $driver)
    {
        $this->driverRef = \WeakReference::create($driver);

        return $this;
    }

    private function getDriver()
    {
        return $this->driverRef->get();
    }
}
