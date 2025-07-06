<?php

namespace Wtsergo\LaminasDbDriverAsync;

use Laminas\Db\Adapter\Adapter;

interface ConnectionPool
{
    /**
     * @return Adapter
     */
    public function createConnection(): Adapter;
}
