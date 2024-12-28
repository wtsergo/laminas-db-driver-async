<?php

namespace Wtsergo\LaminasDbDriverAsync\AsyncMysqli;

class PooledLink
{
    /**
     * @param \Closure(\mysqli):void $push Closure to push the mysqli link back into the queue.
     */
    public function __construct(
        public readonly \mysqli  $link,
        private readonly \Closure $push
    ) {
    }

    /**
     * Automatically pushes the mysqli link back into the queue.
     */
    public function __destruct()
    {
        ($this->push)($this->link);
    }

    public function __call($name, $args): mixed
    {
        return call_user_func_array([$this->link, $name], $args);
    }

}
