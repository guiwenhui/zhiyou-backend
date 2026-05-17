-- 比较线程标识是否与锁中的标识一致
if (redis.call('get', KEYS[1]) == ARGV[1]) then
    -- 释放锁
    redis.call("del", KEYS[1])
    return true
end
return 0