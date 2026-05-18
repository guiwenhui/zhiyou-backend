package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author Kyle
 * @since 2022-10-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private ISeckillVoucherService seckillVoucherService;

    @Autowired
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private IVoucherOrderService proxy;


    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        // 确保 stream 和消费者组存在，防止 handler 启动时报 pending-list 异常
        try {
            // 先用 MKSTREAM 确保 stream 存在，再创建消费者组
            stringRedisTemplate.opsForStream().createGroup("stream.orders", ReadOffset.from("0"), "g1");
            log.info("Redis Stream 消费者组 g1 创建成功");
        } catch (Exception e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("BUSYGROUP")) {
                // 消费者组已存在，正常情况
                log.debug("消费者组 g1 已存在，跳过创建");
            } else {
                log.warn("Stream 初始化异常，将在 handler 中重试: {}", msg);
            }
        }
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";
        // 连续失败计数，用于指数退避
        private int consecutiveErrors = 0;
        private static final int MAX_BACKOFF_SECONDS = 60;

        @Override
        public void run() {
            while (true) {
                try {
                    //1. 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 读取成功，重置错误计数
                    consecutiveErrors = 0;
                    // 判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        // 否，进入下一次循环
                        continue;
                    }
                    // 解析消息队列里的消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 有，创建订单
                    handleVoucherOrder(voucherOrder);
                    // ACK确认 SACK stream.orders g1 *
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1" , record.getId());
                } catch (Exception e) {
                    consecutiveErrors++;
                    // 只在第一次和每隔10次打印完整错误，避免日志爆炸
                    if (consecutiveErrors == 1 || consecutiveErrors % 10 == 0) {
                        log.error("订单处理异常 (连续第{}次)", consecutiveErrors, e);
                    }
                    handlePendingList();
                    // 指数退避：2^n 秒，上限60秒
                    int backoffSeconds = Math.min(1 << Math.min(consecutiveErrors, 6), MAX_BACKOFF_SECONDS);
                    try {
                        Thread.sleep(backoffSeconds * 1000L);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }

        private void handlePendingList() {
            int retryCount = 0;
            final int MAX_RETRIES = 3;
            while (true) {
                try {
                    //1. 获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0")));
                    //2. 判断pending-list中是否有未处理消息
                    if (records == null || records.isEmpty()) {
                        //如果没有就说明没有异常消息，直接结束循环
                        break;
                    }
                    //3. 消息获取成功之后，我们需要将其转为对象
                    MapRecord<String, Object, Object> record = records.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //4. 获取成功，执行下单逻辑，将数据保存到数据库中
                    handleVoucherOrder(voucherOrder);
                    //5. 手动ACK，SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                    // 处理成功，重置重试计数
                    retryCount = 0;
                } catch (Exception e) {
                    retryCount++;
                    if (retryCount >= MAX_RETRIES) {
                        log.warn("处理pending-list连续失败{}次，退出重试", retryCount);
                        break;
                    }
                    log.info("处理pending-list异常，第{}次重试", retryCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
    }
//    private final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
//    private class VoucherOrderHandler implements Runnable {
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    //1. 获取队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    //2. 创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("订单处理异常", e);
//                }
//            }
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //1. 获取用户
        Long userId = voucherOrder.getUserId();
        //2. 创建锁对象，作为兜底方案
        RLock redisLock = redissonClient.getLock("order:" + userId);
        //3. 获取锁
        boolean isLock = redisLock.tryLock();
        //4. 判断是否获取锁成功
        if (!isLock) {
            log.error("不允许重复下单!");
            return;
        }
        try {
            //5. 使用代理对象，由于这里是另外一个线程，
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            redisLock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 获取订单id
        long orderId = redisIdWorker.nextId("order");
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                UserHolder.getUser().getId().toString(),
                String.valueOf(orderId)
        );
        if (result.intValue() != 0) {
            return Result.fail(result.intValue() == 1 ? "库存不足" : "不能重复下单");
        }
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }

//    public Result seckillVoucher(Long voucherId) {
//        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
//                Collections.emptyList(), voucherId.toString(),
//                UserHolder.getUser().getId().toString());
//        if (result.intValue() != 0) {
//            return Result.fail(result.intValue() == 1 ? "库存不足" : "不能重复下单");
//        }
//        long orderId = redisIdWorker.nextId("order");
//        //封装到voucherOrder中
//        VoucherOrder voucherOrder = new VoucherOrder();
//        voucherOrder.setVoucherId(voucherId);
//        voucherOrder.setUserId(UserHolder.getUser().getId());
//        voucherOrder.setId(orderId);
//        //加入到阻塞队列
//        orderTasks.add(voucherOrder);
//        //主线程获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        return Result.ok(orderId);
//    }


    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 一人一单逻辑
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        synchronized (userId.toString().intern()) {
            Long count = query().eq("voucher_id", voucherId).eq("user_id", userId).count();
            if (count > 0) {
                log.error("你已经抢过优惠券了哦");
                return;
            }
            //5. 扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId)
                    .gt("stock", 0)
                    .update();
            if (!success) {
                log.error("库存不足");
            }
            //7. 将订单数据保存到表中
            save(voucherOrder);
        }
    }
}