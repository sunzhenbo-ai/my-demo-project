package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setResultType(Long.class);
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
    }

    //创建阻塞队列
    //private BlockingDeque<VoucherOrder> orderTasks = new LinkedBlockingDeque<>(1024 * 1024);
    //创建线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //该注解用于使该方法在类加载时遍执行
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    //消息队列实现
    private class VoucherOrderHandler implements Runnable {
        private String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    //1获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2判断获取是否成功
                    if(list == null || list.isEmpty()){
                        //2.1如果失败说明没有信息，开启下一次获取
                        continue;
                    }
                    //3 解析消息中的订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //4如果成功则开始下单
                    handleVoucherOrder(voucherOrder);
                    //5 ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("获取订单异常", e);
                    handlePendingList();
                }
            }
        }

        //在PendingList再次处理
        private void handlePendingList(){
            while (true) {
                try {
                    //1获取PendingList中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2判断获取是否成功
                    if(list == null || list.isEmpty()){
                        //2.1如果失败说明没有信息，结束循环
                        break;
                    }
                    //3 解析消息中的订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //4如果成功则开始下单
                    handleVoucherOrder(voucherOrder);
                    //5 ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("PendingList中获取订单异常", e);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

    //阻塞队列实现
/*    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                //1获取队列中的订单信息
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    //2创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("获取订单异常", e);
                }
            }
        }
    }
 */

    //双重校验加锁保证一个用户一次只能进去一个线程
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //（1）获取指定锁名称的锁对象
        RLock lock = redissonClient.getLock("order:" + voucherOrder.getUserId());
        //（2）尝试获取锁
        boolean isLock = lock.tryLock();
        //(3)判断获取锁是否成功
        if (!isLock) {
            //返回失败结果
            log.error("不允许重复下单");
            return ;
        }
        try {
            //又因为这个方法带有事务，需要Bean对象来实现，所以要调用该方法获取该类实现接口的Bean对象
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            //simpleRedisLock.unlock();
            lock.unlock();
        }
    }

    //定义动态代理对象
    private IVoucherOrderService proxy;

    //消息队列
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户id,订单id
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        //1判断Lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId));
        //2判断是否为0
        int r = result.intValue();
        if (r != 0) {
            //2.1不为0，代表没有下单资格
            return Result.fail(r == 1 ? "库存不足" : "不可重复下单");
        }

        //3获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //4返回订单id
        return Result.ok(orderId);
    }

    //阻塞队列实现
/*    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户id
        Long userId = UserHolder.getUser().getId();
        //1判断Lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString());
        //2判断是否为0
        int r = result.intValue();
        if (r != 0) {
            //2.1不为0，代表没有下单资格
            return Result.fail(r == 1 ? "库存不足" : "不可重复下单");
        }
        //2.2为0有资格下单，将订单信息保存到消息队列
        //创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //2.3代金券id
        voucherOrder.setVoucherId(voucherId);
        //2.4订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //2.5用户id
        voucherOrder.setUserId(userId);
        //2.6放入阻塞队列
        orderTasks.add(voucherOrder);
        //3获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //4返回订单id
        return Result.ok(orderId);
    }
 */
    //双重校验，避免超卖、写入数据库
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //5一人一单
        Long userId = voucherOrder.getUserId();
        //查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            //重复购买
            log.error("不能重复购买");
            return;
        }
        //6扣除库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足");
            return;
        }
        save(voucherOrder);
    }

/*
    @Override
    public Result seckillVoucher(Long voucherId) {
        //1查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2判断是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始");
        }
        //3判断是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束");
        }
        //4判断库存是否充足
        if(voucher.getStock() < 1){
            return Result.fail("优惠券已被抢光");
        }

        Long userId = UserHolder.getUser().getId();
        
        //这里使用Redisson来实现（即对Redis不足的改进）第三种
        //（1）获取指定锁名称的锁对象
        RLock lock = redissonClient.getLock("order:" + userId);
        //（2）尝试获取锁
        boolean isLock = lock.tryLock();
        //(3)判断获取锁是否成功
        if (!isLock) {
            //返回失败结果
            return Result.fail("一人只能下一单");
        }
        try {
            //又因为这个方法带有事务，需要Bean对象来实现，所以要调用该方法获取该类实现接口的Bean对象
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            //释放锁
            //simpleRedisLock.unlock();
            lock.unlock();
        }
    }
*/

/*
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        //5一人一单
        Long userId = UserHolder.getUser().getId();
        //查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            //重复购买
            return Result.fail("优惠券不能重复购买");
        }
        //6扣除库存
        boolean success = seckillVoucherService.update().
                setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0)
                .update();
        if (!success) {
            return Result.fail("优惠券已被抢光");
        }
        //7创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //7.1代金券id
        voucherOrder.setVoucherId(voucherId);
        //7.2订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //7.3用户id
        voucherOrder.setUserId(userId);

        save(voucherOrder);

        //8返回
        return Result.ok(orderId);
    }
 */
}
