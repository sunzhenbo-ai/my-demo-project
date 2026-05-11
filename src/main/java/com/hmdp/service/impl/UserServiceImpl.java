package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    /**
     * 发送验证码
     * @param phone
     * @param session
     * @return
     */
    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            //2.不符合则返回错误信息
            return Result.fail("手机号格式错误");
        }
        //3。符合则生成验证码
        String code = RandomUtil.randomNumbers(6);
        //4.保存验证码至Redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //5.发送验证码
        log.debug("发送验证码成功,验证码为:{}",code);
        //6.返回ok
        return Result.ok();
    }

    /**
     * 登录校验
     * @param loginForm
     * @param session
     * @return
     */
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1验证手机号
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){
            //不符合则返回错误信息
            return Result.fail("手机扫格式错误");
        }
        //2验证验证码
        String code = loginForm.getCode();
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        if(cacheCode == null || !cacheCode.equals(code)){
            //3不一样返回错误信息
            return Result.fail("验证码错误");
        }
        //4一样，根据手机号查询用户
        User user = query().eq("phone", phone).one();
        //5在数据库检验手机号是否存在
        if(user == null){
            //6不存在则创建新用户
            user = createUserWithPhone(phone);
        }
        //7保存用户信息至Redis
        //7.1随机生成token作为登录令牌
        String token = UUID.randomUUID().toString(true);
        //7.2将User对象转化为hash存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName,fieldValue) -> fieldValue.toString()));
        //7.3存储
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        //7.4设置token有效期
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);
        //返回token
        return Result.ok(token);
    }

    /**
     * 根据号码创建用户
     * @param phone
     * @return
     */
    private User createUserWithPhone(String phone){
        //创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        //保存用户
        save(user);
        return user;
    }

    /**
     * 签到功能
     * @return
     */
    @Override
    public Result sign() {
        //1获取用户id
        Long userId = UserHolder.getUser().getId();
        //2获取当天日期
        LocalDateTime now = LocalDateTime.now();
        //3拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyy-MM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4获取当天天数
        int dayOfMonth = now.getDayOfMonth();
        //5写入Redis
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    /**
     * 查询连续签到天数
     * @return
     */
    @Override
    public Result signCount() {
        //1获取用户id
        Long userId = UserHolder.getUser().getId();
        //2获取当天日期
        LocalDateTime now = LocalDateTime.now();
        //3拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyy-MM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4获取当天天数
        int dayOfMonth = now.getDayOfMonth();
        //5获取本月连续签到天数,返回十进制的数字
        List<Long> results = stringRedisTemplate.opsForValue().bitField(key, BitFieldSubCommands.create()
                .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        if(results == null || results.isEmpty()){
            return Result.ok();
        }
        Long num = results.get(0);
        if (num == null || num == 0){
            return Result.ok();
        }
        //6循环遍历
        int count = 0;
        while (true){
            //7让这个数字与1进行与运算，从而获取最后一个bit位
            // 8判断是否为0
            if((num & 1) == 0){
                //9如果为0代表没有签到，跳出循环
                break;
            }else{
                //10不为零则签到了，计数器加一
                count++;
            }
            //11将数字右移一位，抛弃比较过的bit位，得到新的数字继续比较
            num = num >>> 1;
        }
        return Result.ok(count);
    }
}
