package com.datagenerator.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.datagenerator.entity.User;

public interface UserService extends IService<User> {
    /**
     * 根据用户名查询用户
     */
    User getByUsername(String username);
    
    /**
     * 创建用户
     */
    void createUser(User user);
    
    /**
     * 更新用户
     */
    void updateUser(User user);
    
    /**
     * 删除用户
     */
    void deleteUser(Long id);
    
    /**
     * 修改密码
     */
    void updatePassword(Long id, String oldPassword, String newPassword);
} 