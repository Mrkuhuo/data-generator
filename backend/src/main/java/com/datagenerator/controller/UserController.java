package com.datagenerator.controller;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.datagenerator.common.Result;
import com.datagenerator.entity.User;
import com.datagenerator.service.UserService;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

@RestController
@RequestMapping("/users")
public class UserController {

    @Resource
    private UserService userService;

    @GetMapping("/page")
    @PreAuthorize("hasAuthority('user:list')")
    public Result<Page<User>> page(@RequestParam(defaultValue = "1") Integer current,
                                 @RequestParam(defaultValue = "10") Integer size,
                                 @RequestParam(required = false) String username) {
        // 构建查询条件
        LambdaQueryWrapper<User> wrapper = new LambdaQueryWrapper<>();
        wrapper.like(username != null, User::getUsername, username)
                .eq(User::getDeleted, 0)
                .orderByDesc(User::getCreateTime);
        
        // 分页查询
        return Result.success(userService.page(new Page<>(current, size), wrapper));
    }

    @GetMapping("/{id}")
    @PreAuthorize("hasAuthority('user:query')")
    public Result<User> getById(@PathVariable Long id) {
        return Result.success(userService.getById(id));
    }

    @PostMapping
    @PreAuthorize("hasAuthority('user:add')")
    public Result<Void> save(@RequestBody User user) {
        userService.createUser(user);
        return Result.success();
    }

    @PutMapping
    @PreAuthorize("hasAuthority('user:update')")
    public Result<Void> update(@RequestBody User user) {
        userService.updateUser(user);
        return Result.success();
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("hasAuthority('user:delete')")
    public Result<Void> delete(@PathVariable Long id) {
        userService.deleteUser(id);
        return Result.success();
    }

    @PutMapping("/{id}/password")
    @PreAuthorize("hasAuthority('user:update')")
    public Result<Void> updatePassword(@PathVariable Long id,
                                    @RequestParam String oldPassword,
                                    @RequestParam String newPassword) {
        userService.updatePassword(id, oldPassword, newPassword);
        return Result.success();
    }
} 