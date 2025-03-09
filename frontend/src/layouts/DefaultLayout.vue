<template>
  <el-container class="layout-container">
    <el-aside width="200px">
      <div class="logo-container">
        <div class="dg-logo">DG</div>
        <h2 class="platform-title">数据生成平台</h2>
      </div>
      <el-menu
        :router="true"
        :default-active="route.path"
        class="el-menu-vertical">
        <el-menu-item index="/">
          <el-icon><HomeFilled /></el-icon>
          <span>首页</span>
        </el-menu-item>
        <el-menu-item index="/data-sources">
          <el-icon><Connection /></el-icon>
          <span>数据源管理</span>
        </el-menu-item>
        <el-menu-item index="/tasks">
          <el-icon><List /></el-icon>
          <span>任务管理</span>
        </el-menu-item>
        <el-menu-item index="/executions">
          <el-icon><Document /></el-icon>
          <span>执行记录</span>
        </el-menu-item>
      </el-menu>
    </el-aside>
    <el-container>
      <el-header>
        <div class="header-title">{{ route.meta.title }}</div>
        <div class="header-links">
          <a href="https://gitee.com/wzylzjtn/data-warehouse-learning" target="_blank" class="repo-link">
            <el-button type="primary" size="small" plain>Gitee仓库</el-button>
          </a>
          <a href="https://github.com/Mrkuhuo/data-warehouse-learning" target="_blank" class="repo-link">
            <el-button type="primary" size="small" plain>GitHub仓库</el-button>
          </a>
          <el-popover
            placement="bottom"
            trigger="hover"
            width="200"
          >
            <template #reference>
              <el-button type="success" size="small" plain>公众号</el-button>
            </template>
            <div class="qrcode-container">
              <img src="/images/gzh.jpg" alt="公众号二维码" class="qrcode-image" />
              <div class="qrcode-text">扫码关注"大数据技能圈"</div>
            </div>
          </el-popover>
        </div>
      </el-header>
      <el-main>
        <router-view></router-view>
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup lang="ts">
import { useRoute } from 'vue-router'
import { HomeFilled, Connection, List, Document } from '@element-plus/icons-vue'

const route = useRoute()
</script>

<style scoped>
.layout-container {
  height: 100vh;
}

.el-aside {
  background-color: #304156;
}

.el-menu {
  border-right: none;
}

.el-menu-vertical {
  height: 100%;
  background-color: #304156;
}

.el-header {
  background-color: #fff;
  border-bottom: 1px solid #dcdfe6;
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.header-title {
  font-size: 18px;
  font-weight: bold;
}

.header-links {
  display: flex;
  gap: 10px;
  margin-left: auto;
}

.repo-link {
  text-decoration: none;
}

.qrcode-container {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
}

.qrcode-image {
  width: 150px;
  height: 150px;
  object-fit: cover;
}

.qrcode-text {
  margin-top: 8px;
  font-size: 14px;
  color: #606266;
}

.el-main {
  background-color: #f0f2f5;
  padding: 20px;
}

.logo-container {
  display: flex;
  align-items: center;
  padding: 16px 10px;
  background-color: #304156;
}

.dg-logo {
  font-size: 24px;
  font-weight: bold;
  color: #fff;
  background: linear-gradient(135deg, #409EFF, #1E88E5);
  width: 40px;
  height: 40px;
  border-radius: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin-right: 10px;
  box-shadow: 0 3px 6px rgba(0, 0, 0, 0.16);
}

.platform-title {
  font-size: 16px;
  font-weight: 600;
  color: #fff;
  margin: 0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
</style> 