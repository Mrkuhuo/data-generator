<template>
  <div class="home-container">
    <el-row :gutter="20">
      <el-col :span="8">
        <el-card class="box-card">
          <template #header>
            <div class="card-header">
              <span>数据源统计</span>
            </div>
          </template>
          <div class="card-content">
            <el-statistic :value="dataSourceCount">
              <template #title>
                <div style="display: inline-flex; align-items: center">
                  数据源总数
                </div>
              </template>
            </el-statistic>
          </div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card class="box-card">
          <template #header>
            <div class="card-header">
              <span>任务统计</span>
            </div>
          </template>
          <div class="card-content">
            <el-statistic :value="taskCount">
              <template #title>
                <div style="display: inline-flex; align-items: center">
                  任务总数
                </div>
              </template>
            </el-statistic>
          </div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card class="box-card">
          <template #header>
            <div class="card-header">
              <span>运行中任务</span>
            </div>
          </template>
          <div class="card-content">
            <el-statistic :value="runningTaskCount">
              <template #title>
                <div style="display: inline-flex; align-items: center">
                  运行中任务数
                </div>
              </template>
            </el-statistic>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20" class="mt-20">
      <el-col :span="12">
        <el-card class="box-card">
          <template #header>
            <div class="card-header">
              <span>最近执行记录</span>
            </div>
          </template>
          <el-table :data="recentExecutions" style="width: 100%">
            <el-table-column prop="taskName" label="任务名称" />
            <el-table-column prop="startTime" label="开始时间" />
            <el-table-column prop="status" label="状态">
              <template #default="scope">
                <el-tag :type="getStatusType(scope.row.status)">
                  {{ scope.row.status }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="totalCount" label="总记录数" />
            <el-table-column prop="successCount" label="成功数" />
            <el-table-column prop="errorCount" label="失败数" />
          </el-table>
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card class="box-card">
          <template #header>
            <div class="card-header">
              <span>系统状态</span>
            </div>
          </template>
          <div class="system-status">
            <el-descriptions :column="1" border>
              <el-descriptions-item label="CPU使用率">
                <el-progress :percentage="systemStatus.cpuUsage" />
              </el-descriptions-item>
              <el-descriptions-item label="内存使用率">
                <el-progress :percentage="systemStatus.memoryUsage" />
              </el-descriptions-item>
              <el-descriptions-item label="磁盘使用率">
                <el-progress :percentage="systemStatus.diskUsage" />
              </el-descriptions-item>
            </el-descriptions>
          </div>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'

const dataSourceCount = ref(0)
const taskCount = ref(0)
const runningTaskCount = ref(0)
const recentExecutions = ref([])
const systemStatus = ref({
  cpuUsage: 0,
  memoryUsage: 0,
  diskUsage: 0
})

const getStatusType = (status: string) => {
  const statusMap: Record<string, string> = {
    'SUCCESS': 'success',
    'FAILED': 'danger',
    'RUNNING': 'warning',
    'STOPPED': 'info'
  }
  return statusMap[status] || 'info'
}

onMounted(() => {
  // TODO: 从后端获取数据
  dataSourceCount.value = 5
  taskCount.value = 10
  runningTaskCount.value = 3
  recentExecutions.value = [
    {
      taskName: '测试任务1',
      startTime: '2024-03-06 10:00:00',
      status: 'SUCCESS',
      totalCount: 1000,
      successCount: 1000,
      errorCount: 0
    },
    {
      taskName: '测试任务2',
      startTime: '2024-03-06 09:30:00',
      status: 'RUNNING',
      totalCount: 500,
      successCount: 300,
      errorCount: 0
    }
  ]
  systemStatus.value = {
    cpuUsage: 45,
    memoryUsage: 60,
    diskUsage: 30
  }
})
</script>

<style scoped>
.home-container {
  padding: 20px;
}

.logo-container {
  display: flex;
  align-items: center;
  margin-bottom: 20px;
}

.dg-logo {
  font-size: 28px;
  font-weight: bold;
  color: #fff;
  background: linear-gradient(135deg, #409EFF, #1E88E5);
  width: 50px;
  height: 50px;
  border-radius: 10px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin-right: 15px;
  box-shadow: 0 3px 6px rgba(0, 0, 0, 0.16);
}

.platform-title {
  font-size: 22px;
  font-weight: 600;
  color: #303133;
  margin: 0;
}

.box-card {
  margin-bottom: 20px;
}

.mt-20 {
  margin-top: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.card-content {
  text-align: center;
  padding: 20px 0;
}

.system-status {
  padding: 20px;
}
</style> 