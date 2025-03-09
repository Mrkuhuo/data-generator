<template>
  <div class="executions-container">
    <div class="toolbar">
      <el-form :inline="true" :model="queryForm">
        <el-form-item label="任务名称">
          <el-select
            v-model="queryForm.taskName"
            filterable
            allow-create
            default-first-option
            placeholder="请选择或输入任务名称"
            style="width: 220px">
            <el-option
              v-for="(name, id) in taskNameMap"
              :key="id"
              :label="name"
              :value="name" />
          </el-select>
        </el-form-item>
        <el-form-item label="状态">
          <el-select v-model="queryForm.status" placeholder="请选择状态" @change="handleSearch">
            <el-option label="全部" value="" />
            <el-option label="成功" value="SUCCESS" />
            <el-option label="失败" value="FAILED" />
            <el-option label="运行中" value="RUNNING" />
            <el-option label="已停止" value="STOPPED" />
          </el-select>
        </el-form-item>
        <el-form-item label="时间范围">
          <el-date-picker
            v-model="queryForm.timeRange"
            type="datetimerange"
            range-separator="至"
            start-placeholder="开始时间"
            end-placeholder="结束时间" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="handleSearch">
            <el-icon><Search /></el-icon>查询
          </el-button>
          <el-button @click="handleReset">
            <el-icon><Refresh /></el-icon>重置
          </el-button>
        </el-form-item>
      </el-form>
    </div>

    <el-table :data="executions" style="width: 100%" v-loading="loading">
      <el-table-column prop="id" label="ID" width="80" />
      <el-table-column label="任务名称" min-width="150">
        <template #default="scope">
          {{ taskNameMap[scope.row.taskId] || '未知任务' }}
        </template>
      </el-table-column>
      <el-table-column label="开始时间" width="180">
        <template #default="scope">
          {{ formatDateTime(scope.row.startTime) }}
        </template>
      </el-table-column>
      <el-table-column label="结束时间" width="180">
        <template #default="scope">
          {{ formatDateTime(scope.row.endTime) }}
        </template>
      </el-table-column>
      <el-table-column prop="status" label="状态" width="100">
        <template #default="scope">
          <el-tag :type="getStatusType(scope.row.status)">
            {{ getStatusText(scope.row.status) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="totalCount" label="总记录数" width="100" />
      <el-table-column prop="successCount" label="成功数" width="100" />
      <el-table-column prop="errorCount" label="失败数" width="100" />
      <el-table-column label="操作" width="120" fixed="right">
        <template #default="scope">
          <el-button-group>
            <el-button
              type="primary"
              link
              @click="handleViewDetail(scope.row)">
              详情
            </el-button>
            <el-button
              type="danger"
              link
              @click="handleDelete(scope.row)">
              删除
            </el-button>
          </el-button-group>
        </template>
      </el-table-column>
    </el-table>

    <div class="pagination">
      <el-pagination
        v-model:current-page="currentPage"
        v-model:page-size="pageSize"
        :page-sizes="[10, 20, 50, 100]"
        :total="total"
        layout="total, sizes, prev, pager, next, jumper"
        @size-change="handleSizeChange"
        @current-change="handleCurrentChange" />
    </div>

    <!-- 执行详情对话框 -->
    <el-dialog
      v-model="detailVisible"
      title="执行详情"
      width="800px">
      <el-descriptions :column="2" border>
        <el-descriptions-item label="ID">
          {{ detail.id }}
        </el-descriptions-item>
        <el-descriptions-item label="任务名称">
          {{ taskNameMap[detail.taskId] || '未知任务' }}
        </el-descriptions-item>
        <el-descriptions-item label="开始时间">
          {{ formatDateTime(detail.startTime) }}
        </el-descriptions-item>
        <el-descriptions-item label="结束时间">
          {{ formatDateTime(detail.endTime) }}
        </el-descriptions-item>
        <el-descriptions-item label="状态">
          <el-tag :type="getStatusType(detail.status)">
            {{ getStatusText(detail.status) }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="总记录数">
          {{ detail.totalCount }}
        </el-descriptions-item>
        <el-descriptions-item label="成功数">
          {{ detail.successCount }}
        </el-descriptions-item>
        <el-descriptions-item label="失败数">
          {{ detail.errorCount }}
        </el-descriptions-item>
      </el-descriptions>

      <div class="error-message" v-if="detail.errorMessage">
        <h4>错误信息：</h4>
        <pre>{{ detail.errorMessage }}</pre>
      </div>

      <div class="progress-chart" v-if="detail.id">
        <h4>执行进度：</h4>
        <div ref="chartRef" style="height: 300px"></div>
      </div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted, computed, watch } from 'vue'
import { Search, Refresh } from '@element-plus/icons-vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import * as echarts from 'echarts'
import request, { ApiResponse } from '../utils/request'
import { format } from 'date-fns'

const loading = ref(false)
const detailVisible = ref(false)
const currentPage = ref(1)
const pageSize = ref(10)
const total = ref(0)
const chartRef = ref<HTMLElement>()
let chart: echarts.ECharts | null = null

// 任务ID到任务名称的映射
const taskNameMap = ref<Record<number, string>>({})

const queryForm = reactive({
  taskName: '',
  status: '',
  timeRange: [] as Date[]
})

const detail = reactive({
  id: 0,
  taskId: 0,
  startTime: '',
  endTime: '',
  status: '',
  totalCount: 0,
  successCount: 0,
  errorCount: 0,
  errorMessage: ''
} as any)

const executions = ref([])

// 格式化日期时间
const formatDateTime = (dateTime: string | null) => {
  if (!dateTime) return '-'
  try {
    return format(new Date(dateTime), 'yyyy-MM-dd HH:mm:ss')
  } catch (e) {
    return dateTime
  }
}

// 获取状态类型
const getStatusType = (status: string) => {
  const statusMap: Record<string, string> = {
    'SUCCESS': 'success',
    'FAILED': 'danger',
    'RUNNING': 'warning',
    'COMPLETED': 'success',
    'STOPPED': 'info'
  }
  return statusMap[status] || 'info'
}

// 获取状态文本
const getStatusText = (status: string) => {
  const statusTextMap: Record<string, string> = {
    'SUCCESS': '成功',
    'FAILED': '失败',
    'RUNNING': '运行中',
    'COMPLETED': '已完成',
    'STOPPED': '已停止'
  }
  return statusTextMap[status] || status
}

// 加载任务数据
const loadTaskData = async () => {
  try {
    const res = await request.get<any, ApiResponse<any[]>>('/tasks')
    if (res.code === 200 && res.data) {
      const taskMap: Record<number, string> = {}
      res.data.forEach((task: any) => {
        taskMap[task.id] = task.name
      })
      taskNameMap.value = taskMap
    }
  } catch (error) {
    console.error('加载任务数据失败', error)
  }
}

// 加载执行记录数据
const loadData = async () => {
  loading.value = true
  try {
    // 构建查询参数
    const params: any = {
      pageNum: currentPage.value,
      pageSize: pageSize.value
    }
    
    if (queryForm.status) {
      params.status = queryForm.status
    }
    
    if (queryForm.taskName) {
      params.taskName = queryForm.taskName
    }
    
    if (queryForm.timeRange && queryForm.timeRange.length === 2) {
      params.startTime = format(queryForm.timeRange[0], 'yyyy-MM-dd HH:mm:ss')
      params.endTime = format(queryForm.timeRange[1], 'yyyy-MM-dd HH:mm:ss')
    }
    
    const res = await request.get<any, ApiResponse<any>>('/execution-records/page', { params })
    if (res.code === 200) {
      executions.value = res.data.records || []
      total.value = res.data.total || 0
    } else {
      ElMessage.error(res.message || '加载数据失败')
    }
  } catch (error) {
    console.error('加载数据失败', error)
    ElMessage.error('加载数据失败')
  } finally {
    loading.value = false
  }
}

const handleSearch = () => {
  currentPage.value = 1
  loadData()
}

const handleReset = () => {
  Object.assign(queryForm, {
    taskName: '',
    status: '',
    timeRange: []
  })
  handleSearch()
}

const handleSizeChange = (val: number) => {
  pageSize.value = val
  loadData()
}

const handleCurrentChange = (val: number) => {
  currentPage.value = val
  loadData()
}

// 查看详情
const handleViewDetail = async (row: any) => {
  try {
    const res = await request.get<any, ApiResponse<any>>(`/execution-records/${row.id}`)
    if (res.code === 200) {
      Object.assign(detail, res.data)
      detailVisible.value = true
      
      // 初始化图表
      setTimeout(() => {
        initChart()
      }, 100)
    } else {
      ElMessage.error(res.message || '获取详情失败')
    }
  } catch (error) {
    console.error('获取详情失败', error)
    ElMessage.error('获取详情失败')
  }
}

// 初始化图表
const initChart = () => {
  if (chartRef.value) {
    if (chart) {
      chart.dispose()
    }
    chart = echarts.init(chartRef.value)
    
    // 计算时间点
    const startTime = new Date(detail.startTime)
    const endTime = detail.endTime ? new Date(detail.endTime) : new Date()
    const duration = endTime.getTime() - startTime.getTime()
    
    // 生成中间点
    const midPoints = []
    if (duration > 10000) { // 如果执行时间超过10秒，添加中间点
      const pointCount = 5
      for (let i = 1; i < pointCount; i++) {
        const time = new Date(startTime.getTime() + (duration * i / pointCount))
        const successValue = detail.successCount * i / pointCount
        const errorValue = detail.errorCount * i / pointCount
        midPoints.push({
          success: { value: [time, successValue] },
          error: { value: [time, errorValue] }
        })
      }
    }
    
    const option = {
      title: {
        text: '执行进度'
      },
      tooltip: {
        trigger: 'axis',
        formatter: function(params: any) {
          const time = format(new Date(params[0].value[0]), 'yyyy-MM-dd HH:mm:ss')
          let html = `${time}<br/>`
          params.forEach((param: any) => {
            html += `${param.seriesName}: ${Math.round(param.value[1])}<br/>`
          })
          return html
        }
      },
      legend: {
        data: ['成功数', '失败数']
      },
      xAxis: {
        type: 'time',
        axisLabel: {
          formatter: '{HH}:{mm}:{ss}'
        }
      },
      yAxis: {
        type: 'value'
      },
      series: [
        {
          name: '成功数',
          type: 'line',
          data: [
            { value: [startTime, 0] },
            ...midPoints.map(p => p.success),
            { value: [endTime, detail.successCount] }
          ],
          itemStyle: {
            color: '#67C23A'
          }
        },
        {
          name: '失败数',
          type: 'line',
          data: [
            { value: [startTime, 0] },
            ...midPoints.map(p => p.error),
            { value: [endTime, detail.errorCount] }
          ],
          itemStyle: {
            color: '#F56C6C'
          }
        }
      ]
    }
    chart.setOption(option)
  }
}

// 删除执行记录
const handleDelete = (row: any) => {
  ElMessageBox.confirm('确认删除该执行记录吗？', '提示', {
    type: 'warning'
  }).then(async () => {
    try {
      const res = await request.delete<any, ApiResponse<any>>(`/execution-records/${row.id}`)
      if (res.code === 200) {
        ElMessage.success('删除成功')
        loadData()
      } else {
        ElMessage.error(res.message || '删除失败')
      }
    } catch (error) {
      console.error('删除失败', error)
      ElMessage.error('删除失败')
    }
  })
}

// 监听对话框关闭
watch(detailVisible, (val) => {
  if (!val && chart) {
    chart.dispose()
    chart = null
  }
})

// 页面加载时获取数据
onMounted(() => {
  loadTaskData()
  loadData()
  
  // 添加窗口大小变化监听
  window.addEventListener('resize', () => {
    if (chart) {
      chart.resize()
    }
  })
})
</script>

<style scoped>
.executions-container {
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

.toolbar {
  margin-bottom: 20px;
}

.pagination {
  margin-top: 20px;
  display: flex;
  justify-content: flex-end;
}

.error-message {
  margin-top: 20px;
  padding: 10px;
  background-color: #fef0f0;
  border-radius: 4px;
}

.error-message pre {
  margin: 10px 0;
  white-space: pre-wrap;
  word-wrap: break-word;
}

.progress-chart {
  margin-top: 20px;
}
</style> 