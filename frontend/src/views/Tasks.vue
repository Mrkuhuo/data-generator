<template>
  <div class="tasks-container">
    <div class="toolbar">
      <el-button type="primary" @click="handleAdd">
        <el-icon><Plus /></el-icon>添加任务
      </el-button>
    </div>

    <el-table :data="tasks" style="width: 100%" v-loading="loading">
      <el-table-column prop="name" label="任务名称" />
      <el-table-column prop="dataSourceName" label="数据源" />
      <el-table-column prop="targetType" label="目标类型">
        <template #default="scope">
          <el-tag>{{ scope.row.targetType }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="targetName" label="目标名称" />
      <el-table-column prop="writeMode" label="写入模式">
        <template #default="scope">
          <el-tag :type="getWriteModeType(scope.row.writeMode)">
            {{ scope.row.writeMode }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="dataFormat" label="数据格式">
        <template #default="scope">
          <el-tag type="info">{{ scope.row.dataFormat }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="status" label="状态">
        <template #default="scope">
          <el-tag :type="getStatusType(scope.row.status)">
            {{ getStatusText(scope.row.status) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="createTime" label="创建时间" width="180" />
      <el-table-column label="操作" width="250" fixed="right">
        <template #default="scope">
          <el-button-group>
            <el-button
              type="primary"
              link
              @click="handleEdit(scope.row)">
              编辑
            </el-button>
            <el-button
              type="success"
              link
              @click="handleStart(scope.row)"
              :disabled="scope.row.status === 'RUNNING'">
              启动
            </el-button>
            <el-button
              type="warning"
              link
              @click="handleStop(scope.row)"
              :disabled="scope.row.status !== 'RUNNING'">
              停止
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

    <!-- 添加分页控件 -->
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

    <!-- 添加/编辑任务对话框 -->
    <el-dialog
      v-model="dialogVisible"
      :title="dialogType === 'add' ? '添加任务' : '编辑任务'"
      width="600px">
      <el-form
        ref="formRef"
        :model="form"
        :rules="rules"
        label-width="100px">
        <el-form-item label="任务名称" prop="name">
          <el-input v-model="form.name" placeholder="请输入任务名称" />
        </el-form-item>
        <el-form-item label="数据源" prop="dataSourceId">
          <el-select 
            v-model="form.dataSourceId" 
            placeholder="请选择数据源"
            @change="handleDataSourceChange">
            <el-option
              v-for="item in dataSources"
              :key="item.id"
              :label="item.name"
              :value="item.id" />
          </el-select>
        </el-form-item>
        
        <!-- MySQL等数据库特有的表选择 -->
        <el-form-item 
          v-if="currentDataSource?.type && ['MYSQL', 'ORACLE'].includes(currentDataSource.type)"
          label="目标表" 
          prop="targetTables">
          <div class="table-select-container">
            <div class="table-select-buttons">
              <el-button size="small" @click="handleSelectAll">全选</el-button>
              <el-button size="small" @click="handleInvertSelection">反选</el-button>
              <el-button size="small" type="info" @click="showTableDependencies" :disabled="selectedTables.length === 0">
                查看表依赖关系
              </el-button>
            </div>
            <el-select
              v-model="selectedTables"
              multiple
              placeholder="请选择目标表"
              :disabled="!form.dataSourceId"
              style="width: 100%"
              @change="handleTableSelect">
              <el-option
                v-for="table in tables"
                :key="table.name"
                :label="table.name"
                :value="table.name">
                <span>{{ table.name }}</span>
              </el-option>
            </el-select>
            <div class="field-tips">
              <p>选择表时，系统会自动检测表之间的外键依赖关系。建议同时选择有依赖关系的表，以避免外键约束错误。</p>
              <p>数据生成时会按照依赖关系顺序处理表，确保先生成主表数据，再生成从表数据。</p>
              <p><strong>注意：</strong>如果选择的表有外键依赖于未选择的表，可能会导致外键约束错误。系统会尝试自动处理外键依赖，但最好选择完整的表依赖链。</p>
            </div>
            <!-- 显示选中表的列信息 -->
            <div v-if="selectedTables.length > 0" class="table-columns-info">
              <div v-for="tableName in selectedTables" :key="tableName" class="table-column">
                <div class="table-header" @click="toggleTableExpand(tableName)">
                  <h4>{{ tableName }} 表结构</h4>
                  <el-button type="text">
                    {{ isTableExpanded(tableName) ? '收起' : '展开' }}
                    <el-icon>
                      <component :is="isTableExpanded(tableName) ? 'ArrowUp' : 'ArrowDown'" />
                    </el-icon>
                  </el-button>
                </div>
                <el-collapse-transition>
                  <div v-show="isTableExpanded(tableName)">
                    <el-table :data="tables.find(t => t.name === tableName)?.columns || []" size="small">
                      <el-table-column prop="name" label="字段名" />
                      <el-table-column prop="type" label="类型" />
                      <el-table-column prop="comment" label="注释" />
                    </el-table>
                  </div>
                </el-collapse-transition>
              </div>
            </div>
          </div>
        </el-form-item>

        <!-- Kafka特有的主题配置 -->
        <template v-if="currentDataSource?.type === 'KAFKA'">
          <el-form-item label="目标主题" prop="targetName">
            <el-input v-model="form.targetName" placeholder="请输入目标主题名称">
              <template #append>
                <el-button @click="handleCreateTopic" type="primary">创建主题</el-button>
              </template>
            </el-input>
            <div class="field-tips">
              <p>如果主题不存在，可以点击"创建主题"按钮创建新主题</p>
            </div>
          </el-form-item>
          <el-form-item label="分区数" prop="partitions" v-if="showTopicConfig">
            <el-input-number v-model="form.partitions" :min="1" :max="100" />
            <div class="field-tips">
              <p>主题的分区数，建议根据数据量和并行度需求设置</p>
            </div>
          </el-form-item>
          <el-form-item label="副本数" prop="replicationFactor" v-if="showTopicConfig">
            <el-input-number v-model="form.replicationFactor" :min="1" :max="3" />
            <div class="field-tips">
              <p>主题的副本数，建议在1-3之间，需要小于或等于Broker数量</p>
            </div>
          </el-form-item>
          <el-form-item label="数据格式" prop="dataFormat">
            <el-select v-model="form.dataFormat" placeholder="请选择数据格式">
              <el-option label="JSON" value="JSON" />
              <el-option label="AVRO" value="AVRO" />
              <el-option label="PROTOBUF" value="PROTOBUF" />
            </el-select>
          </el-form-item>
          <el-form-item label="消息模板" prop="template">
            <el-input
              v-model="form.template"
              type="textarea"
              :rows="6"
              placeholder="请输入消息生成模板（JSON格式）" />
            <div class="field-tips">
              <p>使用JSON格式定义消息结构和数据生成规则，每个字段需要指定type和params</p>
              <p>支持的类型: <strong>string</strong>(字符串), <strong>random</strong>(随机数), <strong>enum</strong>(枚举), <strong>date</strong>(日期), <strong>sequence</strong>(序列号)</p>
              <p>示例模板:</p>
              <pre>{
  "common": { 
    "type": "string", 
    "params": { 
      "pattern": "{\"ar\":\"${enum:xiaomi|huawei|oppo}\",\"uid\":\"${string:[a-z0-9]{6}}\"}" 
    } 
  },
  "start": { 
    "type": "string", 
    "params": { 
      "pattern": "{\"entry\":\"${enum:icon|notice|install}\"}" 
    } 
  },
  "actions": { 
    "type": "string", 
    "params": { 
      "pattern": "[{\"action_id\":\"${string:act[0-9]{3}}\"}]" 
    } 
  },
  "ts": { 
    "type": "random", 
    "params": { 
      "min": 1704067200000, 
      "max": 1735689599000, 
      "integer": true 
    } 
  }
}</pre>
              <p>模式占位符: <code>${enum:v1|v2}</code>, <code>${random:min-max}</code>, <code>${string:pattern}</code></p>
            </div>
          </el-form-item>
        </template>

        <el-form-item label="写入模式" prop="writeMode">
          <el-select v-model="form.writeMode" placeholder="请选择写入模式">
            <el-option label="覆盖" value="OVERWRITE" />
            <el-option label="追加" value="APPEND" />
            <el-option 
              label="更新" 
              value="UPDATE" 
              v-if="currentDataSource?.type && ['MYSQL', 'ORACLE'].includes(currentDataSource.type)" />
          </el-select>
        </el-form-item>
        <el-form-item label="批量大小" prop="batchSize">
          <el-input-number v-model="form.batchSize" :min="1" :max="10000" />
        </el-form-item>
        <el-form-item label="生成频率" prop="frequency">
          <el-input-number v-model="form.frequency" :min="1" :max="3600" />
          <span class="ml-2">秒</span>
        </el-form-item>
        <el-form-item label="并发数" prop="concurrentNum">
          <el-input-number v-model="form.concurrentNum" :min="1" :max="10" />
        </el-form-item>
      </el-form>
      <template #footer>
        <span class="dialog-footer">
          <el-button @click="dialogVisible = false">取消</el-button>
          <el-button type="primary" @click="handleSubmit">确定</el-button>
        </span>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted, watch } from 'vue'
import { Plus, ArrowUp, ArrowDown } from '@element-plus/icons-vue'
import type { FormInstance, FormRules } from 'element-plus'
import { ElMessage, ElMessageBox } from 'element-plus'
import axios from 'axios'

const API_BASE_URL = 'http://localhost:8888/api'

// 创建 axios 实例
const api = axios.create({
  baseURL: API_BASE_URL
})

const loading = ref(false)
const dialogVisible = ref(false)
const dialogType = ref<'add' | 'edit'>('add')
const formRef = ref<FormInstance>()

// 添加分页相关变量
const currentPage = ref(1)
const pageSize = ref(10)
const total = ref(0)

interface DataSource {
  id: number
  name: string
  type: string
  url: string
  username: string
  description?: string
  createTime?: string
}

interface TableColumn {
  name: string
  type: string
  comment?: string
}

interface Table {
  name: string
  description?: string
  columns?: TableColumn[]
}

interface Task {
  id: number
  name: string
  dataSourceName: string
  targetType: string
  targetName: string
  writeMode: string
  dataFormat: string
  status: string
  createTime: string
}

// 数据源列表
const dataSources = ref<DataSource[]>([])
// 表列表
const tables = ref<Table[]>([])
// 选中的表
const selectedTables = ref<string[]>([])

const form = reactive({
  id: undefined as number | undefined,
  name: '',
  dataSourceId: undefined as number | undefined,
  targetType: 'TABLE',
  targetName: '',
  writeMode: '',
  dataFormat: '',
  template: '',
  batchSize: 1000,
  frequency: 100,
  concurrentNum: 1,
  // Kafka特有配置
  partitions: 1,
  replicationFactor: 1
})

// 当前选中的数据源
const currentDataSource = ref<DataSource | null>(null)

// 添加展开状态管理
const expandedTables = ref<Set<string>>(new Set())

// 切换表的展开状态
const toggleTableExpand = (tableName: string) => {
  if (expandedTables.value.has(tableName)) {
    expandedTables.value.delete(tableName)
  } else {
    expandedTables.value.add(tableName)
  }
}

// 检查表是否展开
const isTableExpanded = (tableName: string) => {
  return expandedTables.value.has(tableName)
}

// 加载数据源列表
const loadDataSources = async () => {
  try {
    const response = await api.get('/data-sources/page', {
      params: {
        pageNum: 1,
        pageSize: 100
      }
    })
    dataSources.value = response.data.data.records
  } catch (error) {
    ElMessage.error('加载数据源列表失败')
  }
}

// 监听数据源变化
watch(() => form.dataSourceId, async (newId) => {
  if (newId) {
    // 获取当前选中的数据源信息
    currentDataSource.value = dataSources.value.find(ds => ds.id === newId) || null
    // 如果不是在编辑状态，才清空选中的表
    if (dialogType.value !== 'edit') {
      selectedTables.value = []
    }
    await loadTables(newId)
  } else {
    currentDataSource.value = null
    tables.value = []
    selectedTables.value = []
  }
})

// 加载表列表
const loadTables = async (dataSourceId: number): Promise<void> => {
  try {
    const response = await api.get(`/data-sources/${dataSourceId}/tables`)
    if (response.data.code === 200) {
      // 如果是关系型数据库，加载表结构
      if (currentDataSource.value?.type === 'MYSQL' || currentDataSource.value?.type === 'POSTGRESQL') {
        const tablesWithColumns = await Promise.all(
          response.data.data.map(async (tableName: string) => {
            try {
              const columnResponse = await api.get(`/data-sources/${dataSourceId}/tables/${encodeURIComponent(tableName)}/columns`)
              if (columnResponse.data.code === 200) {
                return {
                  name: tableName,
                  description: tableName,
                  columns: columnResponse.data.data
                }
              }
              return {
                name: tableName,
                description: tableName,
                columns: []
              }
            } catch (error) {
              console.error(`加载表 ${tableName} 结构失败:`, error)
              return {
                name: tableName,
                description: tableName,
                columns: []
              }
            }
          })
        )
        tables.value = tablesWithColumns
      } else {
        tables.value = response.data.data.map((table: string) => ({
          name: table,
          description: table
        }))
      }
    } else {
      throw new Error(response.data.message || '加载表列表失败')
    }
  } catch (error: any) {
    console.error('加载表列表错误:', error)
    throw error
  }
}

const rules = reactive<FormRules>({
  name: [{ required: true, message: '请输入任务名称', trigger: 'blur' }],
  dataSourceId: [{ required: true, message: '请选择数据源', trigger: 'change' }],
  targetType: [{ required: true, message: '请选择目标类型', trigger: 'change' }],
  targetName: [{ 
    required: true, 
    message: '请输入目标名称', 
    trigger: 'blur',
    validator: (rule, value, callback) => {
      if (form.targetType === 'TOPIC' && !value) {
        callback(new Error('请输入目标名称'))
      } else {
        callback()
      }
    }
  }],
  writeMode: [{ required: true, message: '请选择写入模式', trigger: 'change' }],
  dataFormat: [{ 
    required: true, 
    message: '请选择数据格式', 
    trigger: 'change',
    validator: (rule, value, callback) => {
      if (!currentDataSource.value?.type || !['MYSQL', 'POSTGRESQL'].includes(currentDataSource.value.type)) {
        if (!value) {
          callback(new Error('请选择数据格式'))
        } else {
          callback()
        }
      } else {
        callback()
      }
    }
  }],
  template: [{ 
    required: true, 
    message: '请输入数据生成模板', 
    trigger: 'blur',
    validator: (rule, value, callback) => {
      if (!currentDataSource.value?.type || !['MYSQL', 'POSTGRESQL'].includes(currentDataSource.value.type)) {
        if (!value) {
          callback(new Error('请输入数据生成模板'))
        } else {
          try {
            JSON.parse(value)
            callback()
          } catch (e) {
            callback(new Error('数据生成模板必须是有效的JSON格式'))
          }
        }
      } else {
        callback()
      }
    }
  }]
})

const tasks = ref<Task[]>([])

const loadData = async () => {
  loading.value = true
  try {
    const response = await api.get('/tasks/page', {
      params: {
        current: currentPage.value,
        size: pageSize.value
      }
    })
    if (response.data.code === 200) {
      tasks.value = response.data.data.records
      total.value = response.data.data.total || 0
    } else {
      ElMessage.error(response.data.message || '加载任务列表失败')
      tasks.value = []
      total.value = 0
    }
  } catch (error: any) {
    console.error('加载任务列表错误:', error)
    if (error.code === 'ERR_NETWORK' || error.message?.includes('Network Error')) {
      ElMessage.error({
        message: '无法连接到后端服务(端口8888),请确保后端服务已启动',
        duration: 5000
      })
    } else {
      ElMessage.error(error.response?.data?.message || '加载任务列表失败: ' + (error.message || '未知错误'))
    }
    tasks.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

const getWriteModeType = (mode: string) => {
  const modeMap: Record<string, string> = {
    'OVERWRITE': 'danger',
    'APPEND': 'success',
    'UPDATE': 'warning'
  }
  return modeMap[mode] || 'info'
}

const getStatusType = (status: string) => {
  const statusMap: Record<string, string> = {
    'RUNNING': 'success',
    'STOPPED': 'info',
    'COMPLETED': 'success',
    'FAILED': 'danger'
  }
  return statusMap[status] || 'info'
}

const getStatusText = (status: string) => {
  const statusTextMap: Record<string, string> = {
    'RUNNING': '运行中',
    'STOPPED': '已停止',
    'COMPLETED': '已完成',
    'FAILED': '失败'
  }
  return statusTextMap[status] || status
}

const handleAdd = () => {
  dialogType.value = 'add'
  Object.assign(form, {
    id: undefined,
    name: '',
    dataSourceId: undefined,
    targetType: 'TABLE',
    targetName: '',
    writeMode: '',
    dataFormat: '',
    template: '',
    batchSize: 1000,
    frequency: 100,
    concurrentNum: 1,
    partitions: 1,
    replicationFactor: 1
  })
  selectedTables.value = []
  dialogVisible.value = true
}

const handleEdit = async (row: any) => {
  dialogType.value = 'edit'
  
  // 先清空选中的表和展开状态
  selectedTables.value = []
  expandedTables.value = new Set()
  
  // 先设置数据源ID，触发表加载
  form.dataSourceId = row.dataSourceId
  
  // 等待表加载完成
  if (row.dataSourceId) {
    try {
      await loadTables(row.dataSourceId)
      
      // 表加载完成后，再设置其他表单数据
      Object.assign(form, row)
      
      // 如果是数据库表类型，设置选中的表
      if (row.targetType === 'TABLE' && row.targetName) {
        const tableNames = row.targetName.split(',')
        selectedTables.value = tableNames
        // 默认展开第一个表
        if (tableNames.length > 0) {
          expandedTables.value = new Set([tableNames[0]])
        }
      }
    } catch (error) {
      console.error('加载表失败:', error)
      ElMessage.error('加载表失败')
    }
  }
  
  dialogVisible.value = true
}

const handleDelete = (row: any) => {
  ElMessageBox.confirm('确认删除该任务吗？', '提示', {
    type: 'warning'
  }).then(async () => {
    try {
      await api.delete(`/tasks/${row.id}`)
      ElMessage.success('删除成功')
      loadData()
    } catch (error: any) {
      console.error('删除任务失败:', error)
      if (error.code === 'ERR_NETWORK' || error.message?.includes('Network Error')) {
        ElMessage.error({
          message: '无法连接到后端服务(端口8888),请确保后端服务已启动',
          duration: 5000
        })
      } else {
        ElMessage.error(error.response?.data?.message || '删除失败')
      }
    }
  })
}

const handleStart = async (row: any) => {
  try {
    await api.post(`/tasks/${row.id}/start`)
    ElMessage.success('启动成功')
    loadData()
  } catch (error: any) {
    console.error('启动任务失败:', error)
    if (error.code === 'ERR_NETWORK' || error.message?.includes('Network Error')) {
      ElMessage.error({
        message: '无法连接到后端服务(端口8888),请确保后端服务已启动',
        duration: 5000
      })
    } else {
      ElMessage.error(error.response?.data?.message || '启动失败')
    }
  }
}

const handleStop = async (row: any) => {
  try {
    await api.post(`/tasks/${row.id}/stop`)
    ElMessage.success('停止成功')
    loadData()
  } catch (error: any) {
    console.error('停止任务失败:', error)
    if (error.code === 'ERR_NETWORK' || error.message?.includes('Network Error')) {
      ElMessage.error({
        message: '无法连接到后端服务(端口8888),请确保后端服务已启动',
        duration: 5000
      })
    } else {
      ElMessage.error(error.response?.data?.message || '停止失败')
    }
  }
}

// 是否显示主题配置
const showTopicConfig = ref(false)

// 处理数据源变化
const handleDataSourceChange = async (dataSourceId: number) => {
  if (!dataSourceId) {
    currentDataSource.value = null
    tables.value = []
    selectedTables.value = []
    return
  }

  // 获取当前选中的数据源信息
  currentDataSource.value = dataSources.value.find(ds => ds.id === dataSourceId) || null
  
  // 重置表单相关字段
  form.targetName = ''
  form.writeMode = ''
  form.dataFormat = ''
  form.template = ''
  showTopicConfig.value = false

  // 如果是Kafka数据源，设置默认值
  if (currentDataSource.value?.type === 'KAFKA') {
    form.targetType = 'TOPIC'
    form.writeMode = 'APPEND'
    form.dataFormat = 'JSON'
  } else {
    form.targetType = 'TABLE'
    selectedTables.value = []
    await loadTables(dataSourceId)
  }
}

// 处理创建主题
const handleCreateTopic = () => {
  if (!form.targetName) {
    ElMessage.warning('请先输入主题名称')
    return
  }
  showTopicConfig.value = true
}

// 修改handleSubmit方法
const handleSubmit = async () => {
  if (!formRef.value) return
  await formRef.value.validate(async (valid) => {
    if (valid) {
      try {
        const taskData = {
          ...form,
          // 如果是Kafka数据源，处理特殊字段
          ...(currentDataSource.value?.type === 'KAFKA' && showTopicConfig.value ? {
            partitions: form.partitions,
            replicationFactor: form.replicationFactor
          } : {})
        }
        
        // 如果是Kafka数据源，设置目标类型为TOPIC
        if (currentDataSource.value?.type === 'KAFKA') {
          taskData.targetType = 'TOPIC'
        } else if (['MYSQL', 'ORACLE'].includes(currentDataSource.value?.type || '')) {
          // 数据库相关处理保持不变
          taskData.targetType = 'TABLE'
          if (selectedTables.value.length > 0) {
            taskData.targetName = selectedTables.value.join(',')
          }
        }
        
        if (dialogType.value === 'add') {
          await api.post('/tasks', taskData)
        } else {
          await api.put(`/tasks/${taskData.id}`, taskData)
        }
        
        ElMessage.success(dialogType.value === 'add' ? '添加成功' : '更新成功')
        dialogVisible.value = false
        loadData()
      } catch (error: any) {
        console.error('保存任务失败:', error)
        if (error.code === 'ERR_NETWORK' || error.message?.includes('Network Error')) {
          ElMessage.error({
            message: '无法连接到后端服务(端口8888),请确保后端服务已启动',
            duration: 5000
          })
        } else {
          ElMessage.error(error.response?.data?.message || (dialogType.value === 'add' ? '添加失败' : '更新失败'))
        }
      }
    }
  })
}

const handleSelectAll = () => {
  selectedTables.value = tables.value.map(table => table.name)
}

const handleInvertSelection = () => {
  const allTableNames = tables.value.map(table => table.name)
  selectedTables.value = allTableNames.filter(name => !selectedTables.value.includes(name))
}

// 修改handleTableSelect方法
const handleTableSelect = async (value: string[]) => {
  // 保存原始选择
  const originalSelection = [...selectedTables.value];
  selectedTables.value = value;
  
  // 如果选择了表，自动设置为TABLE类型
  if (value.length > 0) {
    form.targetType = 'TABLE';
    form.targetName = value.join(',');
    
    // 检查是否需要自动添加依赖表
    if (value.length > originalSelection.length) {
      // 找出新增的表
      const newlyAddedTables = value.filter(table => !originalSelection.includes(table));
      
      if (newlyAddedTables.length > 0 && currentDataSource.value) {
        try {
          // 获取依赖关系
          const response = await api.get(`/data-sources/${currentDataSource.value.id}/table-dependencies`, {
            params: { tables: newlyAddedTables.join(',') }
          });
          
          if (response.data.code === 200 && response.data.data) {
            const dependencies = response.data.data;
            const missingDependencies: string[] = [];
            
            // 检查是否有缺失的依赖表
            for (const table in dependencies) {
              const requiredTables = dependencies[table];
              for (const requiredTable of requiredTables) {
                if (!selectedTables.value.includes(requiredTable) && !missingDependencies.includes(requiredTable)) {
                  missingDependencies.push(requiredTable);
                }
              }
            }
            
            // 如果有缺失的依赖表，提示用户
            if (missingDependencies.length > 0) {
              ElMessageBox.confirm(
                `表 ${newlyAddedTables.join(', ')} 依赖于以下表：${missingDependencies.join(', ')}。是否自动添加这些依赖表？`,
                '发现表依赖关系',
                {
                  confirmButtonText: '添加依赖表',
                  cancelButtonText: '仅保留选择的表',
                  type: 'warning'
                }
              ).then(() => {
                // 添加依赖表
                const updatedSelection = [...selectedTables.value, ...missingDependencies];
                selectedTables.value = updatedSelection;
                form.targetName = updatedSelection.join(',');
                ElMessage.success('已自动添加依赖表');
              }).catch(() => {
                ElMessage.info('未添加依赖表，可能会导致外键约束错误');
              });
            }
          }
        } catch (error) {
          console.error('获取表依赖关系失败:', error);
        }
      }
    }
  }
}

// 添加一个新方法来显示表依赖关系
const showTableDependencies = async () => {
  if (!currentDataSource.value || selectedTables.value.length === 0) {
    ElMessage.warning('请先选择数据源和表');
    return;
  }
  
  try {
    const response = await api.get(`/data-sources/${currentDataSource.value.id}/table-dependencies`, {
      params: { tables: selectedTables.value.join(',') }
    });
    
    if (response.data.code === 200 && response.data.data) {
      const dependencies = response.data.data;
      let message = '表依赖关系：\n';
      
      for (const table in dependencies) {
        const requiredTables = dependencies[table];
        if (requiredTables.length > 0) {
          message += `${table} 依赖于: ${requiredTables.join(', ')}\n`;
        } else {
          message += `${table} 没有依赖\n`;
        }
      }
      
      ElMessageBox.alert(message, '表依赖关系', {
        dangerouslyUseHTMLString: true
      });
    }
  } catch (error) {
    console.error('获取表依赖关系失败:', error);
    ElMessage.error('获取表依赖关系失败');
  }
}

// 添加分页处理方法
const handleSizeChange = (val: number) => {
  pageSize.value = val
  loadData()
}

const handleCurrentChange = (val: number) => {
  currentPage.value = val
  loadData()
}

onMounted(() => {
  loadData()
  loadDataSources()
})
</script>

<style scoped>
.tasks-container {
  padding: 20px;
}

.toolbar {
  margin-bottom: 20px;
}

.pagination {
  margin-top: 20px;
  display: flex;
  justify-content: flex-end;
}

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
}

.ml-2 {
  margin-left: 8px;
}

.text-gray-400 {
  color: #9ca3af;
}

.table-select-container {
  width: 100%;
}

.table-select-buttons {
  margin-bottom: 8px;
  display: flex;
  gap: 8px;
}

.table-columns-info {
  margin-top: 16px;
}

.table-column {
  margin-bottom: 16px;
  border: 1px solid #EBEEF5;
  border-radius: 4px;
  overflow: hidden;
}

.table-column:last-child {
  margin-bottom: 0;
}

.table-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px;
  background-color: #f5f7fa;
  border-radius: 4px;
  cursor: pointer;
  margin-bottom: 8px;
}

.table-header h4 {
  margin: 0;
}

.field-tips {
  margin-top: 4px;
  font-size: 12px;
  color: #909399;
  max-width: 100%;
  word-break: break-word;
}

.field-tips p {
  margin: 2px 0;
}

.field-tips pre {
  max-height: 120px;
  overflow-y: auto;
  background-color: #f5f7fa;
  padding: 8px;
  border-radius: 4px;
  font-size: 11px;
  margin: 4px 0;
  white-space: pre-wrap;
  word-break: break-word;
}
</style> 