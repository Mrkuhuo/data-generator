<template>
  <div class="datasources-container">
    <div class="toolbar">
      <el-button type="primary" @click="handleAdd">
        <el-icon><Plus /></el-icon>添加数据源
      </el-button>
    </div>

    <el-table :data="dataSources" style="width: 100%" v-loading="loading">
      <el-table-column prop="name" label="名称" />
      <el-table-column prop="type" label="类型">
        <template #default="scope">
          <el-tag>{{ scope.row.type }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="url" label="连接URL" show-overflow-tooltip />
      <el-table-column prop="username" label="用户名" />
      <el-table-column prop="description" label="描述" show-overflow-tooltip />
      <el-table-column prop="createTime" label="创建时间" width="180" />
      <el-table-column label="操作" width="200" fixed="right">
        <template #default="scope">
          <el-button-group>
            <el-button type="primary" link @click="handleEdit(scope.row)">
              编辑
            </el-button>
            <el-button type="primary" link @click="handleTest(scope.row)">
              测试连接
            </el-button>
            <el-button type="danger" link @click="handleDelete(scope.row)">
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

    <!-- 添加/编辑数据源对话框 -->
    <el-dialog
      v-model="dialogVisible"
      :title="dialogType === 'add' ? '添加数据源' : '编辑数据源'"
      width="500px">
      <el-form
        ref="formRef"
        :model="form"
        :rules="rules"
        label-width="100px">
        <el-form-item label="名称" prop="name">
          <el-input v-model="form.name" placeholder="请输入数据源名称" />
        </el-form-item>
        <el-form-item label="类型" prop="type">
          <el-select v-model="form.type" placeholder="请选择数据源类型">
            <el-option label="MySQL" value="MYSQL" />
            <el-option label="Oracle" value="ORACLE" />
            <el-option label="Kafka" value="KAFKA" />
          </el-select>
        </el-form-item>
        <el-form-item label="连接URL" prop="url">
          <el-input v-model="form.url" placeholder="请输入连接URL">
            <template #append>
              <el-button @click="handleTest(form)">
                <el-icon><Connection /></el-icon>测试连接
              </el-button>
            </template>
          </el-input>
          <div class="url-tips">
            <template v-if="form.type === 'MYSQL'">
              <p>MySQL示例：jdbc:mysql://localhost:3306/database?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai</p>
            </template>
            <template v-else-if="form.type === 'ORACLE'">
              <p>Oracle示例：jdbc:oracle:thin:@localhost:1521:orcl</p>
            </template>
            <template v-else-if="form.type === 'KAFKA'">
              <p>Kafka示例：localhost:9092（多个broker用逗号分隔，如：localhost:9092,localhost:9093）</p>
            </template>
          </div>
        </el-form-item>
        <el-form-item 
          label="用户名" 
          prop="username"
          :required="form.type !== 'KAFKA'">
          <el-input v-model="form.username" placeholder="请输入用户名" />
          <div class="field-tips" v-if="form.type === 'KAFKA'">
            <p>如果Kafka启用了SASL认证，请填写用户名</p>
          </div>
        </el-form-item>
        <el-form-item 
          label="密码" 
          prop="password"
          :required="form.type !== 'KAFKA'">
          <el-input
            v-model="form.password"
            type="password"
            placeholder="请输入密码"
            show-password />
          <div class="field-tips" v-if="form.type === 'KAFKA'">
            <p>如果Kafka启用了SASL认证，请填写密码</p>
          </div>
        </el-form-item>
        <!-- Kafka特有配置 -->
        <template v-if="form.type === 'KAFKA'">
          <el-form-item label="安全协议" prop="securityProtocol">
            <el-select v-model="form.securityProtocol" placeholder="请选择安全协议">
              <el-option label="PLAINTEXT" value="PLAINTEXT" />
              <el-option label="SASL_PLAINTEXT" value="SASL_PLAINTEXT" />
              <el-option label="SASL_SSL" value="SASL_SSL" />
              <el-option label="SSL" value="SSL" />
            </el-select>
            <div class="field-tips">
              <p>PLAINTEXT: 无安全认证（默认推荐）</p>
              <p>SASL_PLAINTEXT: SASL认证，无SSL加密</p>
              <p>SASL_SSL: SASL认证，使用SSL加密</p>
              <p>SSL: 仅SSL加密，无SASL认证</p>
              <p class="warning-tip">注意：如果Kafka服务器未配置SASL，请使用PLAINTEXT</p>
            </div>
          </el-form-item>
          <el-form-item 
            label="认证机制" 
            prop="saslMechanism"
            v-if="form.securityProtocol?.includes('SASL')">
            <el-select v-model="form.saslMechanism" placeholder="请选择认证机制">
              <el-option label="PLAIN" value="PLAIN" />
              <el-option label="SCRAM-SHA-256" value="SCRAM-SHA-256" />
              <el-option label="SCRAM-SHA-512" value="SCRAM-SHA-512" />
            </el-select>
            <div class="field-tips">
              <p>请确保选择的认证机制与Kafka服务器配置一致</p>
              <p>当前错误表明服务器未启用任何SASL认证机制</p>
            </div>
          </el-form-item>
        </template>
        <el-form-item label="驱动类名" prop="driverClassName" v-if="form.type !== 'KAFKA'">
          <el-input
            v-model="form.driverClassName"
            placeholder="请输入驱动类名" />
        </el-form-item>
        <el-form-item label="描述" prop="description">
          <el-input
            v-model="form.description"
            type="textarea"
            placeholder="请输入描述" />
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
import { Plus, Connection } from '@element-plus/icons-vue'
import type { FormInstance, FormRules } from 'element-plus'
import { ElMessage, ElMessageBox } from 'element-plus'
import axios from 'axios'

const API_BASE_URL = 'http://localhost:8888/api'

// 创建 axios 实例
const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*'
  }
})

// 请求拦截器
api.interceptors.request.use(config => {
  const token = localStorage.getItem('token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  // 添加 CORS 相关的请求头
  config.headers['Access-Control-Allow-Origin'] = '*'
  config.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
  config.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
  return config
})

// 响应拦截器
api.interceptors.response.use(
  response => {
    // 检查响应中是否包含错误信息
    if (response.data.message && response.data.message.includes('Access denied')) {
      return Promise.reject(new Error(response.data.message))
    }
    
    // 检查响应状态码和业务状态码
    if (response.status === 200 && response.data.code === 200) {
      return response
    }
    // 如果状态码不是200或业务状态码不是200，返回错误
    return Promise.reject(new Error(response.data.message || '请求失败'))
  },
  error => {
    if (error.response) {
      switch (error.response.status) {
        case 401:
          // 未授权，跳转到登录页
          localStorage.removeItem('token')
          window.location.href = '/login'
          break
        case 403:
          ElMessage.error('没有权限访问')
          break
        case 404:
          ElMessage.error('请求的资源不存在')
          break
        case 500:
          // 从错误响应中提取具体的错误信息
          const errorMessage = error.response.data?.message || 
                             error.response.data?.error || 
                             '服务器错误'
          console.error('服务器错误:', error) // 添加错误日志
          ElMessage.error(errorMessage)
          break
        default:
          const message = error.response.data?.message || 
                         error.response.data?.error || 
                         '请求失败'
          console.error('请求错误:', error) // 添加错误日志
          ElMessage.error(message)
      }
    } else if (error.request) {
      console.error('网络错误:', error) // 添加错误日志
      ElMessage.error('网络错误，请检查网络连接')
    } else {
      console.error('请求配置错误:', error) // 添加错误日志
      ElMessage.error('请求配置错误')
    }
    return Promise.reject(error)
  }
)

const loading = ref(false)
const dialogVisible = ref(false)
const dialogType = ref<'add' | 'edit'>('add')
const formRef = ref<FormInstance>()
// 添加分页相关变量
const currentPage = ref(1)
const pageSize = ref(10)
const total = ref(0)

const form = reactive({
  id: undefined as number | undefined,
  name: '',
  type: '',
  url: '',
  username: '',
  password: '',
  driverClassName: '',
  description: '',
  // Kafka特有配置
  securityProtocol: 'PLAINTEXT', // 默认使用PLAINTEXT
  saslMechanism: undefined as string | undefined // 初始不设置认证机制
})

const rules = reactive<FormRules>({
  name: [{ required: true, message: '请输入数据源名称', trigger: 'blur' }],
  type: [{ required: true, message: '请选择数据源类型', trigger: 'change' }],
  url: [{ required: true, message: '请输入连接URL', trigger: 'blur' }],
  username: [{ 
    required: true, 
    message: '请输入用户名', 
    trigger: 'blur',
    validator: (rule, value, callback) => {
      if (form.type === 'KAFKA' && !form.securityProtocol?.includes('SASL')) {
        callback() // 非SASL模式下用户名可选
      } else if (form.type === 'KAFKA' && form.securityProtocol?.includes('SASL') && !value) {
        callback(new Error('启用SASL认证时用户名为必填项'))
      } else if (form.type !== 'KAFKA' && !value) {
        callback(new Error('请输入用户名'))
      } else {
        callback()
      }
    }
  }],
  password: [{ 
    required: true, 
    message: '请输入密码', 
    trigger: 'blur',
    validator: (rule, value, callback) => {
      if (form.type === 'KAFKA' && !form.securityProtocol?.includes('SASL')) {
        callback() // 非SASL模式下密码可选
      } else if (form.type === 'KAFKA' && form.securityProtocol?.includes('SASL') && !value) {
        callback(new Error('启用SASL认证时密码为必填项'))
      } else if (form.type !== 'KAFKA' && !value) {
        callback(new Error('请输入密码'))
      } else {
        callback()
      }
    }
  }],
  securityProtocol: [{ 
    required: true, 
    message: '请选择安全协议', 
    trigger: 'change',
    validator: (rule, value, callback) => {
      if (form.type === 'KAFKA' && !value) {
        callback(new Error('请选择安全协议'))
      } else {
        callback()
      }
    }
  }],
  saslMechanism: [{ 
    required: true, 
    message: '请选择认证机制', 
    trigger: 'change',
    validator: (rule, value, callback) => {
      if (form.type === 'KAFKA' && form.securityProtocol?.includes('SASL') && !value) {
        callback(new Error('请选择认证机制'))
      } else {
        callback()
      }
    }
  }]
})

// 根据数据源类型设置驱动类名
const setDriverClassName = (type: string) => {
  switch (type) {
    case 'MYSQL':
      form.driverClassName = 'com.mysql.cj.jdbc.Driver'
      break
    case 'ORACLE':
      form.driverClassName = 'oracle.jdbc.OracleDriver'
      break
    case 'KAFKA':
      form.driverClassName = ''
      break
    default:
      form.driverClassName = ''
  }
}

interface DataSource {
  id: number
  name: string
  type: string
  url: string
  username: string
  password?: string
  driverClassName?: string
  description?: string
  createTime?: string
  securityProtocol?: string
  saslMechanism?: string
}

const dataSources = ref<DataSource[]>([])

const loadData = async () => {
  loading.value = true
  try {
    const response = await api.get('/data-sources/page', {
      params: {
        pageNum: currentPage.value,
        pageSize: pageSize.value
      }
    })
    dataSources.value = response.data.data.records
    total.value = response.data.data.total || 0
  } catch (error: any) {
    console.error('加载数据失败:', error)
    if (error.code === 'ERR_NETWORK' || error.message.includes('Network Error')) {
      ElMessage.error({
        message: '无法连接到后端服务(端口8888),请确保后端服务已启动',
        duration: 5000
      })
    } else {
      ElMessage.error('加载数据失败: ' + (error.message || '未知错误'))
    }
    dataSources.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

const handleAdd = () => {
  dialogType.value = 'add'
  Object.assign(form, {
    id: undefined,
    name: '',
    type: '',
    url: '',
    username: '',
    password: '',
    driverClassName: '',
    description: '',
    securityProtocol: 'PLAINTEXT',
    saslMechanism: undefined
  })
  dialogVisible.value = true
}

const handleEdit = (row: any) => {
  dialogType.value = 'edit'
  Object.assign(form, row)
  dialogVisible.value = true
}

const handleDelete = (row: DataSource) => {
  ElMessageBox.confirm('确认删除该数据源吗？', '提示', {
    type: 'warning'
  }).then(async () => {
    try {
      await api.delete(`/data-sources/${row.id}`)
      ElMessage.success('删除成功')
      loadData()
    } catch (error: any) {
      console.error('删除数据源失败:', error)
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

const handleTest = async (row: Partial<DataSource>) => {
  try {
    console.log('测试连接参数:', row) // 添加日志
    const response = await api.post('/data-sources/test', row, {
      headers: {
        'Content-Type': 'application/json'
      }
    })
    console.log('测试连接响应:', response) // 添加日志
    
    // 检查响应中是否包含错误信息
    if (response.data.message && response.data.message.includes('Access denied')) {
      console.error('数据库访问权限错误:', response.data.message)
      ElMessage.error(response.data.message)
      return
    }
    
    // 检查响应状态码和业务状态码
    if (response.status === 200 && response.data.code === 200) {
      ElMessage.success('连接测试成功')
    } else {
      // 如果后端返回了错误信息
      const errorMessage = response.data.message || '连接测试失败'
      console.error('连接测试失败:', errorMessage)
      ElMessage.error(errorMessage)
    }
  } catch (error: any) {
    console.error('测试连接错误详情:', error) // 添加详细错误日志
    // 从错误响应中提取具体的错误信息
    if (error.code === 'ERR_NETWORK' || error.message?.includes('Network Error')) {
      ElMessage.error({
        message: '无法连接到后端服务(端口8888),请确保后端服务已启动',
        duration: 5000
      })
    } else {
      const errorMessage = error.response?.data?.message || 
                          error.response?.data?.error || 
                          error.message || 
                          '连接测试失败'
      console.error('连接测试错误:', errorMessage)
      ElMessage.error(errorMessage)
    }
  }
}

const handleSubmit = async () => {
  if (!formRef.value) return
  await formRef.value.validate(async (valid) => {
    if (valid) {
      try {
        // 先测试连接
        const testResponse = await api.post('/data-sources/test', form)
        if (testResponse.status !== 200 || testResponse.data.code !== 200) {
          const errorMessage = testResponse.data.message || '连接测试失败'
          console.error('连接测试失败:', errorMessage)
          ElMessage.error(errorMessage)
          return
        }

        // 连接测试成功后保存数据源
        if (dialogType.value === 'add') {
          await api.post('/data-sources', form)
        } else {
          await api.put('/data-sources', form)
        }
        ElMessage.success(dialogType.value === 'add' ? '添加成功' : '更新成功')
        dialogVisible.value = false
        loadData()
      } catch (error: any) {
        // 从错误响应中提取具体的错误信息
        if (error.code === 'ERR_NETWORK' || error.message?.includes('Network Error')) {
          ElMessage.error({
            message: '无法连接到后端服务(端口8888),请确保后端服务已启动',
            duration: 5000
          })
        } else {
          const errorMessage = error.response?.data?.message || 
                             error.response?.data?.error || 
                             error.message || 
                             (dialogType.value === 'add' ? '添加失败' : '更新失败')
          console.error('保存数据源错误:', error) // 添加错误日志
          ElMessage.error(errorMessage)
        }
      }
    }
  })
}

// 监听类型变化
watch(() => form.type, (newType) => {
  setDriverClassName(newType)
})

// 监听安全协议变化
watch(() => form.securityProtocol, (newProtocol) => {
  if (!newProtocol?.includes('SASL')) {
    form.saslMechanism = undefined
    form.username = ''
    form.password = ''
  }
})

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
})
</script>

<style scoped>
.datasources-container {
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

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
}

.url-tips {
  margin-top: 8px;
  font-size: 12px;
  color: #909399;
}

.url-tips p {
  margin: 4px 0;
}

.field-tips {
  margin-top: 4px;
  font-size: 12px;
  color: #909399;
}

.field-tips p {
  margin: 2px 0;
}

.warning-tip {
  color: #E6A23C;
  font-weight: bold;
}
</style> 