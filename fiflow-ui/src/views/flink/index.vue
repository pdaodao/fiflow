<template>
  <div>
    <el-row style="margin-bottom:7px;">
      <el-col :span="3">
        <el-button type="primary"
                   size="mini"
                   @click="add"
                   icon="el-icon-plus">
          添加
        </el-button>
      </el-col>
      <el-col :span="6"
              :push="13">

      </el-col>
    </el-row>
    <el-row>
      <el-table :data="tableData"
                stripe>
        <el-table-column prop="code"
                         label="名称">
        </el-table-column>
        <el-table-column prop="desc"
                         label="描述">
        </el-table-column>
        <el-table-column prop="type"
                         label="类型">
        </el-table-column>
        <el-table-column label="地址">
          <template slot-scope="scope">
            <span>{{scope.row.host}} </span>
            <span v-if="scope.row.host && scope.row.port">:</span>
            <span> {{scope.row.port}}</span>
          </template>
        </el-table-column>

        <el-table-column label="操作"
                         width="180">
          <template slot-scope="scope">
            <el-button size="mini"
                       @click="handleEdit(scope.$index, scope.row)">编辑</el-button>
            <el-button size="mini"
                       type="danger"
                       @click="handleDelete(scope.$index, scope.row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-row>

    <el-dialog title="flink 集群信息"
               width="60%"
               :visible.sync="dialogVisible">
      <div>
        <el-form ref="form"
                 :model="form"
                 label-width="80px">
          <el-form-item label="集群名称">
            <el-input v-model="form.code"
                      placeholder="集群名称英文"></el-input>
          </el-form-item>

          <el-form-item label="描述">
            <el-input v-model="form.desc"
                      placeholder="集群的描述名称"></el-input>
          </el-form-item>

          <el-form-item label="运行模式">
            <el-select v-model="form.model"
                       placeholder="请选择运行模式">
              <el-option label="local"
                         value="local"></el-option>
              <el-option label="standalone"
                         value="standalone"></el-option>
              <el-option label="yarn todo"
                         value="yarn"></el-option>
              <el-option label="yarnper todo"
                         value="yarnper"></el-option>
              <el-option label="k8s todo"
                         value="k8s"></el-option>
            </el-select>
          </el-form-item>

          <el-form-item label="连接信息">
            <el-col :span="12">
              <el-input v-model="form.host"
                        placeholder="地址"></el-input>
            </el-col>

            <el-col :span="3"
                    style="text-align:right;padding-right:9px;">端口</el-col>
            <el-col :span="9">
              <el-input-number v-model="form.port"
                               placeholder="端口"></el-input-number>
            </el-col>
          </el-form-item>

          <el-form-item label="高级配置">
            <el-input type="textarea"
                      v-model="form.desc"></el-input>
          </el-form-item>

          <el-form-item>
            <el-button type="primary"
                       v-if="form.id">保存信息</el-button>
            <el-button type="primary"
                       v-else>立即创建</el-button>
            <el-button @click="dialogVisible=false">取消</el-button>
          </el-form-item>
        </el-form>
      </div>
    </el-dialog>

  </div>
</template>

<script>
export default {
  data () {
    return {
      dialogVisible: false,
      form: {
        id: null,  // 主键 
        code: '',  // 英文名称 唯一code 方便使用
        desc: '',  // 描述
        model: '', // 运行模式 
        host: '',
        port: '',
      },
      tableData: [{
        id: '1',
        code: 'local',
        desc: '本地测试',
        type: 'local',
        host: '',
        port: '',
      }
      ]
    }
  },
  mounted () {

  },
  methods: {
    add () {
      this.dialogVisible = true
    },
    handleEdit () {

    },
    handleDelete () {

    }
  }
}
</script>