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
        <el-table-column prop="name"
                         label="名称">
        </el-table-column>
        <el-table-column prop="comment"
                         label="描述">
        </el-table-column>
        <el-table-column prop="clusterType"
                         label="集群类型">
        </el-table-column>
        <el-table-column label="地址">
          <template slot-scope="scope">
            <span>{{scope.row.host}} </span>
            <span v-if="scope.row.host && scope.row.port">:
              {{scope.row.port}}
            </span>
          </template>
        </el-table-column>

        <el-table-column label="信息"
                         width="180">
          <template>
            概况 任务
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
            <el-input v-model="form.name"
                      placeholder="集群名称英文"></el-input>
          </el-form-item>

          <el-form-item label="描述">
            <el-input v-model="form.comment"
                      placeholder="集群的描述名称"></el-input>
          </el-form-item>

          <el-form-item label="运行模式">
            <el-col :span="12">
              <el-select v-model="form.clusterType"
                         placeholder="请选择运行模式">
                <el-option label="local"
                           value="local"></el-option>
                <el-option label="standalone"
                           value="standalone"></el-option>
                <el-option label="yarnper todo"
                           value="peryarn"></el-option>
                <el-option label="yarn todo"
                           value="yarn"></el-option>
                <el-option label="k8s todo"
                           value="k8s"></el-option>
              </el-select>
            </el-col>
            <el-col :span="11"
                    :offset="1">
              插槽数
              <el-input-number v-model="form.slots"
                               placeholder="插槽数"></el-input-number>
            </el-col>
          </el-form-item>

          <el-form-item label="连接地址">
            <el-col :span="12">
              <el-input v-model="form.host"
                        placeholder="地址"></el-input>
            </el-col>
            <el-col :span="11"
                    :offset="1">
              端口号
              <el-input-number v-model="form.port"
                               placeholder="端口"></el-input-number>
            </el-col>
          </el-form-item>

          <el-form-item label="依赖jar包">
            <el-input type="textarea"
                      v-model="form.dependJars"></el-input>
          </el-form-item>

          <el-form-item style="text-align:center;">
            <el-button type="primary"
                       @click="save">保存信息</el-button>
            <el-button @click="dialogVisible=false">取消</el-button>
          </el-form-item>
        </el-form>
      </div>
    </el-dialog>

  </div>
</template>

<script>
import ajax from '@/utils/ajax.js'

export default {
  data () {
    return {
      dialogVisible: false,
      content: {
        id: null,  // 主键 
        name: '',  // 英文名称 唯一code 方便使用
        comment: '',  // 描述
        clusterType: '', // 运行模式 
        host: '',
        port: 8080,
        slots: 3,
        dependJars: '',
      },
      form: { ...this.content },
      tableData: []
    }
  },
  mounted () {
    this.load()
  },
  methods: {
    load () {
      ajax.get("/cluster", {}).then(d => {
        this.tableData = d
      })
    },
    add () {
      this.dialogVisible = true
      this.form = { ...this.content }
    },
    save () {
      ajax.post("/cluster", this.form).then(d => {
        console.log("cluster .... save", d)
        this.load()
        this.dialogVisible = false
      })
    },
    handleEdit (index, row) {
      const id = row.id
      ajax.get("/cluster/" + id, {}).then(d => {
        this.dialogVisible = true
        this.form = d
      })
    },
    handleDelete () {

    }
  }
}
</script>