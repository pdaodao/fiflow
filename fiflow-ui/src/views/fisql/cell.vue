<template>
  <div class="fisql-cell"
       v-loading="loading"
       :class="{'edit': edit}"
       @mouseleave="edit=false"
       @click="setCurrent">

    <div class="input">
      <div class="input-left">
        <span style="font-weight:bold;">
          In [{{info.id}}]:
        </span>
      </div>
      <div class="input-right"
           @click="edit=true">

        <editor v-model="info.sql" />

      </div>
    </div>

    <div class="result">
      <el-collapse accordion
                   v-model="openResult">
        <el-collapse-item name="open">
          <template slot="title">
            运行结果 <i class="el-icon-message-solid"></i>
          </template>
          <div>
            <el-tabs tab-position="left"
                     v-model="activeName">
              <el-tab-pane label="信息"
                           name="msg">
                <div>
                  <p v-for="(msg, index) of msgs"
                     :key="'m'+index">
                    {{msg}}
                  </p>
                </div>
              </el-tab-pane>
              <el-tab-pane label="输出"
                           name="out">
                <div>
                  <row-set v-for="(item, index) of tables"
                           :key="'rs'+index"
                           :rowset="item" />
                </div>
              </el-tab-pane>
            </el-tabs>
          </div>
        </el-collapse-item>
      </el-collapse>
    </div>

  </div>
</template>
<script>
import ajax from '@/utils/ajax.js'
import editor from '@/components/sql-editor'
import RowSet from '@/components/row-set'
export default {
  components: {
    editor,
    RowSet
  },
  props: {
    sessionId: {
      type: String,
      default: ''
    },
    info: {
      type: Object,
      default: () => {
        return {
          'id': 1,
          'sql': ''
        }
      }
    }
  },
  data () {
    return {
      activeName: 'msg',   // 信息 / 输出 
      edit: false,         // 是否编辑状态 
      tables: [],
      msgs: [],
      openResult: "",
      loading: false,
    }
  },

  mounted () {

  },
  methods: {
    // 激活当前cell   
    setCurrent () {
      this.$parent.setCurrent(this.info.id)
    },
    // 运行 sql   
    runSql () {
      if (this.loading == true)
        return
      const param = {
        sessionId: this.sessionId,
        sql: this.info.sql
      }
      this.loading = true
      ajax.post("/fisql/run", param).then(d => {
        this.msgs = []
        this.tables = []
        console.log(d)
        this.msgs = d.msgs
        if (d.table) {
          this.tables.push(d.table)
          this.activeName = 'out'
        } else {
          this.activeName = 'msg'
        }
        this.openResult = "open"
        this.$parent.setSessionId(d.sessionId)
      }).finally(() => {
        this.loading = false
      })
    },
  }
}
</script>

<style lang="scss">
.fisql-cell {
  padding: 5px;
  border-width: 1px;
  border-style: solid;
  border-color: transparent;
  position: relative;

  &.active {
    border-color: #ababab;
  }

  &.active.edit {
    border-color: #66bb6a;
  }

  &.active:before {
    position: absolute;
    display: block;
    top: -1px;
    left: -1px;
    width: 5px;
    height: calc(100% + 2px);
    content: "";
    background: #42a5f5;
  }
  &.active.edit:before {
    background: #66bb6a;
  }

  .input {
    display: flex;
    flex-direction: row;
    align-items: stretch;
    .input-left {
      color: #303f9f;
      border-top: 1px solid transparent;
      width: 88px;
      padding: 0.4em;
      margin: 0px;
      font-family: monospace;
      text-align: right;
    }
    .input-right {
      flex: 1;
    }
  }
  .result {
    margin-bottom: -5px;
    .el-collapse {
      border: none;
    }
    p {
      margin-top: 2px;
      margin-bottom: 2px;
    }
    .el-collapse-item__wrap {
      border: none;
      padding-left: 22px;
    }
    .el-collapse-item__content {
      padding-bottom: 0px;
      border-right: 1px solid #eee;
    }
    .el-collapse-item__header {
      height: 28px;
      line-height: 28px;
      padding: 0.4em;
      margin-left: 88px;
      background-color: #eee;
      margin-top: -1px;
    }
  }
}
</style>