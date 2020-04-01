<template>
  <div class="fisql-main">

    <div class="maintoolbar">
      <div class="container">
        <div class="toolbar">
          <Button size="small">保存</Button>

          <Button icon="md-add"
                  size="small"
                  @click="add">
            添加
          </Button>

          <Button icon="ios-play"
                  @click="run"
                  size="small">
            运行
          </Button>

        </div>
      </div>
    </div>

    <div class="notebook_panel">
      <div class="notebook">
        <div class="container">
          <sql-cell v-for="item of sqlList"
                    :key="item.id"
                    :ref="'cell'+item.id"
                    @click="setCurrent(item.id)"
                    :class="{active: item.id == currentId}"
                    :sessionId="sessionId"
                    :info="item" />
        </div>
        <div class="end_space"></div>
      </div>
    </div>

  </div>

</template>

<script>

import SqlCell from './cell.vue'

export default {
  name: 'fisql',
  components: {
    SqlCell
  },
  data () {
    return {
      sqlList: [],
      sessionId: null,
      currentId: '',
      line: 0,
    }
  },
  mounted () {
    this.add()
  },
  methods: {
    setSessionId (id) {
      this.sessionId = id
    },
    setCurrent (id) {
      this.currentId = id
    },
    add () {
      const item = {
        id: this.line++,
        sql: '',
      }
      this.sqlList.push(item)
    },
    run () {
      let block = this.$refs['cell' + this.currentId]
      if (block) block = block[0]
      if (this.currentId != null && block != null) {
        console.log(block)
        block.runSql()
      } else {
        this.$Notice.warning({
          title: '执行失败',
          desc: '请选择需要执行的sql',
          duration: 10,
        });
      }
    }
  }
}
</script>


<style lang="scss">
.fisql-main {
  .toolbar {
    display: inline-block;
    background-color: #e9e9e9;
    margin-top: 2px;
    .ivu-btn {
      margin-right: 9px;
    }
  }
  .notebook_panel {
    height: 100%;
    overflow: auto;
    height: 100vh;
  }
  .notebook {
    font-size: 14px;
    line-height: 20px;
    overflow-y: hidden;
    overflow-x: auto;
    width: 100%;
    padding-top: 20px;
    margin: 0px;
    outline: none;
    box-sizing: border-box;
    -moz-box-sizing: border-box;
    -webkit-box-sizing: border-box;
    min-height: 100%;
    .container {
      padding: 15px;
      background-color: #fff;
      min-height: 0;
      -webkit-box-shadow: 0px 0px 12px 1px rgba(87, 87, 87, 0.2);
      box-shadow: 0px 0px 12px 1px rgba(87, 87, 87, 0.2);
    }
    .end_space {
      min-height: 100px;
      transition: height 0.2s ease;
    }
  } // .notebook
}
</style>