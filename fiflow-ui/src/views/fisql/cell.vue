<template>
  <div class="fisql-cell"
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
      查询结果

    </div>

  </div>
</template>
<script>
import ajax from '@/utils/ajax.js'
import editor from '@/components/sql-editor'
export default {
  components: {
    editor
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
      edit: false,
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
      console.log("run")
      const param = {
        sessionId: this.sessionId,
        sql: this.info.sql
      }
      ajax.post("/fisql/run", param).then(d => {
        console.log(d)
        this.$parent.setSessionId(d.sessionId)
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

  .input {
    display: flex;
    flex-direction: row;
    align-items: stretch;
    .input-left {
      color: #303f9f;
      border-top: 1px solid transparent;
      min-width: 14ex;
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
    color: red;
  }

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
}
</style>