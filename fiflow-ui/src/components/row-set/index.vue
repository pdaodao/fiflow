<template>
  <div class="sql-row-set">
    <el-table :data="tableData"
              border
              size="mini"
              stripe>

      <el-table-column v-for="column of columns"
                       :prop="column.prop"
                       :key="column.prop"
                       :label="column.label">

        <template slot-scope="scope">
          <span v-html="scope.row[column.prop]" />
        </template>

      </el-table-column>

    </el-table>

  </div>
</template>
<script>

export default {
  props: {
    rowset: {
      type: Object,
      default: () => {
        return {
          'heads': [],
          'rows': []
        }
      }
    }
  },
  data () {
    return {
      columns: [],
      tableData: [],
    }
  },
  beforeMount () {
    this.init()
  },
  mounted () {

  },
  methods: {
    init () {
      console.log("init", this.rowset)
      if (this.rowset && this.rowset.heads) {
        let index = 0;
        for (const h of this.rowset.heads) {
          this.columns.push({
            prop: 'p' + index++,
            label: h
          })
        }
        for (const row of this.rowset.rows) {
          index = 0;
          const r = {}
          for (const v of row) {
            r['p' + index++] = v.replace(/\n/gm, "<br/>")
          }
          this.tableData.push(r)
        }
      }
    },
  },
  beforeDestroy () {

  }
}
</script>
<style lang="scss">
.sql-row-set {
  margin-top: 5px;
  margin-bottom: 5px;
  .el-table__row {
    td {
      padding-top: 3px;
      padding-bottom: 3px;
    }
  }
}
</style>