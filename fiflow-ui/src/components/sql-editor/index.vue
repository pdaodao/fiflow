<template>
  <div ref='editor'
       :style="{'height':height+'px'}"
       class="sql-editor">
  </div>
</template>
<script>
import * as monaco from 'monaco-editor'

export default {
  data () {
    return {
      sqlEditor: null,
      height: 36,
      lineHeight: 18,
      maxHeight: null,
    }
  },
  beforeMount () {

  },
  mounted () {
    this.initEditor()
  },
  methods: {
    initEditor () {
      console.log("init editor")
      this.sqlEditor = monaco.editor.create(this.$refs.editor, {
        value: this.input,
        selectOnLineNumbers: true,
        roundedSelection: false,
        automaticLayout: true, //自动布局
        glyphMargin: true,  //字形边缘
        useTabStops: false,
        minimap: false,
        language: 'sql'  // 这里以sql为例
      })
      const that = this
      this.sqlEditor.onDidChangeModelContent(function () {
        that.$emit("input", that.sqlEditor.getValue())
        that.adjustHeight()
      })
    },
    adjustHeight () {
      this.$nextTick(function () {
        const pos = this.sqlEditor.getPosition()
        const line = pos.lineNumber + 1
        if (line > 2) {
          let h = line * this.lineHeight + 10
          if (this.maxHeight != null && this.maxHeight > 100 && h > this.maxHeight) h = this.maxHeight
          this.height = h
        }
      })
    }
  },
  beforeDestroy () {
    this.sqlEditor.dispose()
  }
}
</script>
<style lang="scss">
.sql-editor {
  border: 1px solid #cfcfcf;
  border-radius: 2px;
  background: #eee;
  width: 100%;
  position: relative;
  resize: vertical;
}
</style>