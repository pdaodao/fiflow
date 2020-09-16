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
      <el-col :span="3"
              :push="20">
        类型
      </el-col>

    </el-row>

  </div>
</template>
<script>
export default {
  data () {
    return {

    }
  },
  methods: {

  }
}
</script>