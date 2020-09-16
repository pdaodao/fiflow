<template>
  <div class="layout-header">
    <div class=" container">

      <div class="logo">
        <span class="logo-main-titile">fiflow</span>
        <span class="logo-sub-title">
          <span>-> 数据流处理 -></span>
          <span class="logo-sub-title-main"></span>
        </span>
      </div>

      <el-menu :default-active="active"
               router
               background-color="#545c64"
               text-color="#f3f3f3"
               active-text-color="#2d8cf0"
               mode="horizontal">

        <li v-for="item of routes"
            :index="item.name"
            :key="item.name"
            :is="item.children ? 'el-submenu' :'el-menu-item'">

          <template v-if="item.children"
                    slot="title">
            <i v-if='item.meta.icon'
               :class="'el-icon-'+item.meta.icon" />{{item.meta.title}}
          </template>
          <template v-else>
            <i v-if='item.meta.icon'
               :class="'el-icon-'+item.meta.icon" />{{item.meta.title}}
          </template>

          <el-menu-item v-for="sub of item.children"
                        :index="sub.name"
                        :key="sub.name">
            <i v-if="sub.meta.icon"
               :class="'el-icon-'+sub.meta.icon" />
            {{sub.meta.title}}
          </el-menu-item>
        </li>

      </el-menu>

    </div>
  </div>
</template>
<script>

export default {
  data () {
    return {
      active: '',
      routes: [],
    }
  },
  mounted () {
    this.routes = this.$router.options.routes[0].children
    this.active = this.$route.name
  },
  methods: {

  }
}

</script>
<style lang="scss" src="./index.scss"></style>
