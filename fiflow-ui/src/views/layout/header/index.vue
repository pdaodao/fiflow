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

      <Menu mode="horizontal"
            theme="dark"
            :active-name="active">

        <li v-for="item of routes"
            :name="item.name"
            :key="item.name"
            :to="item"
            :is="item.children ? 'Submenu' :'MenuItem'">

          <template v-if="item.children">
            <template slot="title">
              <Icon v-if="item.meta.icon"
                    :type="item.meta.icon" />
              {{item.meta.title}}
            </template>
          </template>
          <template v-else>
            <Icon v-if="item.meta.icon"
                  :type="item.meta.icon" />
            {{item.meta.title}}
          </template>

          <MenuItem v-for="sub of item.children"
                    :name="sub.name"
                    :to="sub"
                    :key="sub.name">
          <Icon v-if="sub.meta.icon"
                :type="sub.meta.icon" />
          {{sub.meta.title}}
          </MenuItem>

        </li>

      </Menu>
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
    console.log("routes", this.routes)
    const name = this.$route.name
    this.active = name
  },
  methods: {

  }
}

</script>
<style lang="scss" src="./index.scss"></style>
