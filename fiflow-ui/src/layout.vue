<template>
  <div class="layout-body">

    <layout-header />

    <div class="layout-main">
      <keep-alive>
        <router-view :key="$route.path" />
      </keep-alive>
      <!-- <router-view v-if="!$route.meta.keepAlive " /> -->
    </div>

  </div>
</template>
<script>
import header from './views/layout/header'

export default {
  components: {
    layoutHeader: header.component
  },
}
</script>