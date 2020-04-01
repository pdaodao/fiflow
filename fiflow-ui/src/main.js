import Vue from 'vue'
import VueRouter from 'vue-router'
import router from './router.js'
import ViewUI from 'view-design'
import 'view-design/dist/styles/iview.css'
import App from './App.vue'

Vue.use(VueRouter)
Vue.use(ViewUI)

Vue.config.productionTip = false


new Vue({
    el: '#app',
    router: router,
    render: h => h(App)
});
