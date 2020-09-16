import Vue from 'vue'
import VueRouter from 'vue-router'
import router from './router.js'
import ElementUI from 'element-ui';
import App from './App.vue'
import 'element-ui/lib/theme-chalk/index.css';

Vue.use(VueRouter)
Vue.use(ElementUI, { size: 'small' })

Vue.config.productionTip = false


new Vue({
    el: '#app',
    router: router,
    render: h => h(App)
});
