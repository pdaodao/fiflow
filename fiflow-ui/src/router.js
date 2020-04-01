import VueRouter from 'vue-router';
import Layout from './layout.vue';
const router = new VueRouter({
    routes: [
        {
            path: '',
            name: 'layout',
            redirect: '/sql',
            component: Layout,
            meta: {
                title: 'fiflow'
            },
            children: [
                {
                    path: 'sql',
                    name: 'sql',
                    meta: {
                        title: 'fisql',
                        keepAlive: true, // 需要被缓存
                        icon: 'ios-at'
                    },
                    component: () => import('./views/fisql/index.vue'),
                },
                {
                    path: 'flow',
                    name: 'flow',
                    meta: {
                        title: '数据流',
                        keepAlive: true, // 需要被缓存
                        icon: 'md-analytics'
                    },
                    component: () => import('./views/flow/index.vue'),
                },
                {
                    path: 'config',
                    name: 'config',
                    meta: {
                        title: '管理',
                        icon: 'ios-construct'
                    },
                    component: () => import('./views/config/index.vue'),
                    children: [{
                        name: 'flink',
                        path: 'flink',
                        component: () => import('./views/flink/index.vue'),
                        meta: {
                            title: 'flink集群',
                            icon: 'md-cloud-outline'
                        }
                    },
                    {
                        name: 'datasource',
                        path: 'datasource',
                        component: () => import('./views/datasource/index.vue'),
                        meta: {
                            title: '数据源',
                            icon: 'ios-flower'
                        }
                    },
                    {
                        name: 'monitor',
                        path: 'monitor',
                        component: () => import('./views/flink/index.vue'),
                        meta: {
                            title: '监控',
                            icon: 'ios-help-buoy'
                        }
                    }
                    ]
                },
            ],
        }
    ],
});
router.beforeEach((to, from, next) => {
    // console.log(to, from)
    next()
})

router.afterEach((to) => {
    // console.log(from)
    console.log('访问：', to.fullPath);
    if (to.meta) {
        document.title = to.meta.title || 'fiflow';
    }
})

export default router
    ;
