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
                    name: '/sql',
                    meta: {
                        title: 'fisql',
                        keepAlive: true, // 需要被缓存
                        icon: 'medal'
                    },
                    component: () => import('./views/fisql/index.vue'),
                },
                {
                    path: 'flow',
                    name: '/flow',
                    meta: {
                        title: '数据流',
                        keepAlive: true, // 需要被缓存
                        icon: 'ship'
                    },
                    component: () => import('./views/flow/index.vue'),
                },
                {
                    path: 'config',
                    name: 'config',
                    meta: {
                        title: '管理',
                        icon: 'setting'
                    },
                    component: () => import('./views/config/index.vue'),
                    children: [{
                        name: '/config/flink',
                        path: 'flink',
                        component: () => import('./views/flink/index.vue'),
                        meta: {
                            title: 'flink集群',
                            icon: 's-promotion'
                        }
                    },
                    {
                        name: '/config/datasource',
                        path: 'datasource',
                        component: () => import('./views/datasource/index.vue'),
                        meta: {
                            title: '数据源',
                            icon: 'coin'
                        }
                    },
                    {
                        name: '/config/monitor',
                        path: 'monitor',
                        component: () => import('./views/monitor/index.vue'),
                        meta: {
                            title: '监控',
                            icon: 'attract'
                        }
                    }
                    ]
                },
            ],
        }
    ],
});
router.beforeEach((to, from, next) => {
    next()
})

router.afterEach((to) => {
    // console.log(from)
    if (to.meta) {
        document.title = to.meta.title || 'fiflow'
    }
})

export default router
    ;
