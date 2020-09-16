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
                        icon: 's-promotion'
                    },
                    component: () => import('./views/fisql/index.vue'),
                },
                // {
                //     path: 'flow',
                //     name: '/flow',
                //     meta: {
                //         title: '数据流',
                //         keepAlive: true, // 需要被缓存
                //         icon: 'ship'
                //     },
                //     component: () => import('./views/flow/index.vue'),
                // },
                {
                    path: 'link',
                    name: 'link',
                    meta: {
                        title: '连接',
                        icon: 'link'
                    },
                    component: () => import('./views/config/index.vue'),
                    children: [{
                        name: '/link/cluster',
                        path: 'cluster',
                        component: () => import('./views/cluster/index.vue'),
                        meta: {
                            title: 'flink集群',
                            icon: 'cloudy'
                        }
                    },
                    {
                        name: '/link/connector',
                        path: 'connector',
                        component: () => import('./views/connector/index.vue'),
                        meta: {
                            title: '数据连接',
                            icon: 'guide'
                        }
                    }]
                },
                {
                    path: 'meta',
                    name: 'meta',
                    meta: {
                        title: '元信息',
                        icon: 'wind-power'
                    },
                    component: () => import('./views/config/index.vue'),
                    children: []
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
