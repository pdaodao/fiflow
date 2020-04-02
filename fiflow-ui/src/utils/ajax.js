import axios from 'axios'
import { Message } from 'element-ui'


const instance = axios.create({
    baseURL: '/fiflow',
    timeout: 30000,
    withCredentials: true,
    headers: { 'Content-Type': 'application/json;charset=UTF-8' },
})

instance.interceptors.request.use(config => {
    // 这里先不处理
    return config
})

// respponse 拦截器
instance.interceptors.response.use(
    response => {
        const status = response.status
        if (status != 200) {
            return Promise.reject(response.msg)
        }
        return response.data
    },
    error => {
        let message = error.message
        Message.error({ message: message || '后台接口异常，请联系开发处理！' })
    }
)

const ajax = {
    // get请求
    get (url, params) {
        const json = {
            url: url,
            method: 'get'
        }
        if (params) {
            json.params = params
        }
        return instance(json)
    },
    // post 请求
    post (url, params) {
        return instance.post(url, params)
    },
    delete (url, params) {
        const json = {
            url: url,
            method: 'delete'
        }
        if (params) {
            json.params = params
        }
        return instance(json)
    }
}

export default ajax

