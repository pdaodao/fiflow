package com.github.myetl.fiflow.core.frame;


import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 会话管理器
 */
public final class FlowSessionManager {

    static class SessionWrap{
        final FlowSession session;
        final long createTime;
        long accessTime;
        int count;

        public SessionWrap(FlowSession session) {
            this.session = session;
            createTime = System.currentTimeMillis();
        }

        public void updateAccessTime(){
            accessTime = System.currentTimeMillis();
            count ++;
        }
    }

    final static ConcurrentHashMap<String, SessionWrap> sessionMap = new ConcurrentHashMap<>();

    /**
     * 创建或者获取 session
     * @param id                 session id
     * @param sessionConfig      session 配置信息
     * @return
     */
    public static synchronized FlowSession getOrCreateSession(String id, SessionConfig sessionConfig) {
        if(StringUtils.isEmpty(id)){
            for(int i = 0; i < 30; i++){
                id = RandomStringUtils.randomAlphanumeric(5);
                if(!sessionMap.containsKey(id)) break;
            }
        }
        SessionWrap wrap = sessionMap.get(id);
        if(wrap == null){
            FlowSession flowSession = new FlowSession(id, sessionConfig);
            wrap = new SessionWrap(flowSession);
            sessionMap.putIfAbsent(id,  wrap);
        }
        wrap.updateAccessTime();
        return wrap.session;
    }

}
