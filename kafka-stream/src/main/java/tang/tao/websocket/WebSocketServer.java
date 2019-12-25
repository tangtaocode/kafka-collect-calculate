package tang.tao.websocket;

import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;
import org.springframework.web.socket.config.annotation.EnableWebSocket;

import javax.websocket.*;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArraySet;

@Component
@ServerEndpoint("/chart/{type}")
public class WebSocketServer {
    //客户端存储
    private static CopyOnWriteArraySet<Session> sessionSet = new CopyOnWriteArraySet<Session>();

    //接收type
    private String type="";


    /**
     * 连接建立成功调用的方法*/
    @OnOpen
    public void onOpen(Session session,@PathParam("type") String type) {
        sessionSet.add(session);     //加入set中
        this.type=type;
        try {
            sendMessage(session,"连接成功");
        } catch (IOException e) {
          e.printStackTrace();
        }
    }
    /**
     * 连接关闭调用的方法
     */
    @OnClose
    public void onClose() {
        sessionSet.remove(this);  //从set中删除
    }

    /**
     * 收到客户端消息后调用的方法
     *
     * @param message 客户端发送过来的消息*/
    @OnMessage
    public void onMessage(String message, Session session) {
      System.out.println("客户端>>"+message);
    }

    /**
     *
     * @param session
     * @param error
     */
    @OnError
    public void onError(Session session, Throwable error) {
        error.printStackTrace();
    }
    /**
     * 实现服务器主动推送
     */
    public void sendMessage(Session session,String message) throws IOException {
        session.getBasicRemote().sendText(message);
    }


    /**
     * 群发自定义消息
     * */
    public  void sendInfo(String message,String type) throws IOException {
        for (Session item : sessionSet) {
            try {
                if(!item.isOpen()){
                    sessionSet.remove(item);
                    continue;
                }
                //这里可以设定只推送给这个sid的，为null则全部推送
                if(ObjectUtils.isEmpty(type)) {
                    item.getBasicRemote().sendText(message);
                }else if(type.equals(type)){
                    item.getBasicRemote().sendText(message);
                }
            } catch (IOException e) {
                continue;
            }
        }
    }
}
