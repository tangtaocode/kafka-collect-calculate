package tang.tao.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import tang.tao.websocket.WebSocketServer;

import java.io.IOException;

@Controller
public class PushController {
    @Autowired
    private tang.tao.websocket.WebSocketServer webSocketServer;

    @RequestMapping("/push")
    @ResponseBody
    public void pushClient() throws IOException {
        webSocketServer.sendInfo("haha","hello");
    }
}
