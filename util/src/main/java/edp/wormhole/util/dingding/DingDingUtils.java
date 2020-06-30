package edp.wormhole.util.dingding;

import com.alibaba.fastjson.JSON;
import edp.wormhole.util.httpclient.HttpClientService;
import org.apache.http.entity.ContentType;

import java.util.HashMap;
import java.util.Map;

public class DingDingUtils {

	public static void sendMessage(String message, String url, String title) throws Exception {
		try {
			Map<String, Object> data = getHttpBodyMd(message, title);
			String body = JSON.toJSONString(data);
			HttpClientService httpClientService = new HttpClientService();
			httpClientService.doPost(url, null, body, ContentType.APPLICATION_JSON);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static Map<String, Object> getHttpBodyMd(String message, String title) {
        Map<String, String> content = new HashMap<>();
        content.put("text", message);
        content.put("title", title);
        Map<String, Object> map = new HashMap<>();
        map.put ("msgtype", "markdown");
        map.put ("markdown", content);
        return map;
    }

}
