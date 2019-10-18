package edp.wormhole.util.httpclient;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpClientService {

    static Logger logger = Logger.getLogger(HttpClientService.class);

    public HttpResult doGet(CloseableHttpClient httpClient, String url, Map<String, String> header) throws Exception {
        HttpGet httpGet = new HttpGet(url);
        httpGet.addHeader("Connection", "close");
        if (header != null) {
            header.forEach((k, v) -> {
                httpGet.setHeader(k, v);
            });
        }

        CloseableHttpResponse response = null;
        response = httpClient.execute(httpGet);
        logger.info("status:" + response.getStatusLine().getStatusCode());
        String data = null;
        if (response.getStatusLine().getStatusCode() == 200) {
            if (response.getEntity() != null)
                data = EntityUtils.toString(response.getEntity(), "UTF-8");
            else data = null;
        }
        try {
            response.close();
        } catch (Exception e) {
            logger.warn("", e);
        }
        return new HttpResult(response.getStatusLine().getStatusCode(), data);
    }

    private HttpResult doGetCommon(String url, Map<String, String> headerMap, Map<String, String> params, boolean ssl) throws Exception {
        CloseableHttpClient httpClient = null;
        HttpResult hr = null;
        try {
            URIBuilder uriBuilder = new URIBuilder(url);
            if (params != null)
                for (String key : params.keySet()) {
                    uriBuilder.addParameter(key, params.get(key));
                }
            httpClient = getCloseableHttpClient(ssl);
            hr = doGet(httpClient, uriBuilder.build().toString(), headerMap);
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        } finally {
            if (httpClient != null) {
                try {
                    httpClient.close();
                } catch (Exception e) {
                    logger.warn("close http", e);
                }
            }
        }
        return hr;
    }

    public HttpResult doGet(String url, Map<String, String> headerMap, Map<String, String> params) throws Exception {
        return doGetCommon(url, headerMap, params, false);
    }

    private CloseableHttpClient getCloseableHttpClient(boolean ssl) {
        if (ssl) {
            return createSslDefault();
        } else {
            return HttpClients.createDefault();
        }
    }

    public HttpResult doSslGet(String url, Map<String, String> headerMap, Map<String, String> params) throws Exception {
        return doGetCommon(url, headerMap, params, true);
    }

    public HttpResult doSslGet(CloseableHttpClient httpClient, String url, Map<String, String> headerMap, Map<String, String> params) throws Exception {
        URIBuilder uriBuilder = new URIBuilder(url);
        if (params != null)
            for (String key : params.keySet()) {
                uriBuilder.addParameter(key, params.get(key));
            }
        return doGet(httpClient, uriBuilder.build().toString(), headerMap);
    }

    public HttpResult doPost(CloseableHttpClient httpClient, String url, Map<String, String> header,
                             Map<String, String> params) throws Exception {
        // 创建http POST请求
        HttpPost httpPost = new HttpPost(url);
        httpPost.addHeader("Connection", "close");
        if (header != null) {
            header.forEach((k, v) -> {
                httpPost.setHeader(k, v);
            });
        }

        if (params != null) {
            // 设置2个post参数，一个是scope、一个是q
            List<NameValuePair> parameters = new ArrayList<NameValuePair>();
            for (String key : params.keySet()) {
                parameters.add(new BasicNameValuePair(key, params.get(key)));
            }
            // 构造一个form表单式的实体
            UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(parameters, "UTF-8");
            // 将请求实体设置到httpPost对象中
            httpPost.setEntity(formEntity);
        }

        CloseableHttpResponse response = null;
        response = httpClient.execute(httpPost);
        HttpResult hr = new HttpResult(response.getStatusLine().getStatusCode(),
                EntityUtils.toString(response.getEntity(), "UTF-8"));

        try {
            response.close();
        } catch (Exception e) {
            logger.warn("", e);
        }

        return hr;
    }

    public HttpResult doPost(CloseableHttpClient httpClient, String url, Map<String, String> header, String content,
                             ContentType contentType) throws Exception {
        // 创建http POST请求
        HttpPost httpPost = new HttpPost(url);
        httpPost.addHeader("Connection", "close");
        if (header != null) {
            header.forEach((k, v) -> {
                httpPost.setHeader(k, v);
            });
        }

        if (content != null) {
            // 构造一个form表单式的实体
            StringEntity stringEntity = new StringEntity(content, contentType);
            // 将请求实体设置到httpPost对象中
            httpPost.setEntity(stringEntity);
        }

        CloseableHttpResponse response = null;
        // 执行请求
        response = httpClient.execute(httpPost);
        HttpResult hr = new HttpResult(response.getStatusLine().getStatusCode(),
                EntityUtils.toString(response.getEntity(), "UTF-8"));

        try {
            response.close();
        } catch (Exception e) {
            logger.warn("", e);
        }

        return hr;
    }

    private HttpResult doPostCommon(String url, Map<String, String> headerMap, String content, ContentType contentType, boolean ssl) throws Exception {
        CloseableHttpClient httpClient = null;
        HttpResult hr = null;
        try {
            httpClient = getCloseableHttpClient(ssl);
            hr = doPost(httpClient, url, headerMap, content, contentType);
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        } finally {
            if (httpClient != null) {
                try {
                    httpClient.close();
                } catch (Exception e) {
                    logger.warn("close http", e);
                }
            }
        }
        return hr;
    }

    private HttpResult doPostCommon(String url, Map<String, String> headerMap, Map<String, String> params, boolean ssl) throws Exception {
        CloseableHttpClient httpClient = null;
        HttpResult hr = null;
        try {
            httpClient = getCloseableHttpClient(ssl);
            hr = doPost(httpClient, url, headerMap, params);
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        } finally {
            if (httpClient != null) {
                try {
                    httpClient.close();
                } catch (Exception e) {
                    logger.warn("close http", e);
                }
            }
        }
        return hr;
    }

    public HttpResult doPost(String url, Map<String, String> header, String content, ContentType contentType) throws Exception {
        return doPostCommon(url, header, content, contentType, false);
    }

    public HttpResult doPost(String url, Map<String, String> header, Map<String, String> params) throws Exception {
        return doPostCommon(url, header, params, false);
    }


    public CloseableHttpClient createSslDefault() {
        SSLContext sslContext = SSLContexts.createDefault();
        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);

        CloseableHttpClient client = HttpClients.custom().setSSLSocketFactory(sslsf).build();

        return client;
    }

    public HttpResult doSslPost(String url, Map<String, String> header, String content, ContentType contentType) throws Exception {
        return doPostCommon(url, header, content, contentType, true);
    }

    public HttpResult doSslPost(String url, Map<String, String> header, Map<String, String> params) throws Exception {
        return doPostCommon(url, header, params, true);
    }

}
