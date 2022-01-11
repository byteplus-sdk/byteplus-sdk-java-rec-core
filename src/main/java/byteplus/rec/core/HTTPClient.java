package byteplus.rec.core;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class HTTPClient {
    private HTTPCaller httpCaller;
    private HostAvailabler hostAvailabler;
    private String schema;

    public <Rsp extends Message, Req extends Message> Rsp doPbRequest(
            String path,
            Req request,
            Parser<Rsp> rspParser,
            Options options) throws NetException, BizException {
        String host = hostAvailabler.GetHost();
        String url = URLCenterHelper.urlCenterInstance(schema, host).getURL(path);
        return httpCaller.doPbRequest(url, request, rspParser, options);
    }

    public <Rsp extends Message> Rsp doJsonRequest(
            String path,
            Object request,
            Parser<Rsp> rspParser,
            Options options) throws NetException, BizException {
        String host = hostAvailabler.GetHost();
        String url = URLCenterHelper.urlCenterInstance(schema, host).getURL(path);
        return httpCaller.doJsonRequest(url, request, rspParser, options);
    }

    public void Shutdown() {
        hostAvailabler.Shutdown();
    }
}
