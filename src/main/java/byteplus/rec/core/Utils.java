package byteplus.rec.core;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Utils {
    public interface Callable<Rsp extends Message, Req> {
        Rsp call(Req req, Option... opts) throws BizException, NetException;
    }

    public static <Rsp extends Message, Req> Rsp doWithRetry(
            Callable<Rsp, Req> callable,
            Req req,
            Option[] opts,
            int retryTimes) throws BizException {

        Rsp rsp = null;
        if (retryTimes < 0) {
            retryTimes = 0;
        }
        int tryTimes = retryTimes + 1;
        for (int i = 0; i < tryTimes; i++) {
            try {
                rsp = callable.call(req, opts);
            } catch (NetException e) {
                if (i == tryTimes - 1) {
                    log.error("[DoRetryRequest] fail finally after retried {} times", tryTimes);
                    throw new BizException(e.getMessage());
                }
                continue;
            }
            break;
        }
        return rsp;
    }
}
