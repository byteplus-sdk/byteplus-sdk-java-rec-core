package byteplus.rec.core;

import static byteplus.rec.core.Constant.*;

public class StatusHelper {
    public static boolean isUploadSuccess(int code) {
        // It is still considered as success, which is rejected for idempotent
        return code == STATUS_CODE_SUCCESS || code == STATUS_CODE_IDEMPOTENT;
    }

    public static boolean isSuccess(int code) {
        return code == STATUS_CODE_SUCCESS;
    }

    public static boolean isServerOverload(int code) {
        return code == STATUS_CODE_TOO_MANY_REQUEST;
    }

    public static boolean isLossOperation(int code) {
        return code == STATUS_CODE_OPERATION_LOSS;
    }
}
