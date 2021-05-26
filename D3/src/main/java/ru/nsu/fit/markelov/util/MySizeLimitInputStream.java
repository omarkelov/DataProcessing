package ru.nsu.fit.markelov.util;

import com.Ostermiller.util.SizeLimitInputStream;

import java.io.IOException;
import java.io.InputStream;

public class MySizeLimitInputStream extends SizeLimitInputStream {

    public MySizeLimitInputStream(InputStream in, long maxBytesToRead){
        super(in, maxBytesToRead);
    }

    @Override
    public long skip(long n) throws IOException {
        long skippedBytes = super.skip(n);
        if (skippedBytes < 0) { // warning is wrong, because Ostermiller literally doesn't even care about the LSP
            skippedBytes = 0;
        }
        bytesRead += skippedBytes;
        return skippedBytes;
    }
}
