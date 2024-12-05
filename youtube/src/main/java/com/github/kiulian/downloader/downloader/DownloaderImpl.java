package com.github.kiulian.downloader.downloader;

import static com.github.kiulian.downloader.model.Utils.closeSilently;

import com.github.kiulian.downloader.Config;
import com.github.kiulian.downloader.YoutubeException;
import com.github.kiulian.downloader.downloader.request.RequestWebpage;
import com.github.kiulian.downloader.downloader.response.ResponseImpl;
import io.github.pixee.security.BoundedLineReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class DownloaderImpl implements Downloader {

    private final Config config;
    private final OkHttpClient client;

    public DownloaderImpl(Config config, OkHttpClient client) {
        this.config = config;
        this.client = client;
    }

    @Override
    public ResponseImpl<String> downloadWebpage(RequestWebpage request) {
        if (request.isAsync()) {
            ExecutorService executorService = config.getExecutorService();
            Future<String> result = executorService.submit(() -> download(request));
            return ResponseImpl.fromFuture(result);
        }
        try {
            String result = download(request);
            return ResponseImpl.from(result);
        } catch (IOException | YoutubeException e) {
            return ResponseImpl.error(e);
        }
    }

    private String download(RequestWebpage request) throws IOException, YoutubeException {
        int maxRetries = request.getMaxRetries() != null ? request.getMaxRetries() : config.getMaxRetries();
        YoutubeCallback<String> callback = request.getCallback();
        StringBuilder result = new StringBuilder();
        IOException exception;
        do {
            try {
                Request.Builder builder = getBuilder(request.getDownloadUrl(), request.getHeaders(), config.isCompressionEnabled());
                if (request.getBody() != null) builder.post(RequestBody.create(request.getBody(), MediaType.get("application/json")));
                Response response = client.newCall(builder.build()).execute();

                if (response.code() != 200) {
                    YoutubeException.DownloadException e = new YoutubeException.DownloadException("Failed to download: HTTP " + response.code());
                    if (callback != null) callback.onError(e);
                    throw e;
                }

                if (response.body().contentLength() == 0) {
                    YoutubeException.DownloadException e = new YoutubeException.DownloadException("Failed to download: Response is empty");
                    if (callback != null) callback.onError(e);
                    throw e;
                }

                BufferedReader br = null;
                try {
                    InputStream in = response.body().byteStream();
                    if (config.isCompressionEnabled() && "gzip".equals(response.header("content-encoding"))) {
                        in = new GZIPInputStream(in);
                    }
                    br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
                    String inputLine;
                    while ((inputLine = BoundedLineReader.readLine(br, 5_000_000)) != null) result.append(inputLine).append('\n');
                } finally {
                    closeSilently(br);
                }

                exception = null;
            } catch (IOException e) {
                exception = e;
                maxRetries--;
            }
        } while (exception != null && maxRetries > 0);

        if (exception != null) {
            if (callback != null) callback.onError(exception);
            throw exception;
        }

        String resultString = result.toString();
        if (callback != null) callback.onFinished(resultString);

        return resultString;
    }

    private Request.Builder getBuilder(String httpUrl, Map<String, String> headers, boolean acceptCompression) {
        Request.Builder builder = new Request.Builder().url(httpUrl);
        for (Map.Entry<String, String> entry : config.getHeaders().entrySet()) {
            builder.addHeader(entry.getKey(), entry.getValue());
        }
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                builder.addHeader(entry.getKey(), entry.getValue());
            }
        }
        if (acceptCompression) {
            builder.addHeader("Accept-Encoding", "gzip");
        }
        return builder;
    }
}
