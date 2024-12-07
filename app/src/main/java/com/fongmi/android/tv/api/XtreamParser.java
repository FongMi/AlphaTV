package com.fongmi.android.tv.api;

import android.net.Uri;

import com.fongmi.android.tv.bean.Live;
import com.fongmi.android.tv.bean.XCategory;
import com.fongmi.android.tv.bean.XStream;
import com.github.catvod.net.OkHttp;

import java.util.List;

import okhttp3.HttpUrl;

public class XtreamParser {

    public static HttpUrl.Builder getBuilder(Live live) {
        HttpUrl url = HttpUrl.parse(live.getUrl());
        return new HttpUrl.Builder().scheme(url.scheme()).host(url.host()).port(url.port());
    }

    public static boolean isVerify(Uri uri) {
        return isApiUrl(uri) || isGetUrl(uri);
    }

    public static boolean isApiUrl(String url) {
        return isApiUrl(Uri.parse(url));
    }

    public static boolean isApiUrl(Uri uri) {
        return uri.getPath() != null && uri.getPath().contains("player_api.php") && uri.getQueryParameter("username") != null && uri.getQueryParameter("password") != null;
    }

    public static boolean isGetUrl(Uri uri) {
        return uri.getPath() != null && uri.getPath().contains("get.php") && uri.getQueryParameter("username") != null && uri.getQueryParameter("password") != null;
    }

    public static String getEpgUrl(Live live) {
        return getBuilder(live).addPathSegment("xmltv.php").addQueryParameter("username", live.getUsername()).addQueryParameter("password", live.getPassword()).build().toString();
    }

    public static String getApiUrl(Live live, String action) {
        return getBuilder(live).addPathSegment("player_api.php").addQueryParameter("username", live.getUsername()).addQueryParameter("password", live.getPassword()).addQueryParameter("action", action).build().toString();
    }

    public static String getTsUrl(Live live, String id) {
        return getBuilder(live).addPathSegment("live").addPathSegment(live.getUsername()).addPathSegment(live.getPassword()).addPathSegment(id + ".ts").build().toString();
    }

    public static List<XCategory> getCategoryList(Live live) {
        return XCategory.arrayFrom(OkHttp.string(getApiUrl(live, "get_live_categories")));
    }

    public static List<XStream> getStreamList(Live live) {
        return XStream.arrayFrom(OkHttp.string(getApiUrl(live, "get_live_streams")));
    }
}