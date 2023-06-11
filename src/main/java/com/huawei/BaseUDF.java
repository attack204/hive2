package com.huawei;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;
public class BaseUDF extends UDF {
    //传两个参数
    public String evaluate(String line, String jsonkeysString){
        // 0 创建一个 StringBuilder 用来作为结果集
        StringBuilder sb = new StringBuilder();
        // 1 jsonkeysString 使用","切割
        String[] jsonkeys = jsonkeysString.split(",");
        // 2 line 服务器时间| json
        if (line== null){
            return "";
        }
        //”\\”是转意符
        String[] logContents = line.split("\\|");
        //校验，判断

        if (logContents.length != 2 || StringUtils.isBlank(logContents[1])){
            return "";
        }
        // 3 对 logContents[1]创建 json 对象
        try {
            JSONObject jsonObject = new JSONObject(logContents[1]);
            //4 获取公共字段 cm 的 json 对象
            JSONObject cm = jsonObject.getJSONObject("cm");
            //5 循环遍历
            for (int i = 0; i < jsonkeys.length; i++) {
                String jsonkey = jsonkeys[i];
                if(cm.has(jsonkey)){
                    sb.append(cm.getString(jsonkey)).append("\t");
                }else {
                    sb.append("\t");
                }
            }
            //6 拼接事件字段
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(logContents[0]).append("\t");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return sb.toString();
    } }