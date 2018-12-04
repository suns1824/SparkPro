package com.happy.hbase;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;

import java.util.HashMap;
import java.util.Map;

public class HBaseStationQuery extends Configured implements Tool{
    public Map<String, String> getStationInfo(HTable table, String stationId) throws Exception{
        Get get = new Get(Bytes.toBytes(stationId));
        get.addFamily(HBaseConstant.INFO_COLUMNFAMILY);
        Result res = table.get(get);
        if (res == null) {
            return null;
        }
        Map<String, String> resultMap = new HashMap<>();
        resultMap.put("name", getValue(res, HBaseConstant.INFO_COLUMNFAMILY, HBaseConstant.NAME_QUALIFIER ));
        resultMap.put("location", getValue(res, HBaseConstant.INFO_COLUMNFAMILY, HBaseConstant.LOCATION_QUALIFIER));
        resultMap.put("description", getValue(res, HBaseConstant.INFO_COLUMNFAMILY, HBaseConstant.DESCRIPTION_QUALIFIER));
        return resultMap;
    }

    private static String getValue(Result res, byte[] cf, byte[] qualifier) {
        byte[] value = res.getValue(cf, qualifier);
        return value == null ? "" : Bytes.toString(value);
    }

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static void main(String[] args) {

    }
}
