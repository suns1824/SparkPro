package com.happy.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class HBaseConstant {
    static final byte[] DATA_COLUMNFAMILY = Bytes.toBytes("data");
    static final byte[] AIRTEMP_QUALIFIER = Bytes.toBytes("airtemp");
    static final byte[] INFO_COLUMNFAMILY = Bytes.toBytes("info");
    static final byte[] NAME_QUALIFIER = Bytes.toBytes("name");
    static final byte[] LOCATION_QUALIFIER = Bytes.toBytes("location");
    static final byte[] DESCRIPTION_QUALIFIER = Bytes.toBytes("description");
}
