package com.sogou.feed.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class GenericUDFVersionCompare extends GenericUDF {

    private transient ObjectInspectorConverters.Converter[] converters;
    private IntWritable result = new IntWritable(0);

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The function VERSION_COMPARE accepts exactly 2 arguments.");
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }

        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args[0].get() == null || args[1].get() == null) {
            return null;
        }
        String leftValue = ((Text) converters[0].convert(args[0].get())).toString();
        String rightValue = ((Text) converters[1].convert(args[1].get())).toString();
        String[] leftValueArr = leftValue.trim().split("\\.");
        int leftArrLen = leftValueArr.length;
        String[] rightValueArr = rightValue.trim().split("\\.");
        int rightArrLen = rightValueArr.length;
        int maxLen = Math.max(leftArrLen, rightArrLen);
        for (int i = 0; i < maxLen; i++) {
            int left = 0;
            int right = 0;
            if (i < leftArrLen) {
                left = Integer.parseInt(leftValueArr[i]);
            }
            if (i < rightArrLen) {
                right = Integer.parseInt(rightValueArr[i]);
            }
            if (left > right) {
                result.set(1);
                break;
            }
            if (left < right) {
                result.set(-1);
                break;
            }
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 1);
        return "array length: " + children.length;
    }
}