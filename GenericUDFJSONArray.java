package com.sogou.feed.hive.udf;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.json.JSONArray;

public class GenericUDFJSONArray extends GenericUDF {

    private transient ObjectInspectorConverters.Converter converter;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentException("The function json_array(jsonArrayStr) takes exactly 1 arguments.");
        }
        converter = ObjectInspectorConverters.getConverter(args[0],
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        return ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        assert (args.length == 1);
        if (args[0].get() == null) {
            return null;
        }
        String jsonArrayStr = ((Text) converter.convert(args[0].get())).toString();
        if (StringUtils.isNotEmpty(jsonArrayStr)) {
            JSONArray jsonArray = new JSONArray(jsonArrayStr);
            ArrayList<Text> result = new ArrayList<Text>();
            for (int i = 0; i < jsonArray.length(); i++) {
                result.add(new Text(jsonArray.get(i).toString()));
            }
            return result;
        }
        return null;

    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 1);
        return "array length: " + children.length;
    }
}