package brickhouse.hbase;
/**
 * Copyright 2017 TheMeetGroup, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

/**
 * UDF for doing puts with sketchset into HBase To make batch puts, simply set
 * autoflush to false and modify write buffer Currently supports Long and Double
 * puts without casts to String
 */
@Description(name = "hbase_put", value = "string _FUNC_(config, key, array(quals), array(vals)) - Do a HBase Put on a table. "
		+ " Config must contain zookeeper \n" + "quorum, table name, column, and qualifier. Example of usage: \n"
		+ "  hbase_put(map('hbase.zookeeper.quorum', 'hb-zoo1,hb-zoo2', \n"
		+ "                'table_name', 'metrics', \n"
		+ "                'hbase.client.autoflush', 'false', \n"
		+ "				   'hbase.client.write_buffer_size_mb', '5', \n "
		+ "                'family', 'm'), \n"
		+ "            key                  \n    " 
		// Long qualifiers go here
		+ "            array('long_qual1', 'long_qual2') \n"
		+ "            array('long_val1', 'long_val2') \n"
		// Double qualifiers go here (optional)
		+ "            array('double_qual1', 'double_qual2') \n"
		+ "            array('double_val1', 'doulbe_val2') \n")
public class MulticolumnPutUDF extends GenericUDF {
	private static final Logger LOG = Logger.getLogger(MulticolumnPutSketchsetUDF.class);
	private StringObjectInspector keyInspector;

	private ListObjectInspector valLongInspector;
	private ListObjectInspector qualLongInspector;

	private ListObjectInspector valDoubleInspector;
	private ListObjectInspector qualDoubleInspector;

	private Map<String, String> configMap;

	private HTable table;

	public void hbasePut(DeferredObject qualifiersDO, DeferredObject valuesDO, DeferredObject keyDO,
			ListObjectInspector qualInspector, ListObjectInspector valInspector, HTable table) throws HiveException {
		Object qualifiers = qualifiersDO.get();
		Object values = valuesDO.get();

		String key = keyInspector.getPrimitiveJavaObject(keyDO.get());

		int qualLength = qualInspector.getListLength(qualifiers);
		int valLength = valInspector.getListLength(values);
		if (qualLength != valLength) {
			throw new RuntimeException("Qualifier and Value arrays length don't match");
		}

		try {

			for (int i = 0; i < qualLength; ++i) {
				Object uninspQual = qualInspector.getListElement(qualifiers, i);
				StringObjectInspector soi = (StringObjectInspector) qualInspector.getListElementObjectInspector();
				Object uninspVal = valInspector.getListElement(values, i);
				PrimitiveObjectInspector poi = (PrimitiveObjectInspector) valInspector.getListElementObjectInspector();

				byte[] qualifier = HTableFactory.getByteArray(uninspQual, soi);
				byte[] value = HTableFactory.getByteArray(uninspVal, poi);

				if (value != null) {
					Put thePut = HTableFactory.getPut(key.getBytes(), configMap);
					thePut.add(configMap.get(HTableFactory.FAMILY_TAG).getBytes(), qualifier, value);
					table.put(thePut);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new HiveException();
		}
	}

	@Override
	public Object evaluate(DeferredObject[] arg0) throws HiveException {
		String key = keyInspector.getPrimitiveJavaObject(arg0[1].get());
		try {
			if (table == null) {
				table = HTableFactory.getHTable(configMap);
				// LOG.error(table.getWriteBufferSize());
				// LOG.error(table.isAutoFlush());
			}

			hbasePut(arg0[2], arg0[3], arg0[1], qualLongInspector, valLongInspector, table);
			if (arg0.length > 4)
				hbasePut(arg0[4], arg0[5], arg0[1], qualDoubleInspector, valDoubleInspector, table);

		} catch (IOException e) {
			e.printStackTrace();
			throw new HiveException();
		}
		return "Put " + key;
	}

	@Override
	public String getDisplayString(String[] arg0) {
		return "hbase_multicolumn_put( " + arg0[0] + "," + arg0[1] + "," + arg0[2] + "," + arg0[3] + "," + arg0[4] + " )";
	}

	private void checkConfig(Map<String, String> configIn) {
		if (!configIn.containsKey(HTableFactory.FAMILY_TAG) || !configIn.containsKey(HTableFactory.TABLE_NAME_TAG)
				|| !configIn.containsKey(HTableFactory.ZOOKEEPER_QUORUM_TAG)) {
			String errorMsg = "Error while doing HBase operation with config " + configIn + " ; Config is missing for: "
					+ HTableFactory.TABLE_NAME_TAG + " or " + HTableFactory.ZOOKEEPER_QUORUM_TAG + " or "
					+ HTableFactory.ZOOKEEPER_QUORUM_TAG;
			LOG.error(errorMsg);
			throw new RuntimeException(errorMsg);
		}
		// LOG.error("Config Map is ok: " +
		// Collections.singletonList(configIn));
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
		if (arg0.length != 6 && arg0.length != 4) {
			throw new UDFArgumentException(
					"Argument number: hbase_multicolumn_put requires config map, a String key, qualifier array, value array");
		}
		if (arg0[0].getCategory() != Category.MAP) {
			throw new UDFArgumentException(
					"ARG0 Config Map Error: hbase_multicolumn_put requires config map, a String key, qualifier array, a value array");
		}
		configMap = HTableFactory.getConfigFromConstMapInspector(arg0[0]);
		checkConfig(configMap);

		if (arg0[1].getCategory() != Category.PRIMITIVE
				|| ((PrimitiveObjectInspector) arg0[1]).getPrimitiveCategory() != PrimitiveCategory.STRING) {
			throw new UDFArgumentException(
					"ARG1 Key Error: hbase_multicolumn_put requires config map, a String key, qualifier array, a value array");
		}
		keyInspector = (StringObjectInspector) arg0[1];

		if (arg0[2].getCategory() != Category.LIST) {
			throw new UDFArgumentException(
					"ARG2 Qualifier Error: hbase_multicolumn_put requires config map, a String key, qualifier array, a value array");
		}
		qualLongInspector = (ListObjectInspector) arg0[2];

		if (!(qualLongInspector.getListElementObjectInspector() instanceof StringObjectInspector)) {
			throw new UDFArgumentException("ARG2 Qualifer Error: qualifer array is not string");
		}
		if (arg0[3].getCategory() != Category.LIST) {
			throw new UDFArgumentException(
					"ARG3 Value Error: hbase_multicolumn_put requires config map, a String key, qualifier array, a value array");
		}
		valLongInspector = (ListObjectInspector) arg0[3];

		if (arg0[4].getCategory() != Category.LIST) {
			throw new UDFArgumentException(
					"ARG4 Error: hbase_multicolumn_put requires config map, a String key, qualifier array, a value array");
		}

		if (arg0.length > 4) {
			qualDoubleInspector = (ListObjectInspector) arg0[4];

			if (!(qualDoubleInspector.getListElementObjectInspector() instanceof StringObjectInspector)) {
				throw new UDFArgumentException(
						"ARG4 Secondary Double Qualifier Error: must be a qualifier string array");
			}

			if (arg0[5].getCategory() != Category.LIST) {
				throw new UDFArgumentException(
						"ARG5 Secondary Double Value Error: must be an array");
			}
			valDoubleInspector = (ListObjectInspector) arg0[5];

		}

		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

}
