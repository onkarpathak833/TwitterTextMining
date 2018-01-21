package com.lm.type2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class CompareJsonTables {

	public static String VARIABLE_ACTIVE = "rowActive";
	public static String VARIABLE_STARTDATE = "rowEffectiveDate";
	public static String VARIABLE_ENDDATE = "rowExpirationDate";
	public static String VARIABLE_ACTIVITY = "rowActivity";
	public static final String VALUE_ACTIVE = "1";
	public static final String VALUE_INACTIVE = "0";
	public static final String VALUE_ACTIVITY_NEW_RECORD = "New Record";
	public static final String VALUE_ACTIVITY_MODIFIED_RECORD = "Modified Record";
	public static final String VALUE_ACTIVITY_DELETED_RECORD = "Record Deleted";
	public static final String VALUE_ACTIVITY_ADDED_RECORD = "Record Added";
	public static final String VALUE_CURRENT_ACTIVE_DATE = "9999-12-31 00:00:00";
	public static final String VARIABLE_TABLE_NAME = "TableName";
	public static final String VALUE_ACTIVITY_UNCHANGED = "Record Unchanged";
	public static final String VALUE_OLD_ARRAY = "OLD_ARRAY";
	public static final String VALUE_NEW_ARRAY = "NEW_ARRAY";
	public static long totalRecords = 0;
	public static long recordsModified = 0;
	public static long recordsDeleted = 0;
	public static long recordsUnchanged = 0;
	public static long recordsAdded = 0;
	public static long recordsOfSchemaEvolution = 0;

	public JSONArray firstTableIngestion(JSONArray inputTables, String schemaString) throws JSONException {
		// JSONArray jsonArr = inputTables.getJSONArray(0);
		JSONArray returnArray = new JSONArray();
		Schema schema = Schema.parse(schemaString);
		List<Field> columnNames = schema.getFields();
		String lastValue = inputTables.get(inputTables.length() - 1).toString();
		inputTables.remove(inputTables.length() - 1);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		//sdf.setTimeZone(TimeZone.getTimeZone("EDT"));
		for (int i = 0; i < inputTables.length(); i++) {

			JSONObject object;
			try {
				object = new JSONObject(inputTables.get(i).toString());
				object = removeType2Columns(object);
				// object = inputTables.getJSONObject(i);
				object.put(CompareJsonTables.VARIABLE_ACTIVE, CompareJsonTables.VALUE_ACTIVE);
				Date date = new Date();
				String currDate = sdf.format(date);
				object.put(CompareJsonTables.VARIABLE_STARTDATE, currDate);
				object.put(CompareJsonTables.VARIABLE_ENDDATE, CompareJsonTables.VALUE_CURRENT_ACTIVE_DATE);
				object.put(CompareJsonTables.VARIABLE_ACTIVITY, CompareJsonTables.VALUE_ACTIVITY_NEW_RECORD);
				String recordString = "{";
				boolean isLastColumn = false;
				for (int j = 0; j < columnNames.size(); j++) {
					if (j == columnNames.size() - 1) {
						isLastColumn = true;
					}
					Object value = null;
					if (object.has(columnNames.get(j).name().toString())) {
						value = object.get(columnNames.get(j).name().toString());
						if (columnNames.get(j).schema().toString().equals("\"string\"")) {
							value = "\"" + value + "\"";
						}
						recordString = recordString + "\"" + columnNames.get(j).name().toString() + "\"" + ":" + value;
						if (!isLastColumn) {
							recordString = recordString + ",";
						}
						if (isLastColumn) {
							recordString = recordString + "}";
						}
					}

				}
				returnArray.put(i, recordString);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			totalRecords++;

		}
		returnArray.toString().getBytes(StandardCharsets.UTF_8);
		return returnArray;
	}

	public static JSONArray formatJsonOutput(JSONArray outputArray, String schemaString) throws JSONException {
		Schema schema = Schema.parse(schemaString);
		JSONArray jsonArray = new JSONArray();
		List<Field> columnNames = schema.getFields();
		for (int i = 0; i < outputArray.length(); i++) {
			JSONArray arrayTemp = outputArray.getJSONArray(i);

			for (int index = 0; index < arrayTemp.length(); index++) {
				JSONObject object = new JSONObject(arrayTemp.get(index).toString());
				String recordString = "{";
				boolean isLastColumn = false;
				for (int j = 0; j < columnNames.size(); j++) {
					if (j == columnNames.size() - 1) {
						isLastColumn = true;
					}
					Object value = object.get(columnNames.get(j).name().toString());
					if (columnNames.get(j).schema().toString().equals("\"string\"")) {
						value = "\"" + value + "\"";
					}
					recordString = recordString + "\"" + columnNames.get(j).name().toString() + "\"" + ":" + value;
					if (!isLastColumn) {
						recordString = recordString + ",";
					}
					if (isLastColumn) {
						recordString = recordString + "}";
					}
				}
				jsonArray.put(recordString);

			}
		}

		return jsonArray;
	}

	public long newAddedrecords() {

		return totalRecords;
	}

	public long getRecordsAdded() {
		long recordAdded = recordsAdded;
		recordsAdded = 0;
		return recordAdded;
	}

	public long getRecordsDeleted() {

		long recordDeleted = recordsDeleted;
		recordsDeleted = 0;
		return recordDeleted;
	}

	public long getRecordsModified() {
		long recordModified = recordsModified;
		recordsModified = 0;
		return recordModified;

	}

	public long getRecordsUnchanged() {
		long recordUnchanged = recordsUnchanged;
		recordsUnchanged = 0;
		return recordUnchanged;
	}

	public static JSONObject removeType2Columns(JSONObject object) {
		object.remove(VARIABLE_ACTIVE);
		object.remove(VARIABLE_ACTIVITY);
		object.remove(VARIABLE_ENDDATE);
		object.remove(VARIABLE_STARTDATE);
		return object;
	}

	public static boolean isJsonEqual(JSONObject obj1, JSONObject obj2) throws JSONException {
		boolean isEqual = true;
		Map<String, String> obj1Map = null;
		Map<String, String> obj2Map = null;
		try {
			obj1Map = jsonToMap(obj1);
			obj2Map = jsonToMap(obj2);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Iterator<String> keyItr = obj1Map.keySet().iterator();
		while (keyItr.hasNext()) {
			String key = keyItr.next();
			String obj1Value = obj1Map.get(key);
			String obj2Value = null;
			if (obj2Map.containsKey(key)) {
				obj2Value = obj2Map.get(key);
			} else {
				obj2Value = "_NULL_VALUE_";
			}
			if (!("_NULL_VALUE_".equals(obj2Value)) && obj1Value.equals(obj2Value)) {
				continue;
			} else {
				// System.out.println("Key : "+key+" "+obj1Value+" "+obj2Value);
				isEqual = false;
				break;
			}
		}

		return isEqual;
	}

	public static Map<String, String> jsonToMap(JSONObject json) throws JSONException {
		Map<String, String> retMap = new HashMap<String, String>();

		if (json != JSONObject.NULL) {
			retMap = toMap(json);
		}
		return retMap;
	}

	public static Map<String, String> toMap(JSONObject object) throws JSONException {
		Map<String, String> map = new HashMap<String, String>();

		Iterator<String> keysItr = object.keys();
		while (keysItr.hasNext()) {
			String key = keysItr.next();
			Object value = object.get(key);

			if (value instanceof JSONArray) {
				value = toList((JSONArray) value);
			}

			else if (value instanceof JSONObject) {
				value = toMap((JSONObject) value);
			}
			map.put(key, value.toString());
		}
		return map;
	}

	public static List<Object> toList(JSONArray array) throws JSONException {
		List<Object> list = new ArrayList<Object>();
		for (int i = 0; i < array.length(); i++) {
			Object value = array.get(i);
			if (value instanceof JSONArray) {
				value = toList((JSONArray) value);
			}

			else if (value instanceof JSONObject) {
				value = toMap((JSONObject) value);
			}
			list.add(value);
		}
		return list;
	}

	public static JSONArray findCorrectObject(String[] primaryKeys, JSONObject outerObject, List innerArray, String arrayName)
			throws JSONException {
		int noOfOccurances = 0, count = 0;
		JSONArray matchArray = new JSONArray();
		JSONObject returnObject = new JSONObject();
		for (int i = 0; i < innerArray.size(); i++) {
			String primaryKey = "";
			JSONObject object = (JSONObject) innerArray.get(i);
			for (int index = 0; index < primaryKeys.length; index++) {
				try {
					String key = primaryKeys[index];
					String keyValue = String.valueOf(outerObject.get(key));
					if (String.valueOf(object.get(key)).equals(keyValue)) {
						count++;
						noOfOccurances++;
						primaryKey = primaryKey+keyValue;
					}
				} catch (Exception e) {
					continue;
				}
			}
			if(arrayName.equals(VALUE_NEW_ARRAY) && count==primaryKeys.length){
				count = 0;
				matchArray.put(object);
				break;
			}
			else if(arrayName.equals(VALUE_OLD_ARRAY) && count==primaryKeys.length) {
				count =0 ;
				object.put("PrimaryKey",primaryKey);
				matchArray.put(object);
			}

		}
		/*if (matchArray.length() == 1) {
				returnObject = matchArray.getJSONObject(0);
			}
			if (matchArray.length() > 1) {
				for (int j = 0; j < matchArray.length(); j++) {
					JSONObject obj = matchArray.getJSONObject(j);
					if (obj.get(VARIABLE_ACTIVE).equals("1")) {
						returnObject = obj;
					}
				}
				returnObject.put("MultipleObjects", "True");
			}

		if (matchArray.length() == 0) {
			System.out.println("Deleted record Entry !" + returnObject.length());
		}*/
		return matchArray;
	}

	/*public static boolean removeJsonObject(JSONArray array, JSONObject object) throws JSONException {
		boolean isDeleted = false;
		for (int i = 0; i < array.length(); i++) {
			JSONObject jsonObject = new JSONObject(array.get(i).toString());
			// JSONObject jsonObject = array.getJSONObject(i);
			if (isJsonEqual(jsonObject, object)) {
				array.remove(i);
				isDeleted = true;
			}
		}

		return false;
	}*/

	public static JSONObject getJsonObject(List<JSONObject> array, JSONObject object) throws JSONException {
		for (int i = 0; i < array.size(); i++) {
			JSONObject tempObject = array.get(i);
			if (isJsonEqual(tempObject, object)) {
				return tempObject;
			}

		}
		return null;
	}

	
	
	public static JSONArray removeJsonObjects(String primaryKeys, String primaryKeyValues , JSONArray inputArray, String schemaString) throws JSONException{
		
		String allKeys[] = primaryKeys.split(",");
		String allValues [] = primaryKeyValues.split("#");
		List<List> valueList = new ArrayList<List>();
		int noDeleetd = 0;
		for(int i=0;i<allValues.length;i++){
		 String tempStr[] = allValues[i].split(",");	
		List tempList = new ArrayList();	
		 for(int j=0;j<tempStr.length;j++){
				tempList.add(tempStr[j]);
			}
		 valueList.add(tempList);
		}
		JSONArray returnArray = new JSONArray();
		List<Long> deleteIndices = new ArrayList<Long>();
		for(int i=0;i<inputArray.length();i++){
			JSONObject tempObject = new JSONObject(inputArray.get(i).toString());
			boolean isDeleted = false;
			
			
			for(int k=0;k<valueList.size();k++){
				List tempList = valueList.get(k);
				int count = 0;
				for(int j=0;j<allKeys.length;j++){
					String key = allKeys[j];
					String value = tempObject.getString(key);
					if(tempList.contains(value)){
						count++;
					}
					}
					
				if(count==allKeys.length){
					//inputArray.remove(i);
					deleteIndices.add(Long.valueOf(i));
					noDeleetd++;
					isDeleted = true;
				}
				
				}
			if(!isDeleted){
				returnArray.put(tempObject);
			}
			
		}
			
		
		JSONArray formatArry = new JSONArray();
		formatArry.put(returnArray);
		System.out.println("No. of objects deleted are : "+noDeleetd);
		JSONArray outputArray = formatJsonOutput(formatArry, schemaString);
		return outputArray;
		
		
	}
	
	@SuppressWarnings("deprecation")
	public JSONArray compareJSONTables(JSONArray inputTables, String primaryKeyValue, String escapeColumns,
			String schemaString) throws JSONException {

		boolean escapeColsAvailable = true;
		String primaryKeys[] = primaryKeyValue.split(",");
		String escapeColumnValues[] = null;
		if (escapeColumns == null) {
			escapeColumns = "";
		}

		if (escapeColumns != null && escapeColumns.split(",").length > 1) {
			escapeColumnValues = escapeColumns.split(",");
		}
		if (escapeColumnValues == null) {
			escapeColumnValues = new String[] { CompareJsonTables.VARIABLE_ACTIVE, CompareJsonTables.VARIABLE_ACTIVITY,
					CompareJsonTables.VARIABLE_ENDDATE, CompareJsonTables.VARIABLE_STARTDATE };
		} else {
			int index = escapeColumnValues.length - 1;
			escapeColumnValues[index++] = CompareJsonTables.VARIABLE_ACTIVE;
			escapeColumnValues[index++] = CompareJsonTables.VARIABLE_ACTIVITY;
			escapeColumnValues[index++] = CompareJsonTables.VARIABLE_ENDDATE;
			escapeColumnValues[index++] = CompareJsonTables.VARIABLE_STARTDATE;
		}
		JSONArray table1Array = new JSONArray();
		JSONArray table2Array = new JSONArray();
		boolean firstIngestion = false;
		if (inputTables.length() > 1) {
			table1Array = inputTables.getJSONArray(0);
			table2Array = inputTables.getJSONArray(1);
		}

		JSONArray returnJsonArray = new JSONArray();
		JSONArray returnOriginalArray = new JSONArray();
		JSONArray returnnNewArray = new JSONArray();
		boolean returnBothArrays = false;
		int table1Length = table1Array.length();
		int table2Length = table2Array.length();

		JSONObject tempObject1 = table1Array.getJSONObject(table1Length - 1);
		String tableName1 = tempObject1.getString(CompareJsonTables.VARIABLE_TABLE_NAME);

		if (firstIngestion) {
			tableName1 = tableName1 + "_OLD";
		}

		table1Array.remove(table1Length - 1);
		JSONObject tempObject2 = table2Array.getJSONObject(table2Length - 1);
		String tableName2 = tempObject2.getString(CompareJsonTables.VARIABLE_TABLE_NAME);
		table2Array.remove(table2Length - 1);

		JSONArray outerArray = new JSONArray();
		List<JSONObject> outerArrayList = new ArrayList<JSONObject>();
		List<JSONObject> innerArrayList = new ArrayList<JSONObject>();
		JSONArray innerArray = new JSONArray();
		JSONArray originalArray = new JSONArray();
		JSONArray newArray = new JSONArray();

		if (tableName1.endsWith("OLD")) {
			outerArray = table1Array;
			for (int i = 0; i < table1Array.length(); i++) {
				outerArrayList.add(i, new JSONObject(table1Array.get(i).toString()));
			}
			innerArray = table2Array;
			for (int i = 0; i < table2Array.length(); i++) {
				innerArrayList.add(i, new JSONObject(table2Array.get(i).toString()));

			}
		} else if (tableName2.endsWith("OLD")) {
			outerArray = table2Array;
			for (int i = 0; i < table2Array.length(); i++) {
				outerArrayList.add(i, new JSONObject(table2Array.get(i).toString()));
			}
			innerArray = table1Array;
			for (int i = 0; i < table1Array.length(); i++) {
				innerArrayList.add(i, new JSONObject(table1Array.get(i).toString()));
			}

		}

		List<JSONObject> deletedObjects = new ArrayList<JSONObject>();
		originalArray = outerArray;
		newArray = innerArray;
		Map<String,String> recordProcessed = new HashMap<String,String>();
		JSONArray arrayForDeletedCase = new JSONArray();
		for (int i = 0; i < outerArrayList.size(); i++) {
			boolean isRecordProcessed = false;
			JSONObject jsonOBject1 = outerArrayList.get(i);
			JSONArray tempJsonarray = findCorrectObject(primaryKeys, jsonOBject1, outerArrayList,VALUE_OLD_ARRAY);
			boolean multipleOccurances = false;
			if(tempJsonarray.length() > 1){
				multipleOccurances = true;
				for(int index=0;index<tempJsonarray.length();index++){
					JSONObject tempObj = tempJsonarray.getJSONObject(index);
					if(tempObj.getString(VARIABLE_ACTIVE).equals("1")){
						jsonOBject1 = tempObj;
						tempJsonarray.remove(index);
						break;
					}
				}
			}
			else{
				jsonOBject1 = tempJsonarray.getJSONObject(0);
				tempJsonarray.remove(0);
			}
			if(recordProcessed.containsKey(jsonOBject1.get("PrimaryKey").toString())){
				isRecordProcessed = true;
			}
			String keyValue = jsonOBject1.getString("PrimaryKey");
			jsonOBject1.remove("PrimaryKey");

			if(!isRecordProcessed) {
				// commenting start of old if block here -> if
				// (!jsonOBject1.has(CompareJsonTables.VARIABLE_ACTIVE)) {
				boolean objectFound = false;
				String primaryKey = "";
				if (primaryKeys.length == 1) {
					primaryKey = primaryKeys[0];
				}
				// primaryKey = String.valueOf(jsonOBject1.get("EMP_ID"));
				if (primaryKeys.length > 1) {

				}
				JSONObject jsonObject2 = new JSONObject();
				JSONArray tempjsonObject2 = findCorrectObject(primaryKeys, jsonOBject1, innerArrayList,VALUE_NEW_ARRAY);
				if(tempjsonObject2.length()>0){
					jsonObject2 = tempjsonObject2.getJSONObject(0);
				}
				// int retainObjIndex = innerArrayList.indexOf(jsonObject2);
				// JSONObject retainJsonObject = innerArrayList.get(retainObjIndex);
				boolean dataChangeFlag = false;
				boolean primaryKeyMatch = false;
				if (jsonObject2.length() > 0) {
					// jsonObject2 = removeType2Columns(jsonObject2);
					// jsonOBject1 = removeType2Columns(jsonOBject1);
					objectFound = true;
					primaryKeyMatch = true;
				}

				// if(String.valueOf(jsonObject2.get("EMP_ID")).equals(primaryKey))
				if (primaryKeyMatch) {
					// System.out.println("Comparing rows with same primary keys.");
					objectFound = true;

					JSONArray columnNames = jsonOBject1.names();
					List<String> correctColumns = new ArrayList<String>();

					for (int colIndex = 0; colIndex < columnNames.length(); colIndex++) {

						correctColumns.add(columnNames.getString(colIndex));
					}

					if (escapeColsAvailable) {
						List<String> escapeCols = new ArrayList<String>(Arrays.asList(escapeColumnValues));
						JSONArray tempColumns = jsonOBject1.names();
						correctColumns.clear();
						for (int index = 0; index < tempColumns.length(); index++) {
							String tempCol = tempColumns.getString(index);
							if (!escapeCols.contains(tempCol)) {
								correctColumns.add(tempCol);
							}

						}
					}

					JSONArray columnList = new JSONArray(correctColumns);
					JSONObject object1 = new JSONObject();
					int originalIndex = 0, newArrayIndex = 0;

					for (int index = 0; index < originalArray.length(); index++) {
						JSONObject tempObject = new JSONObject(originalArray.get(index).toString());
						// tempObject = removeType2Columns(tempObject);
						if (isJsonEqual(tempObject, jsonOBject1)) {
							object1 = tempObject;
							// object1 = removeType2Columns(object1);
							originalIndex = index;
							break;
						}
					}

					JSONObject object2 = new JSONObject();

					for (int index = 0; index < newArray.length(); index++) {
						JSONObject tempObject = new JSONObject(newArray.get(index).toString());
						// tempObject = removeType2Columns(tempObject);
						if (isJsonEqual(tempObject, jsonObject2)) {
							object2 = tempObject;
							// object2 = removeType2Columns(object2);
							newArrayIndex = index;
							break;

						}
					}
					boolean isSchemaEvolved = false;
					for (int j = 0; j < columnList.length(); j++) {

						Object object2Value = null;
						if (object2.has(columnList.getString(j))) {
							object2Value = object2.get(columnList.getString(j));
							if (object1.get(columnList.getString(j)).equals(object2Value)) {
								// System.out.println("Value for " +
								// columnList.getString(j) + " is same.");
							} else {
								// System.out.println("Values for " +
								// columnList.getString(j) + " are different.");
								dataChangeFlag = true;
								/*
								 * System.out.println(object1.get(columnList.
								 * getString(j)) + " AND " +
								 * object2.get(columnList.getString(j)));
								 */
								// jsonObject2.putOnce("Active", "1");
							}
						} else {
							// System.out.println("Schema Evolved. Column Not Found
							// in New Table : "+columnList.getString(j));
							isSchemaEvolved = true;
						}

					}
					if (dataChangeFlag == false) {
						String rowActivity = "";
						if(object1.has(CompareJsonTables.VARIABLE_ACTIVITY)){
							rowActivity = object1.getString(CompareJsonTables.VARIABLE_ACTIVITY);
						}
						Date date = new Date();
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						Object dateValue = null;
						if(object1.has(VARIABLE_STARTDATE)){
							dateValue = object1.get(VARIABLE_STARTDATE).toString();
							try{
								Date tempDate = new Date(dateValue.toString());
								dateValue = sdf.format(tempDate);
							} catch(Exception e){
								try{
									dateValue = sdf.format((Date)dateValue);
								}catch(Exception e1){
									dateValue = dateValue.toString();
								}
							} 
						} else{
							dateValue = sdf.format(date);
						}
						object1 = removeType2Columns(object1);
						object1.put(CompareJsonTables.VARIABLE_ACTIVE, CompareJsonTables.VALUE_ACTIVE);
						object1.put(CompareJsonTables.VARIABLE_STARTDATE, dateValue);
						object1.put(CompareJsonTables.VARIABLE_ENDDATE, CompareJsonTables.VALUE_CURRENT_ACTIVE_DATE);
						//object1.put(CompareJsonTables.VARIABLE_ACTIVITY, CompareJsonTables.VALUE_ACTIVITY_UNCHANGED);
						object1.put(CompareJsonTables.VARIABLE_ACTIVITY, rowActivity);
						returnOriginalArray.put(object1);
						if(multipleOccurances){
							for(int index=0;index<tempJsonarray.length();index++){
								returnOriginalArray.put(tempJsonarray.getJSONObject(index));
							}
						}
						//removeJsonObject(newArray, object1);
						newArray.remove(newArrayIndex);
						recordsUnchanged++;
					} else if (dataChangeFlag == true) {
						Object startDate = null;

						// int index = outerArrayList.indexOf(object1);
						JSONObject obj = new JSONObject();
						try{
							obj = getJsonObject(outerArrayList, object1); // outerArrayList.get(index);
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
							DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
							if (obj.has(CompareJsonTables.VARIABLE_STARTDATE)) {
								startDate = obj.get(CompareJsonTables.VARIABLE_STARTDATE);
								try{
									Date tempDate = new Date(startDate.toString());
									startDate = sdf.format(tempDate);
								} catch(Exception e){
									try{
										startDate = sdf.format((Date)startDate);
									}catch(Exception e1){
										startDate = startDate.toString();
									}
								}
							} else {
								startDate = new Date();

								startDate = sdf.format(startDate);
							}
							String currentActivity = "";
							if(obj.has(VARIABLE_ACTIVITY)){
								currentActivity = obj.getString(VARIABLE_ACTIVITY);
							}
							String obj2StartDate = "";
							/*if(obj.has(VARIABLE_ENDDATE)) {
						obj1EndDate = obj.getString(VARIABLE_ENDDATE);
						Date date = sdf.parse(obj1EndDate);
						//Calendar calendar = Calendar.getInstance();
						//calendar.set(date.getYear(), date.getMonth(), date.getDate(), date.getHours(), date.getMinutes(), date.getSeconds());
						//calendar.setTimeInMillis(date.get);
						int seconds = date.getSeconds();
						 seconds = seconds + 1;
						 date.setSeconds(seconds);
						 obj1EndDate = sdf.format(date);
						//date = date.
					}*/

							object1 = removeType2Columns(object1);
							object1.putOnce(CompareJsonTables.VARIABLE_ACTIVE, CompareJsonTables.VALUE_INACTIVE);
							object1.put(CompareJsonTables.VARIABLE_STARTDATE, startDate.toString());
							DateTime date = new DateTime();
							String dateValue = format.print(date);
							int secondsFromDate = Integer.valueOf(dateValue.substring(dateValue.lastIndexOf(":")+1,dateValue.length()))+1;
							//DateTime dateTimeNew = date.plusSeconds(1);
							String newSeconds = String.valueOf((secondsFromDate < 10 ? "0" : "") + secondsFromDate);
							String firstString = dateValue.substring(0,dateValue.lastIndexOf(":"));
							obj2StartDate = firstString+":"+newSeconds;
							//obj2StartDate =  dateValue.replace(dateValue.substring(dateValue.lastIndexOf(":")+1,dateValue.length()),String.valueOf(secondsFromDate));
							//obj2StartDate = format.print(dateTimeNew);
							object1.put(CompareJsonTables.VARIABLE_ENDDATE,dateValue);
							object1.put(CompareJsonTables.VARIABLE_ACTIVITY, currentActivity);
							object2 = removeType2Columns(object2);
							object2.putOnce(CompareJsonTables.VARIABLE_ACTIVE, CompareJsonTables.VALUE_ACTIVE);

							object2.put(CompareJsonTables.VARIABLE_STARTDATE, obj2StartDate);
							object2.put(CompareJsonTables.VARIABLE_ENDDATE, CompareJsonTables.VALUE_CURRENT_ACTIVE_DATE);
							object2.put(CompareJsonTables.VARIABLE_ACTIVITY, CompareJsonTables.VALUE_ACTIVITY_MODIFIED_RECORD);

							returnBothArrays = true;
							returnOriginalArray.put(object1);

							if(multipleOccurances){
								for(int index=0;index<tempJsonarray.length();index++){
									JSONObject defaultObject = tempJsonarray.getJSONObject(index);
									defaultObject.remove(VARIABLE_ACTIVE);
									//defaultObject.remove(VARIABLE_ENDDATE);
									defaultObject.put(VARIABLE_ACTIVE, VALUE_INACTIVE);
									//defaultObject.put(VARIABLE_ENDDATE, dateValue);
									returnOriginalArray.put(tempJsonarray.getJSONObject(index));
								}
							}

							returnnNewArray.put(object2);
							newArray.remove(newArrayIndex);
						}catch(Exception e){
							System.out.println(e);
						}
						// removeJsonObject(newArray, object2);
						recordsModified++;
					}
					if (isSchemaEvolved) {
						recordsOfSchemaEvolution++;
					}

					// innerArray.remove(k);
					// innerArrayList.remove(jsonObject2);
					// table2JsonArray.remove(k);
					/*
					 * if(objectFound) { //outerArray.remove(i);
					 * copyOriginalList.remove(object1); objectFound = false; }
					 */
					// break;
				}

				if (!objectFound) {
					deletedObjects.add(jsonOBject1);
					if(multipleOccurances){
						for(int index=0;index<tempJsonarray.length();index++){
							deletedObjects.add(tempJsonarray.getJSONObject(index));
						}
					}

				}

				// Commenting old if block here.}

				// System.out.println(jsonOBject1);
				// System.out.println(jsonOBject1.names());
				recordProcessed.put(keyValue, "Processed");
			}
		}

		if (newArray.length() > 0) {
			returnBothArrays = true;
			System.out.println("New Table has new records added.");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			for (int i = 0; i < newArray.length(); i++) {
				JSONObject addedObject = new JSONObject(newArray.get(i).toString());
				addedObject = removeType2Columns(addedObject);
				addedObject.put(CompareJsonTables.VARIABLE_ACTIVE, CompareJsonTables.VALUE_ACTIVE);
				addedObject.put(CompareJsonTables.VARIABLE_ACTIVITY, CompareJsonTables.VALUE_ACTIVITY_ADDED_RECORD);
				Date date = new Date();
				String currDate = sdf.format(date);
				addedObject.put(CompareJsonTables.VARIABLE_STARTDATE, currDate);
				addedObject.put(CompareJsonTables.VARIABLE_ENDDATE, CompareJsonTables.VALUE_CURRENT_ACTIVE_DATE);
				returnnNewArray.put(addedObject);
				recordsAdded++;
			}
		}
		int count = 0;
		if (deletedObjects.size() > 0) {
			// System.out.println("New Table has deleted records from old
			// table.");
			returnBothArrays = true;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			for (int i = 0; i < deletedObjects.size(); i++) {
				JSONObject deletedObj = deletedObjects.get(i);
				Date date = new Date();
				String currDate = sdf.format(date);
				Object startDate = null;
				if(deletedObj.getString(VARIABLE_ACTIVE).equals("1")){
					deletedObj = getJsonObject(outerArrayList, deletedObj);


					if (deletedObj.has(CompareJsonTables.VARIABLE_STARTDATE)) {
						startDate = deletedObj.get(CompareJsonTables.VARIABLE_STARTDATE);
						try{
							Date tempDate = new Date(startDate.toString());
							startDate = sdf.format(tempDate);
						} catch(Exception e){
							try{
								startDate = sdf.format((Date)startDate);
							}catch(Exception e1){
								startDate = startDate.toString();
							}
						}

					} else {
						startDate = new Date();
						startDate= sdf.format(startDate);
					}
					deletedObj.remove(VARIABLE_ACTIVE);
					deletedObj.remove(VARIABLE_ENDDATE);
					deletedObj.remove(VARIABLE_STARTDATE);

					deletedObj.put(VARIABLE_ACTIVE, VALUE_INACTIVE);
					deletedObj.put(VARIABLE_STARTDATE, startDate);
					deletedObj.put(VARIABLE_ENDDATE, currDate);

					returnOriginalArray.put(deletedObj);
					//deletedObj = removeType2Columns(deletedObj);
					JSONObject newObject = new JSONObject();
					Iterator itr = deletedObj.keys();
					while(itr.hasNext()){
						String key = itr.next().toString();
						newObject.put(key, deletedObj.get(key));
					}
					newObject = removeType2Columns(newObject);
					newObject.put(VARIABLE_ACTIVE, VALUE_INACTIVE);
					newObject.put(VARIABLE_ACTIVITY, VALUE_ACTIVITY_DELETED_RECORD);
					int secondsFromDate = Integer.valueOf(currDate.substring(currDate.lastIndexOf(":")+1,currDate.length()))+1;
					String newSeconds = String.valueOf((secondsFromDate < 10 ? "0" : "") + secondsFromDate);
					String firstString = currDate.substring(0,currDate.lastIndexOf(":"));
					String dateIncreamenet = firstString+":"+newSeconds;
					//String dateIncreamenet = currDate.replace(currDate.substring(currDate.lastIndexOf(":")+1,currDate.length()),String.valueOf(secondsFromDate));
					newObject.put(VARIABLE_STARTDATE, dateIncreamenet);
					newObject.put(VARIABLE_ENDDATE, VALUE_CURRENT_ACTIVE_DATE);
					returnOriginalArray.put(newObject);

					recordsDeleted++;

				}
				else{

					returnOriginalArray.put(deletedObj);
				}




			}

		}

		if (returnBothArrays) {
			returnJsonArray.put(0, returnOriginalArray);
			returnJsonArray.put(1, returnnNewArray);
		} else {

			returnJsonArray.put(0, returnOriginalArray);
		}
		return formatJsonOutput(returnJsonArray, schemaString);

	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		// String table1 = "[{\"EMP_NAME\":\"Onkar
		// Pathak\",\"EMP_ID\":1234,\"EMP_DESGN\":\"SPE\",\"EMP_SALARY\":55000,\"EMP_CITY\":\"Mumbai\",\"EMP_AGE\":25},{\"EMP_NAME\":\"Onkar
		// Joshi\",\"EMP_ID\":1289,\"EMP_DESGN\":\"DE\",\"EMP_SALARY\":46000,\"EMP_CITY\":\"Kolkata\",\"EMP_AGE\":24},{\"EMP_NAME\":\"Sagar
		// K\",\"EMP_ID\":5897,\"EMP_DESGN\":\"SA\",\"EMP_SALARY\":75000,\"EMP_CITY\":\"Mumbai\",\"EMP_AGE\":30},{\"EMP_NAME\":\"Rishabh
		// Mishra\",\"EMP_ID\":8745,\"EMP_DESGN\":\"CONS\",\"EMP_SALARY\":55000,\"EMP_CITY\":\"Hyderabad\",\"EMP_AGE\":40},{\"TableName\":\"EMPLOYEE_NEW\"}][{\"EMP_NAME\":\"Onkar
		// Pathak\",\"EMP_ID\":1234,\"EMP_DESGN\":\"PE\",\"EMP_SALARY\":25000,\"EMP_CITY\":\"Mumbai\",\"EMP_JOINING_DATE\":\"17-12-1992\"},{\"EMP_NAME\":\"Onkar
		// Joshi\",\"EMP_ID\":1289,\"EMP_DESGN\":\"DE\",\"EMP_SALARY\":26000,\"EMP_CITY\":\"Pune\",\"EMP_JOINING_DATE\":\"10-06-2015\"},{\"EMP_NAME\":\"Sagar
		// K\",\"EMP_ID\":5897,\"EMP_DESGN\":\"SDE\",\"EMP_SALARY\":95000,\"EMP_CITY\":\"Mumbai\",\"EMP_JOINING_DATE\":\"08-10-2004\"},{\"EMP_NAME\":\"Praveen\",\"EMP_ID\":6432,\"EMP_DESGN\":\"BDE\",\"EMP_SALARY\":85000,\"EMP_CITY\":\"Delhi\",\"EMP_JOINING_DATE\":\"23-08-2010\"},{\"TableName\":\"EMPLOYEE_OLD\"}]";
		// String table1 = "[{\"EMP_NAME\":\"Onkar
		// Pathak\",\"EMP_ID\":1234,\"EMP_DESGN\":\"SPE\",\"EMP_SALARY\":55000,\"EMP_CITY\":\"Mumbai\"},{\"EMP_NAME\":\"Onkar
		// Joshi\",\"EMP_ID\":1289,\"EMP_DESGN\":\"DE\",\"EMP_SALARY\":46000,\"EMP_CITY\":\"Kolkata\"},{\"EMP_NAME\":\"Sagar
		// K\",\"EMP_ID\":5897,\"EMP_DESGN\":\"SA\",\"EMP_SALARY\":75000,\"EMP_CITY\":\"Mumbai\"},{\"EMP_NAME\":\"Rishabh
		// Mishra\",\"EMP_ID\":8745,\"EMP_DESGN\":\"CONS\",\"EMP_SALARY\":55000,\"EMP_CITY\":\"Hyderabad\"},{\"TableName\":\"EMPLOYEE_NEW\"}][{\"TableName\":\"EmptyTable\"}]";
		String table1 = "{  \"type\":\"record\", \"name\":\"tblname\",  \"fields\":[ {    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"off_code_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"P_I_N_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"TEAM_pers_last_name_pers_first_name_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"Blank_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"Blank2_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"trx_type_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"job_title_name_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"Unit_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"SUPER_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"Supervisor_PIN_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"Team_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"pers_last_name_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"pers_first_name_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"BCMS_NAME_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"STATUS_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"Effective_Date_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"SDN_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"string\"    ],     \"name\": \"PhoneExt_s\"},{    \"default\":null,     \"type\": [        \"null\",         \"boolean\"    ],     \"name\": \"HomeOffice_b\"} ]  }";
		String appendString = "{\"default\":null,\"type\": [\"null\",\"string\"],\"name\": \"Active\"},{\"default\":null,\"type\": [\"null\",\"string\"],\"name\":\"Activity\"}, {\"default\":null,\"type\": [\"null\",\"string\"],\"name\": \"StartDate\"},{\"default\":null,\"type\": [\"null\",\"string\"],\"name\": \"EndDate\"}]}";
		table1 = table1.replaceAll("\\s+", "");
		// StringBuilder stb = new
		// StringBuilder(StringEscapeUtils.unescapeJava(table1));
		if (table1.endsWith("]}")) {
			table1 = table1.replace("]}", "");
			table1 = table1.concat("," + appendString);
			table1 = table1.replaceAll("\"null\"", null);
		}

		BufferedReader br = new BufferedReader(
				new FileReader(new File("C:\\My Data\\OnkarPathak\\LM\\Type2TestingTemplate\\37691309398182")));
		String line = "";
		StringBuilder stb = new StringBuilder();
		while ((line = br.readLine()) != null) {

			stb.append(line);
		}
		JSONArray test = new JSONArray();
		String allData = stb.toString().replace("][", "###");
		String table[] = allData.split("###");
		String emptyTableName = "[{\"TableName\":\"EmptyTable\"}]";
		try {
			for (int i = 0; i < table.length; i++) {
				String currTable = table[i];
				if (!currTable.startsWith("[")) {
					currTable = "[" + currTable;
				}
				if (!currTable.endsWith("]")) {
					currTable = currTable + "]";
				}
				if (!emptyTableName.equals(currTable)) {
					JSONArray jsonTable = new JSONArray(currTable);
					test.put(jsonTable);
				}

			}
			CompareJsonTables jsonTables = new CompareJsonTables();
			String escapeVals = "EMP_DESGN,EMP_CITY";
			if (test.length() > 1) {
				System.out.println(jsonTables.compareJSONTables(test, "P_I_N_s", null, ""));
			} else if (test.length() == 1) {
				// System.out.println(jsonTables.firstTableIngestion(test),"");
			}
			System.out.println("No. of Unchanged Records : " + recordsUnchanged + "\nNo. of Records Modified : "
					+ recordsModified + "\nNo. of Records Deleted : " + recordsDeleted + "\nNo. of Records Added : "
					+ recordsAdded);
			// JSONArray outputTables = jsonTables.compareJSONTables(test,
			// "EMP_ID", escapeVals);
			// System.out.println(outputTables.toString());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
