package com.lm.type2;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class GetAvroSchema {

	@SuppressWarnings("resource")
	public static String getAvroSchema(InputStream in) throws IOException {

		InputStream input = in;
		GenericDatumReader<GenericRecord> fileReader = null;
		String avroSchema = "";
		try {
			fileReader = new GenericDatumReader<GenericRecord>();
			DataFileStream<GenericRecord> avroStreamReader = new DataFileStream<GenericRecord>(input, fileReader);
			Schema returnSchema = avroStreamReader.getSchema();
			avroSchema = returnSchema.toString();
		} catch (Exception e) {
			System.out.println(e);
		}
		return avroSchema;
	}

	@SuppressWarnings("resource")
	public static JSONArray convertAvroToJson(InputStream in) throws JSONException {

		boolean pretty = false;
		GenericDatumReader<GenericRecord> reader = null;
		JsonEncoder encoder = null;
		ByteArrayOutputStream output = null;
		JSONArray returnJson = new JSONArray();
		try {
			reader = new GenericDatumReader<GenericRecord>();
			InputStream input = in;

			DataFileStream<GenericRecord> streamReader = new DataFileStream<GenericRecord>(input, reader);
			output = new ByteArrayOutputStream();
			Schema schema = streamReader.getSchema();
			DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
			int i = 0;
			List<Field> schemaLists = schema.getFields();
			for (GenericRecord datum : streamReader) {
				String valueString = datum.toString();
				returnJson.put(i, valueString);
				i++;
				//System.out.println(datum.toString());
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println(new String(output.toByteArray()));
			System.out.println(e);
			e.printStackTrace();
		}

		return returnJson;
	}

	public static byte[] convertAvro(byte[] data, String schemaString) throws Exception {
		JsonGenericRecordReader reader = new JsonGenericRecordReader();
		try {

			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
			Schema schema = new Schema.Parser().parse(schemaString);
			GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
			GenericRecord datum = null;
			writer.write(convertToGenericDataRecord(data, schema, reader), encoder);
			encoder.flush();
			return outputStream.toByteArray();

		} catch (IOException e) {

			throw new Exception("Failed to convert to AVRO.", e);
		}

	}

	public static GenericData.Record convertToGenericDataRecord(byte[] data, Schema schema,
			JsonGenericRecordReader reader) {
		return reader.read(data, schema);
	}

	public static byte[] jsonToAvro(JSONArray newArr, String schemaStr) throws Exception {

		DataFileWriter<GenericRecord> writer = null;
		Encoder encoder = null;
		ByteArrayOutputStream output = null;
		String data = "";
		try {
			Schema schema = new Schema.Parser().parse(schemaStr);
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			// input = new ByteArrayInputStream(json.getBytes());
			output = new ByteArrayOutputStream();

			writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>());
			GenericDatumWriter<Object> writer1 = new GenericDatumWriter<>(schema);
			writer.create(schema, output);
			for (int i = 0; i < newArr.length(); i++) {
				String object = newArr.get(i).toString();
				InputStream input = new ByteArrayInputStream(object.getBytes());
				DataInputStream din = new DataInputStream(input);
				Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
				Object datum = null;
				datum = reader.read(null, decoder);
				data = datum.toString();
				writer.append((GenericRecord) datum);
				// convertAvro(object.toString().getBytes(), stb.toString());
				// byte[] outputBytes = jsonToAvro(object.getBytes(),
				// schemaStr);
				// byte[] outputBytes =
				// convertAvro(object.toString().getBytes(), stb.toString());
				// output.write(outputBytes);
			}

			/*
			 * GenericDatumWriter<Object> w = new
			 * GenericDatumWriter<Object>(schema); ByteArrayOutputStream
			 * outputStream = new ByteArrayOutputStream();
			 * 
			 * Encoder e = EncoderFactory.get().binaryEncoder(outputStream,
			 * null);
			 * 
			 * w.write(datum, e); e.flush();
			 */

			/*
			 * while (true) { try { datum = reader.read(null, decoder); } catch
			 * (EOFException eofe) { break; } writer.append((GenericRecord)
			 * datum); }
			 */
			writer.flush();

		} catch (Exception e) {
			System.out.println(e);
			System.out.println(data);

		}
		return output.toByteArray();
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		try {
			String record1 = "{\"off_code_s\":\"096\",\"P_I_N_s\":\"N0000110\",\"TEAM_pers_last_name_pers_first_name_s\":\"MC1-PHINNEY, LINDA\",\"Blank_s\":\"BLANK\",\"Blank2_s\":\"BLANK\",\"trx_type_s\":\"\",\"job_title_name_s\":\"MED & DISABILITY CASE MGR\",\"Unit_s\":\"MC\",\"SUPER_s\":\"GEORGE, MICHAEL\",\"Supervisor_PIN_s\":\"N0114527\",\"Team_s\":\"MC1\",\"pers_last_name_s\":\"PHINNEY\",\"pers_first_name_s\":\"LINDA\",\"BCMS_NAME_s\":\"\",\"STATUS_s\":\"Transferred\",\"Effective_Date_s\":\"2009-09-01 00:00:00\",\"SDN_s\":\"845\",\"PhoneExt_s\":\"30534\",\"HomeOffice_b\":false,\"rowActive\":\"1\",\"rowActivity\":\"New Record\",\"rowEffectiveDate\":\"2017-10-31 13:14:49\",\"rowExpirationDate\":\"9999-12-31 00:00:00\"}";
			String record2 = "{\"off_code_s\":\"096\",\"P_I_N_s\":\"N0000235\",\"TEAM_pers_last_name_pers_first_name_s\":\"MC01-WALLACE, KATHLEEN\",\"Blank_s\":\"BLANK\",\"Blank2_s\":\"BLANK\",\"trx_type_s\":\"\",\"job_title_name_s\":\"PROJECT LEADER-SPECIAL PROJ\",\"Unit_s\":\"TAX & OP\",\"SUPER_s\":\"NELSON, JANE\",\"Supervisor_PIN_s\":\"N0065455\",\"Team_s\":\"MC01\",\"pers_last_name_s\":\"WALLACE\",\"pers_first_name_s\":\"KATHLEEN\",\"BCMS_NAME_s\":\"\",\"STATUS_s\":\"Terminated\",\"Effective_Date_s\":\"2012-08-27 00:00:00\",\"SDN_s\":\"845\",\"PhoneExt_s\":\"26651\",\"HomeOffice_b\":true,\"rowActive\":\"1\",\"rowActivity\":\"New Record\",\"rowEffectiveDate\":\"2017-10-31 13:16:49\",\"rowExpirationDate\":\"9999-12-31 00:00:00\"}";
			
			//File avroFile = new File("C:\\My Data\\OnkarPathak\\LM\\Type2Datetime_Modifications\\History\\cd_admin_rc_adm_hist.avro");
   			InputStream instream = new FileInputStream(
					new File("C:\\My Data\\OnkarPathak\\LM\\Type2Datetime_Modifications\\Current\\cd_admin_rc_adm.avro"));
			// in.read();
			// System.out.println(in.toString());
			//JSONArray jsonArr = null;
			JSONArray jsonArray2 = convertAvroToJson(instream);
			
			JSONArray jsonArray3 =AvroHandler.modifyAvroDatetime(jsonArray2);
			
			BufferedReader br = new BufferedReader(
					new FileReader("C:\\My Data\\OnkarPathak\\LM\\Type2TestingTemplate\\RCAdminSchema_1.avsc"));
			String lineSchema = null;
			StringBuilder stb1 = new StringBuilder();
			while ((lineSchema = br.readLine()) != null) {
				stb1.append(lineSchema);

			}
			
			
			
			
			String schemaString = stb1.toString();
			schemaString = schemaString.replace("[null,string]", "string");
			schemaString = schemaString.replace("[\"null\",\"string\"]", "\"string\"");
			schemaString = schemaString.replace("[\"null\",\"boolean\"]", "\"boolean\"");
			schemaString = schemaString.replace("[\"null\",\"int\"]", "\"int\"");
			schemaString = schemaString.replace("[\"null\",\"long\"]", "\"long\"");
			schemaString = schemaString.replace("[\"null\",\"double\"]", "\"double\"");
			br.close();

			/*
			 * BufferedReader jsonReader1 = new BufferedReader(new
			 * FileReader("C:\\My Data\\OnkarPathak\\LM\\Type2TestingTemplate\\Type2Json.json"
			 * )); String jsonLine1 = null; StringBuilder jsonBuilder1 = new
			 * StringBuilder(); while((jsonLine1=jsonReader1.readLine())!=null){
			 * jsonBuilder1.append(jsonLine1); } JSONArray jsonA = new
			 * JSONArray(jsonBuilder1.toString()); byte[] byData =
			 * jsonToAvro(jsonA, schemaString); OutputStream st = new
			 * ByteArrayOutputStream(); st.write(byData);
			 */
			//String record1 = "{\"off_code_s\":\"185\",\"P_I_N_s\":\"N0000000\",\"TEAM_pers_last_name_pers_first_name_s\":\"W16-SMITH, JOHN\",\"Blank_s\":\"BLANK\",\"Blank2_s\":\"BLANK\",\"trx_type_s\":\"\",\"job_title_name_s\":\"ACCOUNTANT\",\"Unit_s\":\"BFT\",\"SUPER_s\":\"BUTTERWORTH, MICHAEL\",\"Supervisor_PIN_s\":\"N0148158\",\"Team_s\":\"W16\",\"pers_last_name_s\":\"SMITH\",\"pers_first_name_s\":\"JOHN\",\"BCMS_NAME_s\":\"\",\"STATUS_s\":\"Terminated\",\"Effective_Date_s\":\"2014-04-24 00:00:00\",\"SDN_s\":\"\",\"PhoneExt_s\":\"\",\"HomeOffice_b\":false,\"rowActive\":\"1\",\"rowActivity\":\"New Record\",\"rowEffectiveDate\":\"2017-10-31 03:42:49\",\"rowExpirationDate\":\"9999-12-31 00:00:00\"}";

			//JSONObject j1 = new JSONObject(record1);
			BufferedReader bre = new BufferedReader(
					new FileReader("C:\\My Data\\OnkarPathak\\LM\\Output.json"));
			String line = "";
			String errors = "";
			StringBuilder stb = new StringBuilder();
			while ((line = bre.readLine()) != null) {

				stb.append(line);
			}
			
			JSONArray originalArray = new JSONArray(stb.toString());
			JSONArray newArray = CompareJsonTables.removeJsonObjects("P_I_N_s", "N0227728#N0285709", originalArray, schemaString);
			newArray.put(record1);
			newArray.put(record2);
			/*
			 * JSONArray allTablesArray = new JSONArray(stb.toString()); byte[]
			 * newAvro = jsonToAvro(allTablesArray, schemaString);
			 * ByteArrayOutputStream st = new ByteArrayOutputStream();
			 * st.write(newAvro); File file = new
			 * File("C:\\My Data\\OnkarPathak\\LM\\Type2TestingTemplate\\TestAvroNew_1.avro"
			 * ); if(!file.exists()){ file.createNewFile(); } FileOutputStream
			 * fos = new FileOutputStream(file); st.writeTo(fos); fos.close();
			 */
			JSONArray test = new JSONArray();
			JSONArray returnArray = new JSONArray();
			String allData = stb.toString().replace("][", "###");
			String[] allTables = allData.split("###");
			String emptyTableName = "[{\"TableName\":\"EmptyTable\"}]";

			try {
				int index = 0;
				for (int i = 0; i < allTables.length; i++) {
					String currTable = allTables[i];
					if (!currTable.startsWith("[")) {
						currTable = "[" + currTable;
					}
					if (!currTable.endsWith("]")) {
						currTable = currTable + "]";
					}

					if (!emptyTableName.equals(currTable)) {
						JSONArray jsonTable = new JSONArray(currTable);
						jsonTable.get(index).toString();
						test.put(index, jsonTable);
						index = index + 1;
					}
				}
				CompareJsonTables jsonTables = new CompareJsonTables();
				JSONArray comparedTableData = new JSONArray();
				if (test.length() == 1) {
					try {
						returnArray = jsonTables.firstTableIngestion(test.getJSONArray(0), schemaString);
						System.out.println("Newly added records : "+CompareJsonTables.recordsAdded);
						ByteArrayOutputStream ous = new ByteArrayOutputStream();
						ous.write(returnArray.toString().getBytes());
						File file = new File("C:\\My Data\\OnkarPathak\\LM\\Type2TestingTemplate\\Onkar\\FirstInput\\Output.json");

						if(!file.exists()){
							file.createNewFile();
						}
						FileOutputStream fos = new FileOutputStream(file);
						ous.writeTo(fos);
						ous.close();
						InputStream ins = new ByteArrayInputStream(returnArray.toString().getBytes());
						StringWriter writer = new StringWriter();
						IOUtils.copy(ins, writer, "UTF-8");
						JSONArray jsonArray = new JSONArray(writer.toString());
						byte[] firstAvro = jsonToAvro(jsonArray, schemaString);
						File outputFile = new File("C:\\My Data\\OnkarPathak\\LM\\Type2TestingTemplate\\Onkar\\FirstInput\\Output.avro");
						if(!outputFile.exists()){
							outputFile.createNewFile();
						}

						FileOutputStream fos1 = new FileOutputStream(outputFile);
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						baos.write(firstAvro);
						baos.writeTo(fos1);
						baos.close();
						fos1.close();
					} catch (Exception e) {
						errors = errors + "  " + e.toString();
					}

				} else if (test.length() > 1) {
					// returnArray =
					// jsonTables.compareJSONTables(test,"P_I_N_s","");
					try {
						returnArray = jsonTables.compareJSONTables(test, "P_I_N_s", "", schemaString);

						System.out.println(jsonTables.getRecordsAdded() + " Deleted : " + jsonTables.getRecordsDeleted()
						+ " Modified : " + jsonTables.getRecordsModified() + " Unchanged : "
						+ jsonTables.getRecordsUnchanged());
						ByteArrayOutputStream ous = new ByteArrayOutputStream();
						ous.write(returnArray.toString().getBytes());
						File file = new File("C:\\My Data\\OnkarPathak\\LM\\Type2TestingTemplate\\Onkar\\TestCases\\Prod\\Output0711.json");

						if(!file.exists()){
							file.createNewFile();
						}
						FileOutputStream fos = new FileOutputStream(file);
						ous.writeTo(fos);
						ous.close();
						InputStream ins = new ByteArrayInputStream(returnArray.toString().getBytes());
						StringWriter writer = new StringWriter();
						IOUtils.copy(ins, writer, "UTF-8");
						JSONArray jsonArray = new JSONArray(writer.toString());
						byte[] firstAvro = jsonToAvro(jsonArray, schemaString);
						File outputFile = new File("C:\\My Data\\OnkarPathak\\LM\\Type2TestingTemplate\\Onkar\\TestCases\\Prod\\Output0711.avro");
						if(!outputFile.exists()){
							outputFile.createNewFile();
						}

						FileOutputStream fos1 = new FileOutputStream(outputFile);
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						baos.write(firstAvro);
						baos.writeTo(fos1);
						baos.close();
						fos1.close();




					} catch (Exception e) {
						errors = errors + "  " + e.toString();
						e.printStackTrace();
					}

				}

			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			InputStream in = new FileInputStream(
					new File("C:\\My Data\\OnkarPathak\\LM\\Type2TestingTemplate\\newRcAdmin.avro"));
			// in.read();
			// System.out.println(in.toString());
			JSONArray jsonArr = null;
			// JSONArray jsonArr = convertAvroToJson(in);
			JSONObject tableValue = new JSONObject();
			tableValue.put("TableName", "Table_OLD");
			CompareJsonTables compare = new CompareJsonTables();

			// InputStream instr = new
			// ByteArrayInputStream(jsonArr.toString().getBytes());
			// OutputStream strm1 = new ByteArrayOutputStream();
			// strm1.write(jsonArr.toString().getBytes(StandardCharsets.UTF_8));

			BufferedReader jsonReader = new BufferedReader(
					new FileReader("C:\\My Data\\OnkarPathak\\LM\\Type2TestingTemplate\\Testing\\TestNewJson.json"));
			String jsonLine = null;
			StringBuilder jsonBuilder = new StringBuilder();
			while ((jsonLine = jsonReader.readLine()) != null) {
				jsonBuilder.append(jsonLine);
			}

			jsonReader.close();
			JSONArray oldJsonArray = new JSONArray(jsonBuilder.toString());

			JSONArray avroJsonArray = new JSONArray();
			for (int i = 0; i < oldJsonArray.length(); i++) {
				JSONObject object = new JSONObject(oldJsonArray.getString(i));
				avroJsonArray.put(object);
			}

			JSONObject oldTableValue = new JSONObject();
			oldTableValue.put("TableName", "Table_OLD");
			avroJsonArray.put(oldTableValue);

			BufferedReader jsonReaderNew = new BufferedReader(
					new FileReader("C:\\My Data\\OnkarPathak\\LM\\Type2TestingTemplate\\TestNewJSON.json"));
			String jsonLineNew = null;
			StringBuilder jsonBuilderNew = new StringBuilder();
			while ((jsonLineNew = jsonReaderNew.readLine()) != null) {
				jsonBuilderNew.append(jsonLineNew);
			}

			jsonReader.close();
			JSONArray newJsonArray = new JSONArray(jsonBuilderNew.toString());
			JSONArray avroJsonArrayNew = new JSONArray();
			for (int i = 0; i < newJsonArray.length(); i++) {
				JSONObject object = new JSONObject(newJsonArray.getString(i));
				avroJsonArrayNew.put(object);
			}
			JSONObject TableValue = new JSONObject();
			TableValue.put("TableName", "Table_NEW");
			avroJsonArrayNew.put(TableValue);

			JSONArray compareJsonArrays = new JSONArray();
			compareJsonArrays.put(0, avroJsonArray);
			compareJsonArrays.put(1, avroJsonArrayNew);

			String jsonValue = newJsonArray.toString();
			BufferedReader br1 = new BufferedReader(
					new FileReader("C:\\My Data\\OnkarPathak\\LM\\Type2TestingTemplate\\RCAdminSchema.avsc"));
			String lineSchema1 = null;
			StringBuilder stb11 = new StringBuilder();
			while ((lineSchema = br1.readLine()) != null) {
				stb1.append(lineSchema);

			}
			String schemaString1 = stb.toString();
			schemaString = schemaString.replace("[null,string]", "string");
			schemaString = schemaString.replace("[\"null\",\"string\"]", "\"string\"");
			schemaString = schemaString.replace("[\"null\",\"boolean\"]", "\"boolean\"");
			schemaString = schemaString.replace("[\"null\",\"int\"]", "\"int\"");
			schemaString = schemaString.replace("[\"null\",\"long\"]", "\"long\"");
			schemaString = schemaString.replace("[\"null\",\"double\"]", "\"double\"");
			br.close();
			JSONArray outputArray = compare.compareJSONTables(compareJsonArrays, "P_I_N_s", "", schemaString);
			System.out.println("Records Added : " + compare.getRecordsAdded() + " Records Deleted : "
					+ compare.getRecordsDeleted() + " Records Modified : " + compare.getRecordsModified()
					+ " Records Unchanged : " + compare.getRecordsUnchanged());
			byte[] avroOutput = GetAvroSchema.jsonToAvro(outputArray, schemaString);
			OutputStream avroStream = new ByteArrayOutputStream();
			avroStream.write(avroOutput);
			// compare.firstTableIngestion(jsonArr,schemaString);
			// JSONArray outputArray =
			// compare.compareJSONTables(compareJsonArrays, "P_I_N_s", "");
			// convertAvro(jsonBuilder.toString().getBytes(), stb.toString());
			InputStream ins = new ByteArrayInputStream(jsonArr.toString().getBytes());
			StringWriter writer = new StringWriter();
			IOUtils.copy(ins, writer, "UTF-8");
			JSONArray newArr = new JSONArray(writer.toString());
			OutputStream strm = new ByteArrayOutputStream();
			byte[] avroBytes = jsonToAvro(newArr, schemaString);
			strm.write(avroBytes);
			// jsonToAvro(ins, stb.toString());
			JSONArray jsonString = convertAvroToJson(null);
			String jsonData1 = "{\"EMP_NAME\":\"Onkar Pathak\",\"EMP_ID\":\"3165\",\"EMP_DESIGNATION\":\"SE\",\"EMP_SALARY\":\"25000\",\"EMP_ADDRESS\":\"Aundh, Pune\"}{\"EMP_NAME\":\"Rishabh Mishra\",\"EMP_ID\":\"3451\",\"EMP_DESIGNATION\":\"PE\",\"EMP_SALARY\":\"50000\",\"EMP_ADDRESS\":\"Swargate\"}";
			System.out.println(jsonString);
			// InputStream ins1 = new ByteArrayInputStream(jsonData.getBytes());
			DataInputStream din = new DataInputStream(ins);
			InputStream inStream = new FileInputStream(
					new File("C:\\My Data\\OnkarPathak\\LM\\Type2TestingTemplate\\twitter.avro"));
			// String sc =
			// "{\"type\":\"record\",\"name\":\"twitter_schema\",\"namespace\":\"com.miguno.avro\",\"fields\":[{\"name\":\"username\",\"type\":\"string\",\"doc\":\"Name
			// of the user account on
			// Twitter.com\"},{\"name\":\"tweet\",\"type\":\"string\",\"doc\":\"The
			// content of the user's Twitter
			// message\"},{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"Unix
			// epoch time in milliseconds\"}],\"doc:\":\"A basic schema for
			// storing Twitter messages\"}";
			// String sc = "{\"namespace\": \"TestDB.avro\",\"type\":
			// \"record\",\"name\": \"employee\",\"fields\": [{\"name\":
			// \"EMP_ID\", \"type\": \"string\"},{\"name\":
			// \"EMP_NAME\",\"type\": \"string\"},{\"name\":
			// \"EMP_ADDRESS\",\"type\":
			// \"string\"},{\"name\":\"EMP_DESIGNATION\",\"type\":
			// \"string\"},{\"name\": \"EMP_SALARY\",\"type\": \"string\"}]}";
			String sc = "{\"type\":\"record\",\"name\":\"tblname\",\"fields\":[{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"off_code_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"P_I_N_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"TEAM_pers_last_name_pers_first_name_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"Blank_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"Blank2_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"trx_type_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"job_title_name_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"Unit_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"SUPER_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"Supervisor_PIN_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"Team_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"pers_last_name_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"pers_first_name_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"BCMS_NAME_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"STATUS_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"Effective_Date_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"SDN_s\"},{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"PhoneExt_s\"},{\"default\":null,\"type\":[\"null\",\"boolean\"],\"name\":\"HomeOffice_b\"},{\"default\":null,\"type\": [\"null\",\"string\"],\"name\": \"Active\"},{\"default\":null,\"type\": [\"null\",\"string\"],\"name\":\"Activity\"}, {\"default\":null,\"type\": [\"null\",\"string\"],\"name\": \"StartDate\"},{\"default\":null,\"type\": [\"null\",\"string\"],\"name\": \"EndDate\"}]}";
			File inputFile = new File("C:\\Users\\n0316560\\Downloads\\27606044293898");
			InputStream fileStream = new FileInputStream(inputFile);
			// byte[] outputAvro = jsonToAvro(fileStream, sc);
			System.out.println(sc);
		} catch (Exception e) {
			System.out.println(e);
			e.getMessage();

		}
	}
}