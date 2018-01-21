package com.lm.type2;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class AvroHandler {

	
	public static final String VARIBALE_ROWEFFECTIVE_DATE = "rowEffectiveDate";
	public static final String VARIABLE_ROWEXPIRATION_DATE = "rowExpirationDate";
	public static final String ZERO_VALUE = "000";
	public static final String FINAL_SECONDS = "590";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public static JSONArray modifyAvroDatetime(JSONArray inputArray){
		
		JSONArray returnArray = new JSONArray();
		
		for(int i=0;i<inputArray.length();i++){
			try {
				JSONObject jsonObject = new JSONObject(inputArray.get(i).toString());
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				try{
				if(jsonObject.has(VARIBALE_ROWEFFECTIVE_DATE)){
					String rowEffectiveValue = jsonObject.getString(VARIBALE_ROWEFFECTIVE_DATE);
					String splitDate[] = rowEffectiveValue.split(":");
					String timeInSeconds = splitDate[2];
					if(timeInSeconds.equals(ZERO_VALUE)){
						timeInSeconds = "00";
						splitDate[2] = timeInSeconds;
						rowEffectiveValue = splitDate[0]+":"+splitDate[1]+":"+splitDate[2];
						jsonObject.remove(VARIBALE_ROWEFFECTIVE_DATE);
						jsonObject.put(VARIBALE_ROWEFFECTIVE_DATE, rowEffectiveValue);
					}
					else {
						int secondsTime = Integer.valueOf(timeInSeconds);
						if(secondsTime > 59){
							Date date = sdf.parse(rowEffectiveValue);
							rowEffectiveValue = sdf.format(date);
							jsonObject.remove(VARIBALE_ROWEFFECTIVE_DATE);
							jsonObject.put(VARIBALE_ROWEFFECTIVE_DATE, rowEffectiveValue);
						}
					}
				}
				
				if(jsonObject.has(VARIABLE_ROWEXPIRATION_DATE)) {
					String rowExpirationValue = jsonObject.getString(VARIABLE_ROWEXPIRATION_DATE);
					String splitDate[] = rowExpirationValue.split(":");
					String timeInSeconds = splitDate[2];
					if(timeInSeconds.equals(FINAL_SECONDS)){
						
						timeInSeconds = "59";
						splitDate[2] = timeInSeconds;
						rowExpirationValue = splitDate[0]+":"+splitDate[1]+":"+splitDate[2];
						jsonObject.remove(VARIABLE_ROWEXPIRATION_DATE);
						jsonObject.put(VARIABLE_ROWEXPIRATION_DATE, rowExpirationValue);
						
					}
					else{
						
						int secondsTime = Integer.valueOf(timeInSeconds);
						if(secondsTime > 59) {
							Date date = sdf.parse(rowExpirationValue);
							rowExpirationValue = sdf.format(date);
							jsonObject.remove(VARIABLE_ROWEXPIRATION_DATE);
							jsonObject.put(VARIABLE_ROWEXPIRATION_DATE, rowExpirationValue);
						}
					}
					
				}
				returnArray.put(jsonObject);
				}catch(Exception e){
					System.out.println("Error in taking seconds from datetime string."+e);
				}
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				System.out.println("Error in Handling Avro Input Stream."+e);
				e.printStackTrace();
			}
		}
		
		return returnArray;
		
	}

}
