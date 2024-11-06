
import json
import base64
import boto3
import time
import string


def lambda_handler(event, context):

    payload_data = event['PayloadData']
    t = event['WirelessMetadata']['LoRaWAN']['Timestamp']
    deveui = event['WirelessMetadata']['LoRaWAN']['DevEui']
    
    if deveui == 'a123456b789123c4':  # Enter the 16-digit alphanumeric Device EUI for the soil moisture sensor here; all letters should be lowercase
        result = decode_soil_moisture_payload(payload_data)
        print('soil moisture sensor')
        txt_file_name = "smdata.txt"  # Name of soil moisture txt file stored in S3 bucket
        
    elif deveui == 'a567891b234567c8':  # Enter the 16-digit alphanumeric Device EUI for the water level sensor here; all letters should be lowercase
        fport = 2
        probe_length_m = 4.2  # Actual (measured) length of probe in meters
        result = decode_water_level_payload(payload_data, probe_length_m, fport)
        print('water level sensor')
        txt_file_name = "water_level_data.txt"  # Name of water level txt file stored in S3 bucket
        
    #print(f"Payload data is: {payload_data}")
    #print(f"Timestamp: {t}")
    print(f"Result: {result}")
    
    data = {"Timestamp": t,
            "Encoded Payload": payload_data,
            "Payload Data": result
    }
    
    bucket_name = "bucket-name"  # Name of S3 bucket where soil moisture and water level text files are stored
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket_name, txt_file_name)
    existing_data = obj.get()['Body'].read().decode('utf-8') 
    new_data = existing_data + '\n' + json.dumps(data)

    # print(f"Existing data: {existing_data}")
    # print(f"New data: {new_data}")
        
    s3.Bucket(bucket_name).put_object(Key=txt_file_name, Body=new_data)
    return json.dumps(data)      


    
    
def decode_soil_moisture_payload(payload_data):
    decoded = base64.b64decode(payload_data)
    
    value = (decoded[0] << 8 | decoded[1]) & 0x3FFF
    battery_value = value/1000  # /Battery,units:V
    value = decoded[2] << 8 | decoded[3]
    if decoded[2] & 0x80:
        value |= 0xFFFF0000
    temp_DS18B20 = (value/10)  # /DS18B20,temperature,units:℃
    value = decoded[4] << 8 | decoded[5]
    water_SOIL = (value/100)  # /water_SOIL,Humidity,units:%
    value = decoded[6] << 8 | decoded[7]
    if ((value & 0x8000) >> 15) == 0:
        temp_SOIL = (value/100)  # /temp_SOIL,temperature,units:°C
    elif ((value & 0x8000) >> 15) == 1:
        temp_SOIL = ((value-0xFFFF)/100)
    value = decoded[8] << 8 | decoded[9]
    conduct_SOIL = (value/100)  # /conduct_SOIL,conductivity,units:uS/cm
    temp_SOIL_F = (9/5)*temp_SOIL + 32

    result = {
        "battery voltage (V)": battery_value,
        "soil moisture (%)":  water_SOIL,
        "soil temperature (C)":  temp_SOIL,
        "soil temperature (F)":  temp_SOIL_F,
        "soil conductivity (uS/cm)": conduct_SOIL
    }
    
    return result
    
    
def decode_water_level_payload(payload_data, probe_length_m, fport):
    global Water_deep_cm, Water_pressure_MPa, Water_pressure_kPa, DATALOG, result
    bytes = base64.b64decode(payload_data)
    tst1 = len(bytes)
    if fport == 2:

        Bat_V = (bytes[0] << 8 | bytes[1]) / 1000
        Probe_mod = bytes[2]
        probe_mod_bytes2 = bytes[2]
        probe_mod_bytes3 = bytes[3]
        IDC_intput_mA = (bytes[4] << 8 | bytes[5]) / 1000
        VDC_intput_V = (bytes[6] << 8 | bytes[7]) / 1000
        IN1_pin_level = (bytes[8] & 0x08) and "High" or "Low"
        IN2_pin_level = (bytes[8] & 0x04) and "High" or "Low"
        Exti_pin_level = (bytes[8] & 0x02) and "High" or "Low"
        Exti_status = (bytes[8] & 0x01) and "TRUE" or "FALSE"
        Water_pressure_MPa = 'null'
        Water_pressure_kPa = 'null'
        Water_deep_cm = 'null'
        if Probe_mod == 0x00:
            #if IDC_intput_mA <= 4.0:
            if IDC_intput_mA <= 4.1:
                Water_deep_cm = 0
            else:
                #Water_deep_cm = int(IDC_intput_mA - 4.0) * (bytes[3] * 100 / 16)
                Water_deep_cm = (IDC_intput_mA - 4.0) * (probe_length_m * 100 / 16)
                print('IDC_input:', IDC_intput_mA, 'bytes[3]', bytes[3])
        elif Probe_mod == 0x01:
            if IDC_intput_mA <= 4.0:
                Water_pressure_MPa = 0
                if bytes[3] == 1:
                    Water_pressure_MPa = int(IDC_intput_mA - 4.0) * 0.0375
                elif bytes[3] == 2:
                    Water_pressure_MPa = int(IDC_intput_mA - 4.0) * 0.0625
                elif bytes[3] == 3:
                    Water_pressure_MPa = int(IDC_intput_mA - 4.0) * 0.1
                elif bytes[3] == 4:
                    Water_pressure_MPa = int(IDC_intput_mA - 4.0) * 0.15625
                elif bytes[3] == 5:
                    Water_pressure_MPa = int(IDC_intput_mA - 4.0) * 0.625
                elif bytes[3] == 6:
                    Water_pressure_MPa = int(IDC_intput_mA - 4.0) * 2.5
                elif bytes[3] == 7:
                    Water_pressure_MPa = int(IDC_intput_mA - 4.0) * 3.75
                elif bytes[3] == 8:
                    Water_pressure_MPa = int(IDC_intput_mA - 4.0) - 0.00625
                elif bytes[3] == 9:
                    if IDC_intput_mA <= 12.0:
                        Water_pressure_MPa = int(IDC_intput_mA - 4.0) - 0.0125
                    else:
                        Water_pressure_MPa = int(IDC_intput_mA - 12.0) * 0.0125
                elif bytes[3] == 10:
                    Water_pressure_kPa = int(IDC_intput_mA - 4.0) * 0.3125
                elif bytes[3] == 11:
                    Water_pressure_kPa = int(IDC_intput_mA - 4.0) * 3.125
                elif bytes[3] == 12:
                    Water_pressure_kPa = int(IDC_intput_mA - 4.0) * 6.25
        result = {
            "Bat_V": Bat_V,
            #"Probe_mod": Probe_mod,
            "probe_mod_bytes2": probe_mod_bytes2,
            "probe_mod_bytes3": probe_mod_bytes3,
            "IDC_input_mA": IDC_intput_mA,
            #"VDC_input_V": VDC_intput_V,
            #"IN1_pin_level": IN1_pin_level,
            #"IN2_pin_level": IN2_pin_level,
            #"Exti_pin_level": Exti_pin_level,
            #"Exti_status": Exti_status,
            "Water_deep_cm": Water_deep_cm,
            #"Water_pressure_MPa": Water_pressure_MPa,
            #"Water_pressure_kPa": Water_pressure_kPa,
        }
    if fport == 7:
        i = 0
        while i < tst1:
            aa = int(bytes[2 + i] << 8 | bytes[3 + i] << 16) / 1000

            string = '[' + str(aa) + ']' + ','

            if i == 0:
                DATALOG = string
            else:
                DATALOG = DATALOG + string
            i = i + 11
        result = {
            "DATALOG": DATALOG,
        }
    if fport == 5:
        if bytes[0] == 0x16:
            sensor_mode = "PS-LB"
        else:
            sensor_mode = "NULL"

        if bytes[4] == 0xff:
            sub_band = "NULL"
        else:
            sub_band = bytes[4]

        if bytes[3] == 0x01:
            freq_band = "EU868"
        elif bytes[3] == 0x02:
            freq_band = "US915"
        elif bytes[3] == 0x03:
            freq_band = "IN865"
        elif bytes[3] == 0x04:
            freq_band = "AU915"
        elif bytes[3] == 0x05:
            freq_band = "KZ865"
        elif bytes[3] == 0x06:
            freq_band = "RU864"
        elif bytes[3] == 0x07:
            freq_band = "AS923"
        elif bytes[3] == 0x08:
            freq_band = "AS923_1"
        elif bytes[3] == 0x09:
            freq_band = "AS923_2"
        elif bytes[3] == 0x0A:
            freq_band = "AS923_3"
        elif bytes[3] == 0x0B:
            freq_band = "CN470"
        elif bytes[3] == 0x0C:
            freq_band = "EU433"
        elif bytes[3] == 0x0D:
            freq_band = "KR920"
        elif bytes[3] == 0x0E:
            freq_band = "MA869"

        firm_ver = str(bytes[1] & 0x0f) + '.' + str(bytes[2] >> 4 & 0x0f) + '.' + str(bytes[2] & 0x0f)
        batV = (bytes[5] << 8 | bytes[6]) / 1000
        semsor_mod = (bytes[7] >> 6) & 0x3f

        result = {
            "BatV": batV,
            "SENSOR_MODEL": sensor_mode,
            "FIRMWARE_VERSION": firm_ver,
            "FREQUENCY_BAND": freq_band,
            "SUB_BAND": sub_band,

        }

    return result    






