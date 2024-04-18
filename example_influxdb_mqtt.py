#!/usr/bin/env python3

import argparse
import sys
import time
import copy
import re
import math

from influxdb_client import InfluxDBClient
from influxdb_client .client.write_api import SYNCHRONOUS
import requests
import solaredge_modbus

import paho.mqtt.client as mqttClient
import json

previous_values = {}

mqtt_topic_prefix = "solaredge_test"

unitclass_table = [ # order unfortunately matters
    { "re": "voltage",     "class": "voltage",        "unit": "V" },
    { "re": "current",     "class": "current",        "unit": "A" },
    { "re": "apparent",    "class": "apparent_power", "unit": "VA" },
    { "re": "factor",      "class": "power_factor",   "unit": "%" },
    { "re": "power",       "class": "power",          "unit": "W" },
    { "re": "temperature", "class": "power",          "unit": chr(176)+"C" },
    { "re": "frequency",   "class": "frequency",      "unit": "Hz" },
]
for idx,e in enumerate(unitclass_table):
    e = unitclass_table[idx]
    e["re"] = re.compile(e["re"])

def ha_mqtt_devclass(name):
    for e in unitclass_table:
        if re.search(e["re"], name):
            return e["class"]

def ha_mqtt_devunit(name):
    for e in unitclass_table:
        if re.search(e["re"], name):
            return e["unit"]

def fetchData(inverter):
    values = {}
    values = inverter.read_all()

    current_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    if not values: # this is a daemon, try to keep going
    # FIXME: add a logged error
        return
    if not values["c_model"]:
        return

    inverter_data = {
        "measurement": "inverter",
        "tags": {
            "c_manufacturer": values["c_manufacturer"],
            "c_model": values["c_model"],
            "c_version": values["c_version"],
            "c_serialnumber": values["c_serialnumber"],
            "c_deviceaddress": values["c_deviceaddress"],
            "c_sunspec_did": values["c_sunspec_did"]
        },
        "time": current_time,
        "fields": {}
    }

    if ( ( int(values["status"]) <= 2 ) and ( int(values["status"]) >= 0 ) ):
        if( ( int(float(values["temperature"])) == int(0) ) or ( int(float(values["l1_voltage"])) == int(0) ) ):
        # ignore what looks like a periodic reboot
            return
    for k, v in values.items():
        if (isinstance(v, int) or isinstance(v, float)) and "_scale" not in k:
            k_split = k.split("_")
            scale = 0

            if f"{k_split[len(k_split) - 1]}_scale" in values:
                scale = values[f"{k_split[len(k_split) - 1]}_scale"]
            elif f"{k}_scale" in values:
                scale = values[f"{k}_scale"]

            inverter_data["fields"].update({ k: round( float(v * (10 ** scale)), 8 ) })

    device_mqtt_data = copy.deepcopy(inverter_data)
    device_mqtt_topic = "{0}/{1}".format(mqtt_topic_prefix, values['c_serialnumber'])
    device_mqtt_data = device_mqtt_data["fields"]
    for uselessKey in ["c_did", "c_length", "c_sunspec_did", "c_sunspec_length"]:
        del device_mqtt_data[uselessKey]
    json_string = json.dumps(device_mqtt_data)
    mqttc.publish(device_mqtt_topic, json_string)

    ha_entities = [ "l1_current", "l1_voltage", #FIXME: 3-phase needs l1, l2, l3
        "power_apparent", "power_reactive", "power_factor", "power_ac",
        "frequency",
        "voltage_dc", "power_dc",
        "temperature"]
    for ha_entity in ha_entities:
        device_mqtt_metadata_topic_prefix = "homeassistant/sensor/solaredge_" + values['c_serialnumber']
        device_mqtt_metadata_topic = device_mqtt_metadata_topic_prefix + "/" + ha_entity + "/config"
        device_mqtt_metadata = { "state_topic": device_mqtt_topic, "state_class": "measurement" }
        device_mqtt_metadata["name"] = ha_entity
        device_mqtt_metadata["device"] = { "name": "SolarEdge " + values['c_serialnumber'] }
        device_mqtt_metadata["device"]["identifiers"] = "solaredge_" + values['c_serialnumber']
        device_mqtt_metadata["device_class"] = ha_mqtt_devclass(ha_entity)
        device_mqtt_metadata["unit_of_measurement"] = ha_mqtt_devunit(ha_entity)
        device_mqtt_metadata["unique_id"] = "solaredge_" + values['c_serialnumber'] + "_" + ha_entity
        device_mqtt_metadata["value_template"] = "{{ value_json."+ha_entity + " }}"
        mqttc.publish(device_mqtt_metadata_topic, json.dumps(device_mqtt_metadata))

    #print(f"mqtt JSON data {mqtt_topic}")
    #print(json_string)
    json_body.append(inverter_data)

    #meters = inverter.meters()
    #batteries = inverter.batteries()
    meters = {}
    batteries = {}
    for meter, params in meters.items():
        meter_values = params.read_all()

        meter_data = {
            "measurement": "meter",
            "tags": {
                "c_manufacturer": meter_values["c_manufacturer"],
                "c_model": meter_values["c_model"],
                "c_option": meter_values["c_option"],
                "c_version": meter_values["c_version"],
                "c_serialnumber": meter_values["c_serialnumber"],
                "c_deviceaddress": meter_values["c_deviceaddress"],
                "c_sunspec_did": meter_values["c_sunspec_did"]
            },
            "time": current_time,
            "fields": {}
        }

        if ( ( int(meter_values["status"]) <= 2 ) and ( int(meter_values["status"]) >= 0 ) ):
            if( ( int(float(meter_values["temperature"])) == int(0) ) or ( int(float(meter_values["l1_voltage"])) == int(0) ) ):
            # ignore what looks like a periodic reboot
                continue
        #elif ( ! (isinstance(meter_values["temperature"], int) or isinstance(meter_values["temperature"], float)):
        #    continue
        #elif ( ! (isinstance(meter_values["l1_voltage"], int) or isinstance(meter_values["l1_voltage"], float)):
        #    continue
        #elif ( (values["c_serialnumber"] in previous_values.keys()) and  ):
        for k, v in meter_values.items():
            if (isinstance(v, int) or isinstance(v, float)) and "_scale" not in k:
                k_split = k.split("_")
                scale = 0

                if f"{k_split[len(k_split) - 1]}_scale" in meter_values:
                    scale = meter_values[f"{k_split[len(k_split) - 1]}_scale"]
                elif f"{k}_scale" in meter_values:
                    scale = meter_values[f"{k}_scale"]

                meter_data["fields"].update({k: float(v * (10 ** scale))})

        json_body.append(meter_data)
        # cache previous values, so we can skip if
        previous_values[meter_data["tags"]["c_serialnumber"]] = meter_data



    for battery, params in batteries.items():
        battery_values = params.read_all()

        if not battery_values:
            continue
        if not battery_values["c_model"]:
            continue

        battery_data = {
            "measurement": "battery",
            "tags": {
                "c_manufacturer": battery_values["c_manufacturer"],
                "c_model": battery_values["c_model"],
                "c_version": battery_values["c_version"],
                "c_serialnumber": battery_values["c_serialnumber"],
                "c_deviceaddress": battery_values["c_deviceaddress"],
                "c_sunspec_did": battery_values["c_sunspec_did"]
            },
            "time": current_time,
            "fields": {}
        }

        for k, v in battery_values.items():
            if isinstance(v, int) or isinstance(v, float):
                battery_data["fields"].update({k: v})

        json_body.append(battery_data)



inverters = []

def on_mqtt_message(client, userdata, msg):
    pass # we don't want to care, we only want to publish

def on_mqtt_connection(client, userdata, flags, reason_code):
    if reason_code.is_failure:
        print(f"MQTT connection failed: " + connack_string(reason_code))
        sys.exit()

def on_mqtt_disconnect(client, userdata, reason_code):
    print(f"MQTT disconnected: {reason_code}")
    sys.exit()

mqttc = mqttClient.Client() #(mqttClient.CallbackAPIVersion.VERSION1)
mqttc.on_message = on_mqtt_message
mqttc.on_connect = on_mqtt_connection
mqttc.on_disconnect = on_mqtt_disconnect

if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("host", type=str, help="Modbus TCP address")
    argparser.add_argument("port", type=int, help="Modbus TCP port")
    argparser.add_argument("--timeout", type=int, default=1, help="Connection timeout")
    argparser.add_argument("--unit", type=str, default=1, help="Modbus device address")
    argparser.add_argument("--interval", type=int, default=10, help="Update interval")
    argparser.add_argument("--influx_host", type=str, default="localhost", help="InfluxDB host")
    argparser.add_argument("--influx_port", type=int, default=8086, help="InfluxDB port")
    argparser.add_argument("--influx_db", type=str, default="solaredge", help="InfluxDB database")
    argparser.add_argument("--influx_user", type=str, help="InfluxDB username")
    argparser.add_argument("--influx_pass", type=str, help="InfluxDB password")
    argparser.add_argument("--influx_token", type=str, help="InfluxDB auth token")
    argparser.add_argument("--mqtt_host", type=str, help="MQTT hostname [without port]")
    argparser.add_argument("--mqtt_user", type=str, help="MQTT username")
    argparser.add_argument("--mqtt_pass", type=str, help="MQTT password")
    args = argparser.parse_args()

    try:
        if args.influx_user and args.influx:
            client = InfluxDBClient(
                url= f"http://{host}:{port}",
                #host=args.influx_host,
                #port=args.influx_port,
                org="surrealnet",
                token=args.influx_token,
                #username=args.influx_user,
                #password=args.influx_pass
            )
        else:
            #client = InfluxDBClient(host=args.influx_host, port=args.influx_port)
            client = InfluxDBClient(
                url= f"http://{args.influx_host}:{args.influx_port}",
                org="surrealnet",
                token=args.influx_token,
            )

        #client.switch_database(args.influx_db)
        influx_write_api = client.write_api(write_options=SYNCHRONOUS)
    except (ConnectionRefusedError, requests.exceptions.ConnectionError):
        print(f"database connection failed: {args.influx_host,}:{args.influx_port}/{args.influx_db}")
        sys.exit()

    if(args.mqtt_host and args.mqtt_user and args.mqtt_pass):
        mqttc.username_pw_set(args.mqtt_user, args.mqtt_pass)
        mqttc.connect(args.mqtt_host, 1883, 5)
        mqttc.loop(5)

    unit_list = args.unit.split(",")
    master_unit = unit_list.pop(0)
    master_inverter = solaredge_modbus.Inverter(
        host=args.host,
        port=args.port,
        timeout=args.timeout,
        unit=int(master_unit)
    )
    inverters.append(master_inverter)
    for unitnum in unit_list:
        secondary_inverter = solaredge_modbus.Inverter(parent=master_inverter, unit=int(unitnum))
        inverters.append(secondary_inverter)

    while True:
        startTime = time.time()
        json_body = []
        for inverter in inverters:
            fetchData(inverter)
        #client.write_points(json_body)
        influx_write_api.write(bucket=args.influx_db, record=json_body)
        sleep_interval = args.interval - (time.time() - startTime)
        if(sleep_interval <= 0):
            # skip the next run, but process MQTT
            sleep_interval += args.interval
        if(args.mqtt_host and args.mqtt_user and args.mqtt_pass):
            if(sleep_interval > 0):
                #print(f"running MQTT loop for {sleep_interval}")
                mqttc.loop(timeout=sleep_interval)
                # recalculate sleep_interval after MQTT is done
                sleep_interval = args.interval - (time.time() - start_time)
        if(sleep_interval > 0):
            time.sleep(sleep_interval)

