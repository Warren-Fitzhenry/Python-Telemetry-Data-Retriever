# coding: utf-8

'''
===========
DESCRIPTION
===========

This program downloads raw energy data from the BBoxx API.

An INPUT.xlsx file is required specifying the serial numbers
and dates of the required data files to be downloaded.

The data is retrieved in JSON format, but the data is cleaned,
resampled and saved as a CSV file in the specified folder.

Note that POWER and ENERGY metrics are not native but are calculated.

'''


import os
import pytz
import pandas as pd
import datetime as dt
import data_retriever as dr

# where to save raw data files
directory_to = 'C:\Users\warren'

all_units = pd.read_excel('INPUT.xlsx', sheetname=0)

imei_list = all_units['IMEI'].tolist()
imei_list = [x for x in imei_list if str(x) != 'nan']
imei_list = [int(i) for i in imei_list]
imei_list = [str(i) for i in imei_list]

DATES = all_units['DATE'].tolist()
DATES = [str(i) for i in DATES]
DATES = [i[0:10] for i in DATES]
DATES = [x for x in DATES if str(x) != 'nan']

IMEI_LIST = imei_list

# DATA DATE RANGE  (Must be in the format DD-MM-YYYY)
# Enter Same Date for a 24 hour range

# SPECIFY WHAT SHOULD BE CREATED

SAVE_CSV   = True
HOURLY_DATA = False


# Files will be saved at:
#   <ROOT>/outputs/<SERIAL_NUMBER>
# where <ROOT> is the location dataplotter.py and accompanying files are stored

# RESET ENERGY COUNTER at 00:00 EACH DAY
#  True or False

INCLUDE_ENERGY = True
RESET_ENERGY = False

# Include a 5th plot showing temperature
INCLUDE_TEMP = True

###############################################################################
def get_telemetry(units):
    # Connect to influx database to retrieve telemtery data concerning all
    # BBOXX products of interest
    
    telemetry = {}
    for unit in units:
        # 1 BB17SMART_v1
        # 2 BB17SMART_v2
        # 3 BBOXX Home
        # 4 Enterprise_v1
        # 5 Hub
        prod_type_id = unit["product_type_id"]

        if prod_type_id == 3  or [prod_type_id == 6] :
            metrics = ['current', 'current_in', 'current_out', 'temperature', 'voltage']

        elif (prod_type_id == 1) or (prod_type_id == 2):
            metrics = ['current', 'temperature', 'voltage']

        else:
            prod_type = dr.api_get(
                endpoint='product_types',
                filters=[dict(name='product_type_id', op='eq', val=prod_type_id)],
                )
            print "Unknown metrics for product type!: {}".format(prod_type)
            continue

        # Gather the telemetry data for unit
        imei = unit['product_imei']
        telemetry[imei] = dr.get_telemetry2(
            product=unit,
            metrics=metrics,
            start=DLS_DATE,
            end=DLE_DATE,
            localize_index=False,
            )

    return telemetry

###############################################################################
if __name__ == '__main__':
    
    # Download all unit status information from the "dashboard" endpoint
    
    telemetry_list = []
    for d in DATES:
        print 'Connecting to API to get data for {}'.format(d)
        
        ENTITY_ID = 59
        ENTITY = "Sustainability Institute"
        TZ_LOCAL = pytz.timezone('Africa/Johannesburg')

        # SPECIFY MODEL YOU WANT TO LOOK AT HERE
        START_DT   = TZ_LOCAL.localize(dt.datetime.strptime(d, "%d-%m-%Y"))
        START_DATE = START_DT.strftime('%Y-%m-%d %H:%M:%S')
        DLS_DATE   = START_DT.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')

        END_DT   = TZ_LOCAL.localize(dt.datetime.strptime(d, "%d-%m-%Y") + dt.timedelta(days=1))
        END_DATE = END_DT.strftime('%Y-%m-%d %H:%M:%S')
        DLE_DATE = END_DT.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')

        TIMEZONE       = START_DT.strftime("%Z")
        OFFSET_SIGN    = int("{}1".format(START_DT.strftime("%z")[0]))
        OFFSET_HOURS   = int(START_DT.strftime("%z")[1:3])
        OFFSET_MINUTES = int(START_DT.strftime("%z")[3:5])

        counter = 0
        for imei in IMEI_LIST:
            counter += 1
            filters = [{"name": "product_imei", "op": "eq", "val": imei}]
            unit = dr.api_get(endpoint='products', filters=filters, as_df=False)
            serial_number = unit[0]["serial_number"]
            version = unit[0]["product_type_id"]
            telemetry = get_telemetry(unit)

            if version == 1 or version == 2:
                dataset = {
                    # Battery
                    "v_bat": {"title": "Battery Voltage",     "unit": "V",    "integrate": None   , "data": []},
                    "t_bat": {"title": "Battery Temperature", "unit": "degC", "integrate": None   , "data": []},
                    "i_bat": {"title": "Battery Current",     "unit": "A",    "integrate": None   , "data": []},
                    "p_bat": {"title": "Battery Power",       "unit": "W",    "integrate": None   , "data": []},
                    "c_bat": {"title": "Battery Capacity",    "unit": "Ah",   "integrate": "i_bat", "data": []},
                    "e_bat": {"title": "System Energy",       "unit": "Wh",   "integrate": "p_bat", "data": []},
                    }
            elif version == 3 or version == 6:
                dataset = {
                    # Battery
                    "v_bat": {"title": "Battery Voltage",     "unit": "V",    "integrate": None   , "data": []},
                    "t_bat": {"title": "Battery Temperature", "unit": "degC", "integrate": None   , "data": []},
                    "i_bat": {"title": "Battery Current",     "unit": "A",    "integrate": None   , "data": []},
                    "p_bat": {"title": "Battery Power",       "unit": "W",    "integrate": None   , "data": []},
                    #"c_bat": {"title": "Battery Capacity",    "unit": "Ah",   "integrate": "i_bat", "data": []},
                    "e_bat": {"title": "System Energy",       "unit": "Wh",   "integrate": "p_bat", "data": []},
                    # Charger Output
                    "i_chg": {"title": "Charge Current",      "unit": "A",    "integrate": None   , "data": []},
                    "p_chg": {"title": "Charge Power",        "unit": "W",    "integrate": None   , "data": []},
                    #"c_chg": {"title": "Charge Capacity",     "unit": "Ah",   "integrate": "i_chg", "data": []},
                    "e_chg": {"title": "Charge Energy",       "unit": "Wh",   "integrate": "p_chg", "data": []},
                    # Load
                    "i_out": {"title": "Output Current",      "unit": "A",    "integrate": None   , "data": []},
                    "p_out": {"title": "Output Power",        "unit": "W",    "integrate": None   , "data": []},
                    #"c_out": {"title": "Output Capacity",     "unit": "Ah",   "integrate": "i_out", "data": []},
                    "e_out": {"title": "Output Energy",       "unit": "Wh",   "integrate": "p_out", "data": []},
                    }

            # Charger Ouput and Load
            i_plot = []  # Current
            c_plot = []  # Capacity
            p_plot = []  # Power
            e_plot = []  # Energy
            all_data = []

            # Set how the data should be resampled
            num_of_days = (END_DT - START_DT).total_seconds()/(3600*24)
            if num_of_days <= 1:
                sample_time = 60  # seconds
            else:
                sample_time = 60  # seconds

            for unit in telemetry:
                if telemetry[unit] is not None:
                    # Battery voltage and temperature
                    tel_v_bat = telemetry[str(unit)].voltage
                    tel_t_bat = telemetry[str(unit)].temperature

                    if version == 1 or version == 2:
                        tel_i_bat = telemetry[str(unit)].current * -1
                        tel_p_bat = tel_v_bat * tel_i_bat

                        teldata = {
                            "v_bat": tel_v_bat.dropna(),
                            "t_bat": tel_t_bat.dropna(),
                            "i_bat": tel_i_bat.dropna(),
                            "p_bat": tel_p_bat.dropna(),
                            }

                    elif version == 3 or version ==6:
                        # Charger Output
                        tel_i_chg = telemetry[str(unit)].current_in
                        tel_p_chg = tel_v_bat * tel_i_chg
                        # Load
                        tel_i_out = telemetry[str(unit)].current_out
                        tel_p_out = tel_v_bat * tel_i_out
                        # Battery
                        tel_i_bat = tel_i_chg - tel_i_out
                        tel_p_bat = tel_v_bat * tel_i_bat

                        teldata = {
                            # Battery
                            "v_bat": tel_v_bat.dropna(),
                            "t_bat": tel_t_bat.dropna(),
                            "i_bat": tel_i_bat.dropna(),
                            "p_bat": tel_p_bat.dropna(),
                            # Charger Output
                            "i_chg": tel_i_chg.dropna(),
                            "p_chg": tel_p_chg.dropna(),
                            # Load
                            "i_out": tel_i_out.dropna(),
                            "p_out": tel_p_out.dropna(),
                            }

                    # Apply a timezone offset to the timestamps
                    
                    for key in teldata:
                        data = teldata[key]
                        new_index = []
                        for i, ts in enumerate(data.index):
                            new_index.append(
                                ts + dt.timedelta(
                                    hours=OFFSET_SIGN*OFFSET_HOURS,
                                    minutes=OFFSET_SIGN*OFFSET_MINUTES
                                    )
                                )
                        teldata[key].index = new_index

                    # Resample the telemetry data
                    
                    for key, values in dataset.items():
                        integrate = values["integrate"]
                        try:
                            data = teldata[key]
                        except KeyError:
                            # KeyError: Not all keys from dataset are in teldata
                            pass

                        if data.empty:
                            null_index = [
                                (START_DT - dt.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S'),
                                (END_DT + dt.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
                                ]
                            null_data = [0.0001, 0.0001]
                            null_output = pd.Series(null_data, null_index)

                            print "  - There is no data for {}".format(key)
                            null_output.name = "{}, ({})".format(values["title"], values["unit"])
                            dataset[key]["data"].append(null_output)
                            all_data.append(null_output)

                        # Resample the data
                        elif integrate is None:
                            #print "  - Re-sampling {} to {} second(s)".format(key, sample_time)
                            resampled = data.resample("{}S".format(sample_time)).mean()
                            resampled.name = "{}, ({})".format(values["title"], values["unit"])
                            dataset[key]["data"].append(resampled)

                            all_data.append(resampled)

                        # Integrate and resample the specified dataset
                        # In addtion, reset the integrated data to 0.0 at the start of each day.
                        else:
                            tsdata = teldata[integrate]
                            index = tsdata.index
                            new_data = []
                            new_index = []
                            j = 0
                            for i, sample in enumerate(tsdata):
                                if (i == 0) or \
                                   ((RESET_ENERGY is True) and (index[i].day != index[i-1].day)):
                                    new_index.append(dt.datetime.strptime(index[i].strftime('%Y-%m-%d'), '%Y-%m-%d'))
                                    new_data.append(0.0)
                                    j += 1

                                delta_t = index[i] - new_index[j-1]
                                # Simple average the data
                                result = (tsdata[i] + tsdata[i-1]) / 2.0
                                # Multiply by hours
                                result = result * (delta_t.total_seconds() / 3600.0)
                                # Cumulate the result
                                result += new_data[j-1]

                                new_index.append(index[i])
                                new_data.append(result)
                                j += 1

                            output = pd.Series(new_data, new_index)

                            #print "  - Re-sampling {} to {} second(s)".format(key, sample_time)
                            resampled = output.resample("{}S".format(sample_time)).mean()
                            resampled.name = "{}, ({})".format(values["title"], values["unit"])
                            dataset[key]["data"].append(resampled)

                            all_data.append(resampled)

                    if version == 1 or version == 2:
                        i_plot.append(dataset["i_bat"]["data"][0])
                        p_plot.append(dataset["p_bat"]["data"][0])
                    elif version == 3 or version == 6 :
                        # Current plot grouping
                        i_plot.append(dataset["i_chg"]["data"][0])
                        i_plot.append(dataset["i_out"]["data"][0])
                        # Power plot grouping
                        p_plot.append(dataset["p_chg"]["data"][0])
                        p_plot.append(dataset["p_out"]["data"][0])

            for metric in dataset:
                dataset[metric]["data"] = pd.DataFrame(dataset[metric]["data"]).T

            i_plot = pd.DataFrame(i_plot).T
            c_plot = pd.DataFrame(c_plot).T
            p_plot = pd.DataFrame(p_plot).T
            e_plot = pd.DataFrame(e_plot).T

            path_d = "C:\Users\warren.si\Desktop\LonglandsRawData\{}\{}\Raw_data".format(START_DATE[:7],START_DATE[:10])
            path_d_hourly_data = "C:\Users\warren.si\Desktop\LonglandsRawData\{}/{}/HourlyUsage/".format(START_DATE[:7],START_DATE[:10])
            filename = "{}".format(serial_number)
            filename_hourly = "{}_Hourly_Data".format(serial_number)
            

            ##############################################
            ##          GENERATE RESULTS                ##
            ##############################################
            if SAVE_CSV is True:
                all_data = pd.DataFrame(all_data).T.ffill().bfill()#.dropna()
                # WF: reindex data from 00:00 to 23:59, and fill in all gaps with forward fill or backward fill
                all_data = all_data.reindex(pd.date_range(START_DATE, END_DATE, freq='T', closed='left')).ffill().bfill()
                file_csv = "{}.csv".format(filename)
                    
                try:
                    all_data.to_csv(os.path.join(path_d, file_csv))
                   
                except IOError:
                    os.makedirs(path_d)
                    all_data.to_csv(os.path.join(path_d, file_csv))
                else:
                    print str(counter) + "/" + str(len(IMEI_LIST)) + "'{}'' file saved".format(filename)

            # hourly data saved on separate csv file. adds extra hour of day column
                 
            if HOURLY_DATA is True:
                file_csv = "{}.csv".format(filename_hourly)
                all_data.index = pd.to_datetime(all_data.index)
                all_data['Hour'] = all_data.index.hour + 1 #WF adds an hour column
                hourly_data = all_data.iloc[59::60,[11,7, 10]]
                hourly_data = pd.DataFrame(hourly_data)
                hourly_data['Output Energy, (Wh/h)'] = hourly_data['Output Energy, (Wh)'].diff().fillna(0)
                hourly_data['Charge Energy, (Wh/h'] = hourly_data['Charge Energy, (Wh)'].diff().fillna(0)
                  
                
                try:
                    hourly_data.to_csv(os.path.join(path_d_hourly_data, file_csv))
                except IOError:
                    os.makedirs(path_d_hourly_data)
                    hourly_data.to_csv(os.path.join(path_d_hourly_data, file_csv))
                else:
                    print "'{}'' saved to{}".format(filename_hourly, path_d_hourly_data)

            # Plotting function takes start and end as arguments
            start = dt.datetime.strptime(START_DATE, '%Y-%m-%d %H:%M:%S')
            end   = dt.datetime.strptime(END_DATE  , '%Y-%m-%d %H:%M:%S')


