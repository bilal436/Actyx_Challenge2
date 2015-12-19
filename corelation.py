"""
This file defines the functions that will be used to explore the Actyx API.
This script checks the list of machines and the environmental sensor after every 60 seconds to see the values of current
drawn by each and every machine and the values of pressure, temperature, and humidity of the production environment to
derive the person correlation between them
"""

import urllib2
import json
import time
from collections import defaultdict
from scipy.stats.stats import pearsonr

"""
This function defines the function that takes as an argument the url to the API and returns a list of all the
machines with their ID's

input

machines_url : A simple url to the API machines

output

machines_list: A list of all the machines with their ID's
"""


def get_machine_list(machines_url):

    # getting the JSON response from the API. each entry of this response contains a url to the individual machines

    json_obj = urllib2.urlopen(machines_url)
    data = json.load(json_obj)

    # initializing the empty machines_list

    machines_list = []

    # splitting the url on '/' and getting the machine ID at the third index of the split url
    for entry in data:
        machines_list.append(str(entry).split('/')[2])

    return machines_list

"""
This function takes the url for an individual machine and returns its sensor data as JSON response

input

machine_url_list: The list of urls to all machines

output

machine_data_list: The JSON response of every machine's sensor data in a list
"""


def get_machine_data(machine_url_list):

    # initializing the empty machine_data_list
    machine_data_list = []

    # Getting the JSON response from the API for the machine. each entry of this response contains a sensor value

    # Repeating this process for all the machines
    for machine in machine_url_list:

        # Constructing the url
        machine_url = "http://machinepark.actyx.io/api/v1/machine/" + str(machine)

        # Retrieving the JSON response of the sensor data and appending it to the list

        json_obj = urllib2.urlopen(machine_url)
        data = json.load(json_obj)
        machine_data_list.append(data)

    return machine_data_list

"""
This function takes the url for the API environment sensor and returns its sensor data as JSON response

output

env_data_list: The JSON response for the environment sensor
"""


def get_env_data():

    env_sensor_url = "http://machinepark.actyx.io/api/v1/env-sensor"  # Constructing the url

    #  Retrieving the JSON response of the sensor data and appending it to the list
    json_obj = urllib2.urlopen(env_sensor_url)
    env_data_list = json.load(json_obj)

    return env_data_list


"""
This function checks the API after every 60 seconds and for the environmental and power data for each machine and the
environment under they are running. It keeps collecting the data and populates a hash table with

key = variable name
val = variable values

Following are the variables (sets) in our data

1- current drawn by each machine
2- time at which the API was called i.e. time at which the observations were recorded
3- pressure
4- temperature
5- humidity

out put

data:

This contains all the data we collected as a hash table as explained above

pressure_correlation:

The pearson correlation between the temperature and current drawn by every machine. Also a hash table where the key is
the machine name and the value is the person correlation between pressure and the current drawn by the machine

temperature_correlation:

Just as above only replacing pressure with temperature

humidity_correlation:

Just as the above only replacing pressure or temperature with humidity
"""


def get_data_and_correlation():

    # Defining the data collection interval as 60 minutes. Please change this to your own need
    # i.e. for how long you want to collect the data
    data_collection_time_in_minutes = 60*5
    timeout = time.time() + 60*data_collection_time_in_minutes   # 300 minutes/5 hours from now

    # Defining the has map for current data of previous 5 minutes for each machine

    current_env_data_hash_map = defaultdict(list)

    # Getting the list of all the machines

    machine_list = get_machine_list("http://machinepark.actyx.io/api/v1/machines")

    print "Collecting data. This would take " + str(data_collection_time_in_minutes) + " minutes ..."

    while time.time() < timeout:

        # Getting the machine and env_data
        machine_data = get_machine_data(machine_list)
        env_data = get_env_data()

        # Populating the hash table with current values for each machine

        for machine in machine_data:

            current_env_data_hash_map[str(machine['name'])].append(float(machine['current']))

        # Populating the hash table with environment variables

        current_env_data_hash_map['pressure'].append(float(env_data['pressure'][1]))
        current_env_data_hash_map['temperature'].append(float(env_data['temperature'][1]))
        current_env_data_hash_map['humidity'].append(float(env_data['humidity'][1]))

        # Populating the hash table with time

        current_env_data_hash_map['time'].append(str(env_data['pressure'][0]))

        #  call the API for data after every 60 seconds

        time.sleep(60)

    print "Data collection completed."

    # Now calculate te correlation hash maps for each of the three environment variables

    # collecting data for the three environment variables

    pressure = current_env_data_hash_map['pressure']
    temperature = current_env_data_hash_map['temperature']
    humidity = current_env_data_hash_map['humidity']

    # setting up hash maps for the three environment variables

    pressure_correlation_hash_map = defaultdict(list)
    temperature_correlation_hash_map = defaultdict(list)
    humidity_correlation_hash_map = defaultdict(list)

    print ('calculating the correlation now ...')
    print help(pearsonr)

    # Calculating the correlations and populating the hash_maps

    for machine in machine_data:

        machine_current_data = current_env_data_hash_map[str(machine['name'])]

        pressure_correlation = pearsonr(pressure, machine_current_data)
        pressure_correlation_hash_map[str(machine['name'])].append(pressure_correlation)

        temperature_correlation = pearsonr(temperature, machine_current_data)
        temperature_correlation_hash_map[str(machine['name'])].append(temperature_correlation)

        humidity_correlation = pearsonr(humidity, machine_current_data)
        humidity_correlation_hash_map[str(machine['name'])].append(humidity_correlation)

    return current_env_data_hash_map, pressure_correlation_hash_map, temperature_correlation_hash_map, humidity_correlation_hash_map


if __name__ == '__main__':

    data, pressure_correlation, temperature_correlation, humidity_correlation = get_data_and_correlation()
