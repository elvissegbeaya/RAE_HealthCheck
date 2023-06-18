#!/usr/bin/python3
#
# Copyright (c) 2020 NOV Inc. All Rights Reserved
#
# Revision History
#
# 2021-10-31 RRM v1.0 - Original version
#
# 2022-03-07 RRM v1.1 - Added support for Contractor filter
#
# When installing in a new system:
# - apt-get/yum install python36
# - pip3 install --user python-dateutil
# - pip3 install --user requests
# - pip3 install --user python-dateutil
# - pip3 install --user numpy
# - pip3 install --user pyopenssl ndg-httpsclient pyasn1
#
import configparser
import locale
import logging
import os.path
import pprint
import sys
from os import path
from dateutil.parser import parse as dtparse


VERSION = 1.0


def str2dt (stringDT) :

    return dtparse(stringDT)

    #from datetime import datetime
    #for date_format in [ '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', ]:
    #    try:
    #        datetime_object = datetime.strptime(stringDT, date_format)
    #        break
    #    except ValueError:
    #        pass
    #return datetime_object


def readConfig(configFilename, configDef):
    config = configparser.ConfigParser()
    config.read(configFilename)

    sectionName = configDef['SectionName']

    # Check that section exists

    if not sectionName in config.sections():
        logging.error("Section {} does not exist in config file {}".format(sectionName, configFilename))
        print("Exiting.")
        sys.exit()

    cfg = {}

    # gets list of names within the config file
    for parameter in configDef['Parameters']:
        name = list(parameter.keys())[0]
        tmpValue = config.get(sectionName, name)
        value = None

        try:
            if parameter[name]['type'] == 'int':
                value = int(config.get(sectionName, name))
            elif parameter[name]['type'] == 'float':
                value = float(config.get(sectionName, name))
            elif parameter[name]['type'] == 'bool':
                value = bool(config.get(sectionName, name))
            elif parameter[name]['type'] == 'string':
                value = config.get(sectionName, name)
            elif parameter[name]['type'] == 'list':
                s = config.get(sectionName, name)
                value = list(line for line in s.splitlines() if line != '')
        except Exception as e:
            logging.error("Parameter: {}".format(name))
            logging.error(e)
            value = parameter[name]['default']

        cfg[name] = value

    # returns the new config file with all the major sections of the config file
    return cfg


def SetupLogging(logging, Level = logging.INFO):
    format = "[%(asctime)s:%(levelname)10s:%(filename)30s:%(lineno)5s - %(funcName)20s() ] %(message)s"
    logging.basicConfig(format=format, level=Level,
                        datefmt="%H:%M:%S")

def SetupLocale():
    # This is for number printing, i.e.: '{:n}'.format(x)
    locale.setlocale(locale.LC_ALL, '')  # Use '' for auto, or force e.g. to 'en_US.UTF-8'

def VersionCheck():
    # Are we running a Python version that supports unicode?
    if sys.version_info.major < 3:
        logging.error('Pythong 3.x or later required')
        os._exit(os.EX_OK)


def SetConfigFile(defaultConfig):
    configFilename = ""
    if len(sys.argv) > 1:
        configFilename = sys.argv[1]
    if configFilename == "":
        configFilename = 'welldata.cfg'
    if (not path.exists(configFilename)):
        logging.error(
            "Config file " + configFilename + " does not exist. Creating a sample configuration file. Please edit it and try again")
        createSampleConfig(configFilename, defaultConfig)
        logging.error("Exiting.")
        os._exit()

    return configFilename


def createSampleConfig(outputFilename, configDef):
    config = configparser.RawConfigParser(allow_no_value=True)

    sectionName = configDef['SectionName']
    config.add_section(sectionName)

    pp = pprint.PrettyPrinter(indent=12, width=80)
    pp.pprint(configDef)

    for parameter in configDef['Parameters']:
        name = list(parameter.keys())[0]
        config.set(sectionName, "#", None)
        config.set(sectionName, "#", None)
        config.set(sectionName, "# {}".format(name), None)
        config.set(sectionName, "#             Type: {}".format(parameter[name]['type'] + ''), None)
        config.set(sectionName, "#      Description: {}".format(parameter[name]['description'] + ''), None)
        config.set(sectionName, "#    Example Value: {}".format(str(parameter[name]['default']) + ''), None)
        config.set(sectionName, "#          Default: {}".format(str(parameter[name]['default']) + ''), None)
        config.set(sectionName, name, str(parameter[name]['default']))

    # Writing our configuration file to 'example.cfg'
    with open(outputFilename, 'w') as configfile:
        config.write(configfile)






