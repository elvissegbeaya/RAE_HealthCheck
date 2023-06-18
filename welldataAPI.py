#!/usr/bin/python3
#
# welldataAPI.python
#
# Library of WellData Data API functions
from __future__ import annotations

# Copyright (c) 2020 National Oilwell Varco
# All rights reserved
#
# When installing in a new system:
# - apt-get/yum install python36
# - pip3 install --user python-dateutil
# - pip3 install --user requests
# - pip3 install --user python-dateutil
# - pip3 install --user numpy
# - pip3 install --user pyopenssl ndg-httpsclient pyasn1
#

import json
import logging
import os
import os.path
import time
from datetime import datetime
import requests
import sseclient
from pydantic import BaseModel
from pydantic.dataclasses import dataclass
from requests.auth import HTTPBasicAuth
from tenacity import retry, stop_after_attempt, wait_fixed
from typing import List, Optional

def storageConfig():
    return {
        'SectionName': 'storage',
        'Parameters': [
            {'type': {'value': '', 'type': 'string', 'default': 'postgresql', 'description': 'Storage DB engine'}},
            {'server': {'value': '', 'type': 'string', 'default': '', 'description': 'DB Host'}},
            {'port': {'value': '', 'type': 'string', 'default': '', 'description': 'DB TCP Port'}},
            {'username': {'value': '', 'type': 'string', 'default': '', 'description': 'DB Username'}},
            {'password': {'value': '', 'type': 'string', 'default': '', 'description': 'DB Password'}},
            {'runMode': {'value': '', 'type': 'string', 'default': '', 'description': 'Debug Flag'}},
        ]
    }

def serverConfig(ServerName='welldata net'):
    return {
        'SectionName': ServerName,
        'Parameters': [
            {'APIUrl': {'value': '', 'type': 'string', 'default': 'https://data.welldata.net/api/v1',
                        'description': 'https://data.welldata.net/api/v1'}},
            {'appID': {'value': '', 'type': 'string', 'default': '','description': 'App ID provided by WellData Engineering: i.e.: 17147920-2DFB-4E95-B3AB-67ED69D1E02D'}},
            {'appID_rae': {'value': '', 'type': 'string', 'default': '', 'description': 'App ID provided by WellData Engineering: i.e.: 17147920-2DFB-4E95-B3AB-67ED69D1E02D'}},
            {'appID_rae_ca': {'value': '', 'type': 'string', 'default': '', 'description': 'App ID provided by WellData Engineering: i.e.: 17147920-2DFB-4E95-B3AB-67ED69D1E02D'}},
            {'username': {'value': '', 'type': 'string', 'default': '', 'description': 'WellData Username'}},
            {'password': {'value': '', 'type': 'string', 'default': '', 'description': 'WellData Password'}},
            {'rae_user': {'value': '', 'type': 'string', 'default': '', 'description': 'WellData Username'}},
            {'rae_password': {'value': '', 'type': 'string', 'default': '', 'description': 'WellData Password'}},
            {'rae_ca_user': {'value': '', 'type': 'string', 'default': '', 'description': 'WellData Username'}},
            {'rae_ca_password': {'value': '', 'type': 'string', 'default': '', 'description': 'WellData Password'}}

        ]
    }

def defaultConfig():
    # 20220307 v1.2 RRM Added default for Contractor name
    return  {
        'SectionName': 'WellDataDownload',
        'Parameters' : [
            { 'ContractorName':    { 'value': '', 'type': 'list', 'default': '',                         'description': 'If the Contractor string is empty, all wells are retrieved'                                                                 } },
            { 'OperatorName':     { 'value': '', 'type': 'list', 'default': '',                         'description': 'If the Operator string is empty, all wells are retrieved'                                                                 } },
            {'RAE_Rigs': {'value': '', 'type': 'list', 'default': '',                              'description': 'If the Operator string is empty, all wells are retrieved'}},
            {'RAE_OperatorName': {'value': '', 'type': 'list', 'default': '',                              'description': 'If the Operator string is empty, all wells are retrieved'}},
            {'JobStatus': {'value': '', 'type': 'string', 'default': 'ActiveJobs',                       'description': 'Job Status Filter: AllJobs / ActiveJobs / EndedJobs'                                                                              } },
            {'FromHours': {'value': '', 'type': 'string', 'default': '0',                           'description': 'Time Step in seconds. Set to zero for no time log download'}},
            {'ToHours': {'value': '', 'type': 'string', 'default': '0',                         'description': 'Time Step in seconds. Set to zero for no time log download'}},
            {'CurrentFrequency': {'value': '', 'type': 'int', 'default': '0',                                  'description': 'Time Step in seconds. Set to zero for no time log download'}},
            {'HistoricInterval': {'value': '', 'type': 'int', 'default': '0',                                  'description': 'Time Step in seconds. Set to zero for no time log download'}},
            {'CurrentInterval': {'value': '', 'type': 'int', 'default': '0',                                 'description': 'Time Step in seconds. Set to zero for no time log download'}},
            {'FilterList': {'value': '', 'type': 'list', 'default': '',                            'description': "List of attributes to filter by.\n\t Leave empty for no filter"}},
            {'AllActiveJobsOnly': {'value': '', 'type': 'bool', 'default': 'False',                       'description': 'Job Status Filter: True / False'                                                                              } },
            {'ActiveRAEJobsOnly': {'value': '', 'type': 'bool', 'default': 'False',                       'description': 'Job Status Filter: True / False'                                                                              } },
            {'RAEJobsOnly': {'value': '', 'type': 'bool', 'default': 'False',                              'description': 'Job Status Filter: True / False'                                                                              } },
            { 'EnableRealtime':   { 'value': '', 'type': 'bool',   'default': 'False',                    'description': "True or False: defaults to False. Download real-time channels IF WellStatus is set to WellStatus is set to ActiveOnly"       } },
            { 'TimeStep':         { 'value': '', 'type': 'int',    'default': '0',                        'description': 'Time Step in seconds. Set to zero for no time log download'                                                                  } },
            { 'DepthStep':        { 'value': '', 'type': 'int',    'default': '0',                        'description': 'Depth Step in the measured UOM. Set to zero for no depth log download'                                                       } },
            { 'HookLoadbool_min':    { 'value': '', 'type': 'int',    'default': '',                         'description': "Spud Year start. Leave empty for all years"                                                                                  } },
            { 'HookLoadbool_max':      { 'value': '', 'type': 'int', 'default': '',                         'description': "Spud Year end. Leave empty for all years"                                                                                    } },
            { 'PumpPressurebool_min':    { 'value': '', 'type': 'int',    'default': '',                         'description': "Spud Year start. Leave empty for all years"                                                                                  } },
            { 'PumpPressurebool_max':      { 'value': '', 'type': 'int', 'default': '',                         'description': "Spud Year end. Leave empty for all years"                                                                                    } },
            { 'BlockHeightbool_min':    { 'value': '', 'type': 'int',    'default': '',                         'description': "Spud Year start. Leave empty for all years"                                                                                  } },
            { 'BlockHeightbool_max':      { 'value': '', 'type': 'int', 'default': '',                         'description': "Spud Year end. Leave empty for all years"                                                                                    } },
            { 'PumpSpmbool_min':    { 'value': '', 'type': 'int',    'default': '',                         'description': "Spud Year start. Leave empty for all years"                                                                                  } },
            { 'PumpSpmbool_max':      { 'value': '', 'type': 'int', 'default': '',                         'description': "Spud Year end. Leave empty for all years"                                                                                    } },
            { 'PumpSpm2bool_min':    { 'value': '', 'type': 'int',    'default': '',                         'description': "Spud Year start. Leave empty for all years"                                                                                  } },
            { 'PumpSpm2bool_max':      { 'value': '', 'type': 'int', 'default': '',                         'description': "Spud Year end. Leave empty for all years"                                                                                    } },
            {'PumpSpm3bool_min': {'value': '', 'type': 'int', 'default': '', 'description': "Spud Year start. Leave empty for all years"}},
            {'PumpSpm3bool_max': {'value': '', 'type': 'int', 'default': '', 'description': "Spud Year end. Leave empty for all years"}},
            { 'RotaryTorquebool_min':    { 'value': '', 'type': 'int',    'default': '',                         'description': "Spud Year start. Leave empty for all years"                                                                                  } },
            { 'RotaryTorquebool_max':      { 'value': '', 'type': 'int', 'default': '',                         'description': "Spud Year end. Leave empty for all years"                                                                                    } },
            { 'RPM_min':    { 'value': '', 'type': 'int',    'default': '',                         'description': "Spud Year start. Leave empty for all years"                                                                                  } },
            { 'RPM_max':      { 'value': '', 'type': 'int', 'default': '',                         'description': "Spud Year end. Leave empty for all years"                                                                                    } },
            {'tpDriveRPM_min': {'value': '', 'type': 'int', 'default': '', 'description': "Spud Year start. Leave empty for all years"}},
            {'tpDriveRPM_max': {'value': '', 'type': 'int', 'default': '', 'description': "Spud Year end. Leave empty for all years"}},
            {'tpDriveTorq_min': {'value': '', 'type': 'int', 'default': '', 'description': "Spud Year start. Leave empty for all years"}},
            {'tpDriveTorq_max': {'value': '', 'type': 'int', 'default': '', 'description': "Spud Year end. Leave empty for all years"}},
            {'RP_Fast_min': {'value': '', 'type': 'int', 'default': '', 'description': "Spud Year start. Leave empty for all years"}},
            {'RP_Fast_max': {'value': '', 'type': 'int', 'default': '', 'description': "Spud Year end. Leave empty for all years"}},
            { 'BitPositionbool_min':    { 'value': '', 'type': 'int',    'default': '',                         'description': "Spud Year start. Leave empty for all years"                                                                                  } },
            { 'BitPositionbool_max':      { 'value': '', 'type': 'int', 'default': '',                         'description': "Spud Year end. Leave empty for all years"                                                                                    } },
            {'BitStatusbool_min': {'value': '', 'type': 'int', 'default': '', 'description': "Spud Year start. Leave empty for all years"}},
            {'BitStatusbool_max': {'value': '', 'type': 'int', 'default': '', 'description': "Spud Year end. Leave empty for all years"}},
            { 'SlipStatusbool_min':    { 'value': '', 'type': 'int',    'default': '',                         'description': "Spud Year start. Leave empty for all years"                                                                                  } },
            { 'SlipStatusbool_max':      { 'value': '', 'type': 'int', 'default': '',                         'description': "Spud Year end. Leave empty for all years"                                                                                    } },
            {'WOB_min': {'value': '', 'type': 'int', 'default': '', 'description': "Spud Year start. Leave empty for all years"}},
            {'WOB_max': {'value': '', 'type': 'int', 'default': '', 'description': "Spud Year end. Leave empty for all years"}},
            { 'startDate':    { 'value': '', 'type': 'string',    'default': '',                         'description': "Spud Year start. Leave empty for all years"                                                                                  } },
            { 'firstDataDate':      { 'value': '', 'type': 'string', 'default': '',                         'description': "Spud Year end. Leave empty for all years"                                                                                    } },
            {'lastDataDate': {'value': '', 'type': 'string', 'default': '',                               'description': "Spud Year start. Leave empty for all years"}},
            {'endDate': {'value': '', 'type': 'string', 'default': '',                             'description': "Spud Year end. Leave empty for all years"}},
            {'SpudYearStart': {'value': '', 'type': 'string', 'default': '',                               'description': "Spud Year start. Leave empty for all years"}},
            {'SpudYearEnd': {'value': '', 'type': 'string', 'default': '',                             'description': "Spud Year end. Leave empty for all years"}},
            { 'emailRecipients':        { 'value': '', 'type': 'list',   'default': '',                         'description': "List of wells to download.\n\tLeave empty for all wells"                                                                     } },
            { 'WellNames':        { 'value': '', 'type': 'list',   'default': '',                         'description': "List of wells to download.\n\tLeave empty for all wells"                                                                     } },
            { 'ChannelsToOutput': { 'value': '', 'type': 'list',   'default': '',                         'description': "List of channels to output,\n\tone\n\tper\n\tline.\n\tLeave empty for all channels"                                          } },
        ]
    }

    #######################################################################
    #######################################################################
    # Connecting the API Modules to retrieve data from WellData
    #######################################################################
    #######################################################################

class FilterValue(BaseModel):
    value: int


class FilterRange(BaseModel):
    from_: str
    to: str


class FilterIn(BaseModel):
    values: List[int]


class FilterBetween(BaseModel):
    range: FilterRange


class Filter(BaseModel):
    attributeId: str
    isIn: Optional[FilterIn]
    equals: Optional[FilterValue]
    greaterThan: Optional[FilterValue]
    greaterThanEqual: Optional[FilterValue]
    lessThan: Optional[FilterValue]
    lessThanEqual: Optional[FilterValue]
    hasData: Optional[dict]
    between: Optional[FilterBetween]
    isNull: Optional[dict]


class TimeRange(BaseModel):
    from_: datetime
    to: datetime


@retry(stop=stop_after_attempt(4), wait=wait_fixed(2), retry_error_callback=lambda _: print("Retrying..."))
class HistoricalTimeRequest(BaseModel):
    attributes: list
    fromTime: str
    toTime: str
    interval: float
    isDifferential: bool = False


class CurrentTimeRequest(BaseModel):
    attributes: list
    frequency: float
    interval: float
    isDifferential: bool = False


class EventTimeRequest(BaseModel):
    outputAttributes: list
    timeRange: TimeRange
    filter: Filter


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry_error_callback=lambda _: print("Retrying..."))
def historical_data_time(job_id: str, payload: HistoricalTimeRequest, token: any):
    """
    args
        job
        payload
    """
    uri = f'https://data.welldata.net/api/v1/jobs/{job_id}/data/time'
    header = {'token': token}
    r = requests.post(uri, data=payload, headers=header)
    print(r.status_code)
    return r.json()


def current_data_time(job_id: str, payload: CurrentTimeRequest, token: any):
    """
    args
        job
        payload
    """
    uri = f'https://data.welldata.net/api/v1/jobs/{job_id}/data/time/current'
    header = {'token': token}
    r = requests.post(uri, data=payload, headers=header)
    print(r.status_code)
    values = r.json()
    return values




def event_data_time(job_id: str, payload: EventTimeRequest, token: any):
    """
    args
        job
        payload
    """
    uri = f'https://data.welldata.net/api/v1/jobs/{job_id}/data/time/events'
    header = {'token': token}
    r = requests.post(uri, data=payload, headers=header)
    print(r.status_code)
    return r.json()


#getwells original code
#need to fix looping portion
# def getWells1 ( URL, token, CFG, batchSize=100 ) :
#     wells = []
#
#     skip = 0
#     getAdditionalRecords = 1
#
#     while getAdditionalRecords < batchSize :
#
#         parsedPath = URL.replace('<take>', str(batchSize))
#         parsedPath = parsedPath.replace('<skip>', str(skip))
#         print ("Parse Path", parsedPath)
#         logging.error("parsedPath: {}".format(parsedPath))
#
#         headers = { 'Token': token, 'accept': 'application/json' }
#         params = {}
#
#
#         maxRetries = 10
#         retries = 0
#         successfulRequest = False
#
#         while not successfulRequest and retries < maxRetries:
#             r = None
#             try:
#                 r = requests.get ( parsedPath, params=params, headers=headers )
#                 print (r)
#                 successfulRequest = True
#             except Exception as ex:
#                 logging.error("Error sending request to server")
#                 logging.error("Query {}".format(parsedPath))
#                 logging.error("Parameters {}".format(params))
#                 logging.error("Headers {}".format(headers))
#                 logging.error("Response {}".format(r))
#                 retries = retries + 1
#                 logging.error("Sleeping for {} seconds".format(retries))
#                 time.sleep(retries)
#
#         if retries == maxRetries:
#             return wells
#
#         if r.status_code != 200:
#             logging.error ("Error retrieving wells")
#             logging.error ("Request: " + parsedPath)
#             logging.error ("Error code " + str(r.status_code))
#             logging.error ("Error code " + str(r.reason))
#             os._exit(1);
#
#         values = r.json()
#
#         w = values['jobs']
#
#
#         for nw in w:
#            wellName = nw.get('name',None)
#            if wellName == None:
#                logging.info ( "Skipping well with no name", extra = nw )
#                continue
#            # If we have a list of wells, check the well name against the list
#            elif len(CFG['WellNames']) > 0 and wellName not in CFG['WellNames'] :
#                logging.info ( "Skipping well " + wellName + " because WellNames variable was set, and this well was not found in that list" )
#                continue
#
#             #E- ONLY get Patterson Wells
#            if nw['assetInfoList'][0]['owner'] == 'Patterson':
#                wells.append(nw)
#                continue
#
#
#
#
#
#
#         #???  don't think this is in 4.0 REST API
#         # returnedValues = values['ReturnedCount']
#         totalCount     = values['total']
#
#         # logging.info ("Total Wells: {}, Skip: {}, Returned: {}".format( totalCount, skip, returnedValues))
#         #
#         # if totalCount > ( returnedValues + skip ) :
#         #     skip += returnedValues
#         # else :
#         #     getAdditionalRecords = 0
#
#     # 4.1 return a sorted list of wells by name
#     return sorted(wells, key = lambda i: i['name'],reverse=False)
@retry(stop=stop_after_attempt(4), wait=wait_fixed(2), retry_error_callback=lambda _: print("Retrying..."))

def getToken(URL, appID, username, password, processNumber=""):
    headers = {'ApplicationID': appID, 'accept': 'application/json'}
    params = {}

    # print (headers)
    logging.debug("{} Getting Auth Token from {}".format(processNumber, URL))
    r = requests.get(URL + '/tokens/token?', params=params, headers=headers, auth=HTTPBasicAuth(username, password))
    # TODO: Refactor and use the right URLs when you get a chance -> we want to be able to access URLs['getToken]
    # r = requests.get(URLs['getToken'], params=params, headers=headers, auth=HTTPBasicAuth(username, password))
    if r.status_code != 200:
        logging.error("Error code " + str(r.status_code))
        logging.error("Error code " + str(r.reason))

        # FXL (20220718) - Connection for logging to admin db
        # connAdmin = mxAPI.Connection('storage administration')
        # mxAPI.ErrorLog(connAdmin,'Getting Auth Token',"WelldataAPI.py","getToken","Error code: {} - Error reason: {}".format(r.status_code, r.reason))

        os._exit(1)

    # print (r.text)
    # print (r.status_code)
    values = r.json()

    # E- fixed, need to return token lowercase per swagger
    return values['token']

@retry(stop=stop_after_attempt(4), wait=wait_fixed(2), retry_error_callback=lambda _: print("Retrying..."))

def getApiCall(URL, token, CFG, jobId=""):
    # Variables:
    wells = []  # will return number of wells
    r = None
    params = {}
    parsedPath = URL
    retries = 0
    headers = {'Token': token, 'accept': 'application/json'}

    # parsing path
    parsedPath = URL.replace('<jobId>', str(jobId))

    # Checking updated URL
    print(parsedPath)

    # trying to make a connection
    try:
        r = requests.get(parsedPath, params=params, headers=headers)
        print(r)
        if r.status_code == 200:
            successfulRequest = True  # this means we got the data
            values = r.json()
            wells.append(values)

        elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
            # server error, wait and try again
            # take a break for 4 second
            time.sleep(20)
            try:
                r = requests.get(parsedPath, params=params, headers=headers)
                print(r)
                if r.status_code == 200:
                    successfulRequest = True  # this means we got the data
                    values = r.json()
                    wells.append(values)

            except Exception as ex:
                logging.error("Error sending request to server")
                logging.error("Query {}".format(parsedPath))
                logging.error("Parameters {}".format(params))
                logging.error("Headers {}".format(headers))
                logging.error("Response {}".format(r))
                retries = retries + 1
                logging.error("Sleeping for {} seconds".format(retries))

    except Exception as ex:
        logging.error("Error sending request to server")
        logging.error("Query {}".format(parsedPath))
        logging.error("Parameters {}".format(params))
        logging.error("Headers {}".format(headers))
        logging.error("Response {}".format(r))
        retries = retries + 1
        logging.error("Sleeping for {} seconds".format(retries))
        time.sleep(retries)
    return wells

    # Done: only thing missing is AttributeUnits field from swagger

@retry(stop=stop_after_attempt(4), wait=wait_fixed(2), retry_error_callback=lambda _: print("Retrying..."))

def postApiCall(URL, token, CFG, jobId="", data =""):
    # Variables:
    wells = []  # will return number of wells
    params = {}
    r = None
    retries = 0

    # updating the url path
    parsedPath = URL.replace('<jobId>', jobId)
    headers = {'Token': token, 'accept': 'application/json'}

    print(f'This is the parse path: {parsedPath}')
    # trying to make a connection
    try:
        r = requests.post(parsedPath, data=data, headers=headers)
        print(r)
        if r.status_code == 200:

            values = r.json()
            wells.append(values)


        # implement the take and skip functional
        elif r.status_code != 200 and (r.status_code == range(500, 599, 1) or range(400, 410, 1)):  # bad request:
            # server error, wait and try again
            # take a break for 4 second
            time.sleep(20)
            try:
                r = requests.post(parsedPath, data=data, headers=headers)
                print(r)
                if r.status_code == 200:
                    successfulRequest = True  # this means we got the data
                    values = r.json()
                    wells.append(values)

            except Exception as ex:
                logging.error("Error sending request to server")
                logging.error("Query {}".format(parsedPath))
                logging.error("Parameters {}".format(params))
                logging.error("Headers {}".format(headers))
                logging.error("Response {}".format(r))
                retries = retries + 1
                logging.error("Sleeping for {} seconds".format(retries))

    except Exception as ex:
        logging.error("Error sending request to server")
        logging.error("Query {}".format(parsedPath))
        logging.error("Parameters {}".format(params))
        logging.error("Headers {}".format(headers))
        logging.error("Response {}".format(r))
        retries = retries + 1
        logging.error("Sleeping for {} seconds".format(retries))
        time.sleep(retries)

    return wells

    # Done: only thing missing is AttributeUnits field from swagger

@retry(stop=stop_after_attempt(4), wait=wait_fixed(2), retry_error_callback=lambda _: print("Retrying..."))

def getJobs ( URL, token, CFG, **kwargs) : #take = 1, skip =0, totalCount = True ,

    #Variables:

    broadcastTimeTo = ""
    broadcastTimeFrom = ""

    Total = True
    totalCheck = False
    wells = [] #will return number of wells
    attrBool = False    #checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    retries = 0
    successfulRequest = False
    #jobId = "",jobStatus = "ActiveJobs", startDateMin = "", startDateMax = "", endDateMin = "", endDateMax = "", capabilities = False, attributeUnits = "", rigNumber = "", contractor = ""
    jobId = kwargs.get('jobId')
    jobStatus ='ActiveJobs'
    startDateMin = kwargs.get('startDateMin') #2021-07-06 5:13:48 PM
    startDateMax = kwargs.get('startDateMax') #startDateMin=2021-07-06%205%3A13%3A48%20PM   -> URL Format
    endDateMin = kwargs.get('endDateMin')
    endDateMax = kwargs.get('endDateMax')
    Capabilities = False
    rigNumber = kwargs.get('rigNumber')
    contractor = kwargs.get('contractor')
    operator = kwargs.get('operator')
    take = 1
    skip = 0
    sort ='id'
    sortOrder = 'asc'
    totalbool = False
    parsedPath = URL
    if kwargs.get('total') is not None:
        totalbool= kwargs.get('total')
    if kwargs.get('skip') is not None:
        skip = kwargs.get('skip')
    if kwargs.get('take') is not None:
        take = kwargs.get('take')
    if kwargs.get('sort') is not None:
        sort = kwargs.get('sort')
    if kwargs.get('sortOrder') is not None:
        sortOrder = kwargs.get('sortOrder')
    if kwargs.get('Capabilities') is not None:
        Capabilities = kwargs.get('Capabilities')
    if kwargs.get('jobStatus') is not None:
        jobStatus = kwargs.get('jobStatus')


    ############################################
    # handling additional constructor arguments
    ############################################

    headers = {'Token': token, 'accept': 'application/json'}


    if jobId is not None:
        parsedPath = URL.replace('jobId', str(jobId))
        parsedPath = parsedPath.replace('(\'','')
        parsedPath = parsedPath.replace('\',)', '')
    if Capabilities is not None and jobId is None:
        parsedPath = parsedPath.replace('capabilities', f'capabilities={Capabilities}')
        parsedPath = parsedPath.replace('(\'','')
        parsedPath = parsedPath.replace('\',)', '')

    # Checking updated URL
    print(parsedPath)

    #all but GetJob
    if 'includeCapabilities' not in parsedPath or (jobId is not None and 'includeCapabilities' not in parsedPath):
        #trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True    #this means we got the data
                values = r.json()
                wells.append(values)
                #jobid,well name,  contractor, rignumber,startDate, endDate, firstDataDate, lastDataDate
                # wells.append(values['id'] )
                # wells.append(values['name'])
                # wells.append(values['assetInfoList'][0]['owner'])
                # wells.append(values['assetInfoList'][0]['name'])
                # wells.append(values['startDate'])
                # wells.append(values['endDate'])
                # wells.append(values['firstDataDate'])
                # wells.append(values['lastDataDate'])
                # wells.append(values['jobNumber'])


            #implement the take and skip functional
            elif r.status_code != 200 and (r.status_code == range(500,599,1) or range(400,410,1)) : #bad request:
                #server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        successfulRequest = True  # this means we got the data
                        values = r.json()
                        wells.append(values)
                        # jobid,well name,  contractor, rignumber,
                        # wells.append(values['id'])
                        # wells.append(values['name'])
                        # wells.append(values['assetInfoList'][0]['owner'])
                        # wells.append(values['assetInfoList'][0]['name'])
                        # wells.append(values['startDate'])
                        # wells.append(values['endDate'])
                        # wells.append(values['firstDataDate'])
                        # wells.append(values['lastDataDate'])
                        # wells.append(values['jobNumber'])

                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

        return wells
    #for Get Jobs including take and skips
    else:
        parsedPath = URL.replace('<jobStatus>', jobStatus)
        parsedPath = parsedPath.replace('<take>', str(take))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('<sort>', str(sort))
        parsedPath = parsedPath.replace('<sortOrder>', str(sortOrder))
        parsedPath = parsedPath.replace('<includeCapabilities>', str(Capabilities))
        parsedPath = parsedPath.replace('<total>', str(totalbool))

        if startDateMin is not None:
            # converting DateString into parsepath version:   2021-07-06%205%3A13%3A48%20PM  2021-07-06 5:13:48 PM
            dateString = f'{startDateMin[0:10]}%20{startDateMin[11:13]}%3A{startDateMin[14:16]}%3A{startDateMin[17:19]}%20{startDateMin[20:22]}'
            parsedPath = parsedPath.replace('<startDateMin>', str(dateString))
        else:
            parsedPath = parsedPath.replace('&startDateMin=<startDateMin>', '')
        if startDateMax is not None:
            dateString = f'{startDateMax[0:10]}%20{startDateMax[11:13]}%3A{startDateMax[14:16]}%3A{startDateMax[17:19]}%20{startDateMax[20:22]}'
            parsedPath = parsedPath.replace('<startDateMax>', str(dateString))
        else:
            parsedPath = parsedPath.replace('&startDateMax=<startDateMax>', '')
        if endDateMin is not None:
            dateString = f'{endDateMin[0:10]}%20{endDateMin[11:13]}%3A{endDateMin[14:16]}%3A{endDateMin[17:19]}%20{endDateMin[20:22]}'
            parsedPath = parsedPath.replace('<endDateMin>', str(dateString))
        else:
            parsedPath = parsedPath.replace('&endDateMin=<endDateMin>', '')
        if endDateMax is not None:
            dateString = f'{endDateMax[0:10]}%20{endDateMax[11:13]}%3A{endDateMax[14:16]}%3A{endDateMax[17:19]}%20{endDateMax[20:22]}'
            parsedPath = parsedPath.replace('<endDateMax>', str(dateString))
        else:
            parsedPath = parsedPath.replace('&endDateMax=<endDateMax>', '')


        # Checking updated URL
        print(parsedPath)

        while currTake <=  take:
            # trying to make a connection
            try:
                r = requests.get(parsedPath, params=params, headers=headers)
                print(r)
                if r.status_code == 200:
                    successfulRequest = True  # this means we got the data
                    values = r.json()
                    if totalbool is True and totalCheck is False:
                        wells.append(values['total'])
                        totalCheck = True
                    for w in values['jobs']:
                        if contractor is not None or operator is not None:
                            # only append if it meets contractor
                            if contractor is not None and operator is not None:
                                wells.append(w)
                            elif contractor is not None and operator is None:
                                if w['assetInfoList'][0]['owner'] == contractor:
                                    wells.append(w)
                            else:  # contractor is  None and operator is not None:
                                if w['siteInfoList'][0]['owner'] == operator:
                                    wells.append(w)
                        elif rigNumber is not None:
                            if w['assetInfoList'][0]['name'] == rigNumber:
                                wells.append(w)
                        else:
                            wells.append(w)


                # implement the take and skip functional
                elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                    # server error, wait and try again
                    # take a break for 4 second
                    time.sleep(20)
                    try:
                        r = requests.get(parsedPath, params=params, headers=headers)
                        print(r)
                        if r.status_code == 200:
                            values = r.json()
                            if totalbool == True:
                                wells.append(values['total'])
                            for w in values['jobs']:
                                if contractor is not None or operator is not None:
                                    # only append if it meets contractor
                                    if contractor is not None and operator is not None:
                                        wells.append(w)
                                    elif contractor is not None and operator is None:
                                        if w['assetInfoList'][0]['owner'] == contractor:
                                            wells.append(w)
                                    else: #contractor is  None and operator is not None:
                                        if w['siteInfoList'][0]['owner'] == operator:
                                            wells.append(w)
                                elif rigNumber is not None:
                                    if w['assetInfoList'][0]['name'] == rigNumber:
                                        wells.append(w)
                                else:
                                    wells.append(w)

                    except Exception as ex:
                        logging.error("Error sending request to server")
                        logging.error("Query {}".format(parsedPath))
                        logging.error("Parameters {}".format(params))
                        logging.error("Headers {}".format(headers))
                        logging.error("Response {}".format(r))
                        retries = retries + 1
                        logging.error("Sleeping for {} seconds".format(retries))

            except Exception as ex:
                logging.error("Error sending request to server")
                logging.error("Query {}".format(parsedPath))
                logging.error("Parameters {}".format(params))
                logging.error("Headers {}".format(headers))
                logging.error("Response {}".format(r))
                retries = retries + 1
                logging.error("Sleeping for {} seconds".format(retries))
                time.sleep(retries)

        #         #got this far the connection is made and we have the value in r, get the values of job
            skip = skip + currTake
            currTake = currTake + take
        # #gets additional record beyond take
        #     if take > totalcount:
        #         take = totalcount-1
        #     elif currTake + take >= totalcount:
        #         take = totalcount - take - 1

        return wells


    #######################################################################
    #######################################################################
    # Customisable API Calls below per specific requests
    #######################################################################
    #######################################################################

#call to get the total count and return

#returns the count of all jobs/wells
@retry(stop=stop_after_attempt(4), wait=wait_fixed(2), retry_error_callback=lambda _: print("Retrying..."))

def getJobsTotal(URL, token, CFG , take= 0, skip = 0):
    headers = {'Token': token, 'accept': 'application/json'}
    params = {}
    totalCount = 0
    r = None

    try:
        parsedPath = URL.replace('<take>', str(take))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        r = requests.get(parsedPath, params=params, headers=headers)
        print(r)
        if r.status_code == 200:
            totalCount = r.json()['total']
    except Exception as ex:
        logging.error("Error sending request to server")
        logging.error("Query {}".format(parsedPath))
        logging.error("Parameters {}".format(params))
        logging.error("Headers {}".format(headers))
        logging.error("Response {}".format(r))
        logging.error("Sleeping for {} seconds".format(2))
        time.sleep(2)
    return totalCount

#getWells1 function
# def getWells1 ( URL, token, CFG, take= 0, skip = 0, batchSize=0 ) :
#     #variables
#     wells = []
#     getAdditionalRecords = 1
#     r = None
#     w = None
#     totalCount = getWellCount(URL, token, CFG, 1, 0)
#     totalCount = 300
#     headers = {'Token': token, 'accept': 'application/json'}
#     params = {}
#     maxRetries = 10
#     retries = 0
#     currTake = take
#
#     #out do-while for successful skip/take
#     #api request
#     while skip < totalCount:
#         successfulRequest = False
#         #inner do-while for max try at request/take/skip section
#         while retries <= maxRetries or successfulRequest == False:
#             successfulRequest = False
#             try:
#                 #we have not connected yet
#                 #update the URL with new take/skip
#                 parsedPath = URL.replace('<take>', str(currTake))
#                 parsedPath = parsedPath.replace('<skip>', str(skip))
#                 r = requests.get(parsedPath, params=params, headers=headers)
#                 print(r)
#                 if r.status_code == 200:
#                     #if successful, we want to increase the values of take/skip
#                     successfulRequest = True
#                     values = r.json()
#                     for w in values['jobs']:
#                     #wells.append(values['jobs'])
#                         wells.append(w)
#                     skip = skip+take
#                     currTake = currTake+take
#
#             except Exception as ex:
#                 logging.error("Error sending request to server")
#                 logging.error("Query {}".format(parsedPath))
#                 logging.error("Parameters {}".format(params))
#                 logging.error("Headers {}".format(headers))
#                 logging.error("Response {}".format(r))
#                 retries = retries + 1
#                 logging.error("Sleeping for {} seconds".format(retries))
#                 time.sleep(retries)
#             if retries == maxRetries:
#                 logging.info("Skipping wells {} ".format(skip) + " through {}".format(skip+take) + "because of request timeout")
#
#         return wells
#
#     #loggin for data
#
#
#         # for nw in w:
#         #    wellName = nw.get('name',None)
#         #    if wellName == None:
#         #        logging.info ( "Skipping well with no name", extra = nw )
#         #        continue
#         #    # If we have a list of wells, check the well name against the list
#         #    elif len(CFG['WellNames']) > 0 and wellName not in CFG['WellNames'] :
#         #        logging.info ( "Skipping well " + wellName + " because WellNames variable was set, and this well was not found in that list" )
#         #        continue
#
#
#
#
#         # logging.info ("Total Wells: {}, Skip: {}, Returned: {}".format( totalCount, skip, returnedValues))



#TODO: How many takes do you want to do per request if initial request fails?

# get alarms with take/skip method- Original that works. Do not touch
@retry(stop=stop_after_attempt(4), wait=wait_fixed(2), retry_error_callback=lambda _: print("Retrying..."))

def getAlarmsEvents (URL, token, CFG, **kwargs ) :


    #Variables:

    broadcastTimeTo = ""
    broadcastTimeFrom = ""

    totalCheck = False
    wells = [] #will return number of wells
    attrBool = False    #checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    retries = 0
    jobId = kwargs.get('jobId')
    take = 1
    skip = 0
    sortOrder = 'asc'
    totalbool = False
    parsedPath = URL
    if kwargs.get('getTotal') is not None:
        totalbool= kwargs.get('getTotal')
    if kwargs.get('skip') is not None:
        skip = kwargs.get('skip')
    if kwargs.get('take') is not None:
        take = kwargs.get('take')
    if kwargs.get('broadcastTimeTo') is not None:
        broadcastTimeTo = kwargs.get('broadcastTimeTo')
    if kwargs.get('broadcastTimeFrom') is not None:
        broadcastTimeFrom = kwargs.get('broadcastTimeFrom')


    ############################################
    # handling additional constructor arguments
    ############################################

    headers = {'Token': token, 'accept': 'application/json'}




    while currTake <=  take:
        if jobId is not None:
            parsedPath = URL.replace('<jobId>', str(jobId))
        parsedPath = parsedPath.replace('<take>', str(take))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('<total>', str(totalbool))

        if broadcastTimeTo is not None and broadcastTimeTo != '':
            # converting DateString into parsepath version:   2021-07-06%205%3A13%3A48%20PM  2021-07-06 5:13:48 PM
            dateString = f'{broadcastTimeTo[0:10]}%20{broadcastTimeTo[11:13]}%3A{broadcastTimeTo[14:16]}%3A{broadcastTimeTo[17:19]}%20{broadcastTimeTo[20:22]}'
            parsedPath = parsedPath.replace('<broadcastTimeTo>', str(dateString))
        else:
            parsedPath = parsedPath.replace('&broadcastTimeTo=<broadcastTimeTo>', '')
        if broadcastTimeFrom is not None and broadcastTimeFrom != '':
            dateString = f'{broadcastTimeFrom[0:10]}%20{broadcastTimeFrom[11:13]}%3A{broadcastTimeFrom[14:16]}%3A{broadcastTimeFrom[17:19]}%20{broadcastTimeFrom[20:22]}'
            parsedPath = parsedPath.replace('<broadcastTimeFrom>', str(dateString))
        else:
            parsedPath = parsedPath.replace('broadcastTimeFrom=<broadcastTimeFrom>', '')
        # Checking updated URL
        print(parsedPath)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code ==200:
                successfulRequest = True    #this means we got the data
                values = r.json()
                totalcount = values['total']
                if totalbool == True and totalCheck == False:
                    wells.append(f'{jobId} total:{totalcount}')
                    totalCheck = True
                # if attrBool == False :          #gets the attributes
                #     for w in values['attributes']:
                #         wells.append(w)
                #         attrBool = True
                # for w in values['alarmEvents']:
                #     #wells.append([jobId,w])
                #     wells.append(w)
            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        successfulRequest = True  # this means we got the data
                        values = r.json()
                        totalcount = values['total']
                        if totalbool == True and totalCheck == False:
                            wells.append(f'{jobId} total:{totalcount}')
                            totalCheck = True
                        # if attrBool == False:  # gets the attributes
                        #     for w in values['attributes']:
                        #         wells.append(w)
                        #         attrBool = True
                        # for w in values['alarmEvents']:
                        #     # wells.append([jobId,w])
                        #     wells.append(w)

                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

    #         #got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        currTake = currTake + take
    # #gets additional record beyond take
    #     if take > totalcount:
    #         take = totalcount-1
    #     elif currTake + take >= totalcount:
    #         take = totalcount - take - 1

    return wells

# get alarms configuration with take/skip method
@retry(stop=stop_after_attempt(4), wait=wait_fixed(2), retry_error_callback=lambda _: print("Retrying..."))

def getAlarmsConfigurations ( URL, token, CFG, jobId = "", rig = "", take = 0, skip =0 ) :

    #Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format= ""
    attributeId =""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    #variables
    wells = [] #will return number of wells
    alarms = [] #will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False    #checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount =getJobsTotal(URL, token,CFG,jobId,1,0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
    #updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        #trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code ==200:
                successfulRequest = True    #this means we got the data
                values = r.json()
                if attrBool == False :          #gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            #implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500,599,1) or 400 : #bad request:
                #server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            #got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
    #gets additional record beyond take
        if take > totalcount:
            take = totalcount-1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells

@retry(stop=stop_after_attempt(4), wait=wait_fixed(2), retry_error_callback=lambda _: print("Retrying..."))

def getReports (URL, token, CFG, **kwargs ) :


    #Variables:

    classification = 'daily'
    reportGroupId = 2
    timeRangeTo=''
    timeRangeFrom=''
    wells = [] #will return number of wells
    params = {}
    r = None
    retries = 0
    jobId = kwargs.get('jobId')
    parsedPath = URL
    fileFormatId = None
    fileFormat = None
    reportId = None
    reportMulitpleIds = []
    reportDates = ''
    reportMulitpleDates = []

    if kwargs.get('classification') is not None:
        classification = kwargs.get('classification')
    if kwargs.get('timeRangeTo') is not None:
        timeRangeTo = kwargs.get('timeRangeTo')
    if kwargs.get('timeRangeFrom') is not None:
        timeRangeFrom = kwargs.get('timeRangeFrom')
    if kwargs.get('reportGroupId') is not None:
        reportGroupId = kwargs.get('reportGroupId')
    if kwargs.get('fileFormat') is not None:
        fileFormat = kwargs.get('fileFormat')
    if kwargs.get('reportId') is not None:
        reportId = kwargs.get('reportId')



    ############################################
    # handling additional constructor arguments
    ############################################

    headers = {'Token': token, 'accept': 'application/json'}

    if jobId is not None:
        parsedPath = parsedPath.replace('<jobId>', str(jobId))
    if classification is not None:
        parsedPath = parsedPath.replace('<classification>', classification)
    if reportGroupId is not None:
        parsedPath = parsedPath.replace('<reportGroupId>', str(reportGroupId))
    if fileFormat is not None:
        parsedPath = parsedPath.replace('<fileFormatId>', fileFormat)
    if reportId is not None:
        parsedPath = parsedPath + f'?reportIds.ids={reportId}'
        print(parsedPath)


    if timeRangeTo is not None and timeRangeTo != '':
        # converting DateString into parsepath version:   2021-07-06%205%3A13%3A48%20PM  2021-07-06 5:13:48 PM
        dateString = f'{timeRangeTo[0:10]}%20{timeRangeTo[11:13]}%3A{timeRangeTo[14:16]}%3A{timeRangeTo[17:19]}%20{timeRangeTo[20:22]}'
        parsedPath = parsedPath.replace('<timeRangeTo>', str(dateString))
    else:
        parsedPath = parsedPath.replace('&timeRangeTo=<timeRangeTo>', '')
    if timeRangeFrom is not None and timeRangeFrom != '':
        dateString = f'{timeRangeFrom[0:10]}%20{timeRangeFrom[11:13]}%3A{timeRangeFrom[14:16]}%3A{timeRangeFrom[17:19]}%20{timeRangeFrom[20:22]}'
        parsedPath = parsedPath.replace('<timeRangeFrom>', str(dateString))
    else:
        parsedPath = parsedPath.replace('timeRangeFrom=<timeRangeFrom>', '')
    # Checking updated URL
    print(parsedPath)

    if reportId is None:
        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code ==200:
                values = r.json()
                reportIds = values['availableReports']
                for w in reportIds:
                    wells.append(w)

            elif r.status_code != 200 and r.status_code == range(400, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(10)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        reportIds = values['availableReports']
                        for w in reportIds:
                            wells.append(w)

                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

        return wells
    # FileFormat is not empty, and attempting to download report.
    else:
        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                values = r.json()
                wells.append(values)
                # # Open a local file to write the PDF content
                # with open("output.pdf", "wb") as pdf_file:
                #     # Write the content of the response to the local file
                #     pdf_file.write(r.content)
                #     wells.append(r.content)
                # print("PDF file downloaded successfully.")

            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        wells.append(values)
                        # # Open a local file to write the PDF content
                        # with open("output.pdf", "wb") as pdf_file:
                        #     # Write the content of the response to the local file
                        #     pdf_file.write(r.content)
                        #     wells.append(r.content)
                        # print("PDF file downloaded successfully.")

                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))
            else:
                print(f"Failed to download PDF file. Status code: {r.status_code}")
        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

        return wells


def putMudPumps (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def deleteMudPumps (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def putRigs (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def deleteRigs (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def postKPIs (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def postDepthBased (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def postDepthBasedExport (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def postImportData (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def putMudCheck (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def deleteMudCheck (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def putNotes (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def deleteNotes (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def postNotes (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def putBhas (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def deleteBhas (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def putCasings (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def deleteCasings (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def putSwabSurge (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def postSwabSurge (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getWellAlarms(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def postTimeBased (URL, token, CFG, data="", jobId=""):

    # Variables:
    wells = []  # will return number of wells
    params = {}
    r = None

    # updating the url path
    parsedPath = URL.replace('<jobId>', jobId)
    headers = {'Token': token, 'accept': 'application/json'}

    print(f'This is the parse path: {parsedPath}')
    # trying to make a connection
    try:
        r = requests.post(parsedPath, data=data, headers=headers)
        print(r)
        if r.status_code == 200:

            values = r.json()
            wells.append(values)


        # implement the take and skip functional
        elif r.status_code != 200 and (r.status_code == range(500, 599, 1) or range(400, 410, 1)):  # bad request:
            # server error, wait and try again
            # take a break for 4 second
            time.sleep(20)
            try:
                r = requests.post(parsedPath, data=data, headers=headers)
                print(r)
                if r.status_code == 200:
                    successfulRequest = True  # this means we got the data
                    values = r.json()
                    wells.append(values)

            except Exception as ex:
                logging.error("Error sending request to server")
                logging.error("Query {}".format(parsedPath))
                logging.error("Parameters {}".format(params))
                logging.error("Headers {}".format(headers))
                logging.error("Response {}".format(r))
                retries = retries + 1
                logging.error("Sleeping for {} seconds".format(retries))

    except Exception as ex:
        logging.error("Error sending request to server")
        logging.error("Query {}".format(parsedPath))
        logging.error("Parameters {}".format(params))
        logging.error("Headers {}".format(headers))
        logging.error("Response {}".format(r))
        retries = retries + 1
        logging.error("Sleeping for {} seconds".format(retries))
        time.sleep(retries)

    return wells


def postEvents (URL, token, CFG, data="", jobId=""):
    # Variables:

    wells = []  # will return number of wells
    params = {}
    r = None
    retries =0
    # updating the url path
    parsedPath = URL.replace('<jobId>', jobId)
    headers = {'Token': token, 'accept': 'application/json'}

    print(f'This is the parse path: {parsedPath}')
    # trying to make a connection
    try:
        r = requests.post(parsedPath, data= data, headers=headers)
        print(r)
        if r.status_code == 200:
            values = r.json()
            wells.append(values)

        # catch timeouts and errors
        elif r.status_code != 200 and (r.status_code == range(500, 599, 1) or range(400, 410, 1)):  # bad request:
            # server error, wait and try again
            # take a break for 4 second
            time.sleep(20)
            try:
                r = requests.post(parsedPath, data=data, headers=headers)
                print(r)
                if r.status_code == 200:
                    values = r.json()
                    wells.append(values)

            except Exception as ex:
                logging.error("Error sending request to server")
                logging.error("Query {}".format(parsedPath))
                logging.error("Parameters {}".format(params))
                logging.error("Headers {}".format(headers))
                logging.error("Response {}".format(r))
                retries = retries + 1
                logging.error("Sleeping for {} seconds".format(retries))

    except Exception as ex:
        logging.error("Error sending request to server")
        logging.error("Query {}".format(parsedPath))
        logging.error("Parameters {}".format(params))
        logging.error("Headers {}".format(headers))
        logging.error("Response {}".format(r))
        retries = retries + 1
        logging.error("Sleeping for {} seconds".format(retries))
        time.sleep(retries)

    return wells


def postTimeBasedExport (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def putTorqueDrag (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def postTorqueDrag (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells

@retry(stop=stop_after_attempt(4), wait=wait_fixed(2), retry_error_callback=lambda _: print("Retrying..."))
def postReports (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def postSummaryReports (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def putMeta (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


def deleteMeta (URL, token, CFG, jobId="", rig="", take=0, skip=0):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    # variables
    wells = []  # will return number of wells
    alarms = []  # will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('jobid', jobId)
    attrBool = False  # checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = getJobsTotal(URL, token, CFG, jobId, 1, 0)
    successfulRequest = False
    retries = 0

    while currTake <= totalcount:
        # updating the url path

        parsedPath = URL.replace('<take>', str(currTake))
        parsedPath = parsedPath.replace('<skip>', str(skip))
        parsedPath = parsedPath.replace('jobid', jobId)

        # trying to make a connection
        try:
            r = requests.get(parsedPath, params=params, headers=headers)
            print(r)
            if r.status_code == 200:
                successfulRequest = True  # this means we got the data
                values = r.json()
                if attrBool == False:  # gets the attributes
                    for w in values['attributes']:
                        wells.append(w)
                        attrBool = True
                for w in values['alarmEvents']:
                    wells.append(w)

            # implement the take and skip functional
            elif r.status_code != 200 and r.status_code == range(500, 599, 1) or 400:  # bad request:
                # server error, wait and try again
                # take a break for 4 second
                time.sleep(20)
                try:
                    r = requests.get(parsedPath, params=params, headers=headers)
                    print(r)
                    if r.status_code == 200:
                        values = r.json()
                        if attrBool == False:
                            for w in values['attributes']:
                                # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                                wells.append(w)
                                attrBool = True
                        for w in values['alarmEvents']:
                            # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                            wells.append(w)



                except Exception as ex:
                    logging.error("Error sending request to server")
                    logging.error("Query {}".format(parsedPath))
                    logging.error("Parameters {}".format(params))
                    logging.error("Headers {}".format(headers))
                    logging.error("Response {}".format(r))
                    retries = retries + 1
                    logging.error("Sleeping for {} seconds".format(retries))

        except Exception as ex:
            logging.error("Error sending request to server")
            logging.error("Query {}".format(parsedPath))
            logging.error("Parameters {}".format(params))
            logging.error("Headers {}".format(headers))
            logging.error("Response {}".format(r))
            retries = retries + 1
            logging.error("Sleeping for {} seconds".format(retries))
            time.sleep(retries)

            # got this far the connection is made and we have the value in r, get the values of job
        skip = skip + currTake
        # gets additional record beyond take
        if take > totalcount:
            take = totalcount - 1
        elif currTake + take >= totalcount:
            take = totalcount - take - 1
        currTake = currTake + take
    return wells


    ################################################################################################################
    # URLS
    ################################################################################################################


    # builds URLs and checks for Well names within Config file
    # 2022-03-07 v1.2 RRM Added support for Contractor filter


######### previous Methods#############

def getWells ( URL, token, CFG, batchSize=100 ) :

    #variables
    wells = [] #will return number of wells
    skip = 0 # how many items you want to skip
    take = 0 #should equal to how many items you want to take at one time
    headers = {'Token': token, 'accept': 'application/json'}
    params = {}
    r = None
    totalcount = 0
    successfulRequest = False
    retries = 0
    batchSize = getJobsTotal(URL, token, CFG, 1, 0)
    #updating the url path
    parsedPath = URL.replace('<take>', str(batchSize))
    parsedPath = parsedPath.replace('<skip>', str(skip))

    #trying to make a connection
    try:
        r = requests.get(parsedPath, params=params, headers=headers)
        print(r)
        if r.status_code ==200:
            successfulRequest = True    #this means we got the data
            totalcount = r.json()['total']
            logging.info("We are doing take {}".format(totalcount))
            values = r.json()
            for w in values['jobs']:
            #wells.append(values['jobs'])
                wells.append(w)


        #implement the take and skip functional
        if r.status_code != 200 and r.status_code == 500: #bad request:
            print("Here")

        #if bad status, return error code
        if r.status_code != 200:
            logging.error("Error retrieving wells")
            logging.error("Request: " + parsedPath)
            logging.error("Error code " + str(r.status_code))
            logging.error("Error code " + str(r.reason))
            os._exit(1);
    except Exception as ex:
        logging.error("Error sending request to server")
        logging.error("Query {}".format(parsedPath))
        logging.error("Parameters {}".format(params))
        logging.error("Headers {}".format(headers))
        logging.error("Response {}".format(r))
        retries = retries + 1
        logging.error("Sleeping for {} seconds".format(retries))
        time.sleep(retries)

    #got this far the connection is made and we have the value in r, get the values of job
    print("This is the number of wells taken", len(wells))
    return wells


def getJobsbyWellName(token, CFG, wellname:str):
    headers = {'Token': token, 'accept': 'application/json'}
    URL = 'https://data.welldata.net/api/v1/jobs?jobStatus=ActiveJobs&includeCapabilities=false&sort=id%20asc&take=50&skip=0&total=false'
    params = ''
    jobid=''


    try:
        r = requests.get(URL, params=params, headers=headers)
        print(r)
        if r.status_code == 200:
            successfulRequest = True # this means we got the data
            values = r.json()
            for w in values['jobs']:
                if wellname == w['name']:
                    jobid= w['id']

    except Exception as ex:
        logging.error("Error sending request to server")
        logging.error("Query {}".format(parsedPath))
        logging.error("Parameters {}".format(params))
        logging.error("Headers {}".format(headers))
        logging.error("Response {}".format(r))
        retries = retries + 1
        logging.error("Sleeping for {} seconds".format(retries))
        time.sleep(retries)

    return jobid


# get alarms with take/skip method
@retry(stop=stop_after_attempt(4), wait=wait_fixed(2), retry_error_callback=lambda _: print("Retrying..."))
def getWellAlarms ( URL, token, CFG, jobId = "", take = 0, skip =0 ) :


    #variables
    wells = [] #will return number of wells
    alarms = [] #will return list of alarms per job id
    headers = {'Token': token, 'accept': 'application/json'}
    parsedPath = URL.replace('<take>', str(take))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('<jobId>', jobId)
    attrBool = False    #checks to see if attributes taken
    params = {}
    r = None
    currTake = 1
    totalcount = 1
    successfulRequest = False
    retries = 0


    #updating the url path

    parsedPath = URL.replace('<take>', str(currTake))
    parsedPath = parsedPath.replace('<skip>', str(skip))
    parsedPath = parsedPath.replace('<jobId>', jobId)

    #trying to make a connection
    try:
        r = requests.get(parsedPath, params=params, headers=headers)
        print(r)
        if r.status_code ==200:
            successfulRequest = True    #this means we got the data
            values = r.json()
            wells.append(values)
            # if attrBool == False :          #gets the attributes
            #     for w in values['attributes']:
            #         wells.append(w)
            #         attrBool = True
            # for w in values['alarmEvents']:
            #     wells.append(w)

        #implement the take and skip functional
        elif r.status_code != 200 and r.status_code == range(500,599,1) or 400 : #bad request:
            #server error, wait and try again
            # take a break for 4 second
            time.sleep(20)
            try:
                r = requests.get(parsedPath, params=params, headers=headers)
                print(r)
                if r.status_code == 200:
                    values = r.json()
                    wells.append(values)
                    # if attrBool == False:
                    #     for w in values['attributes']:
                    #         # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                    #         wells.append(w)
                    #         attrBool = True
                    # for w in values['alarmEvents']:
                    #     # wells.append(rig, jobId, totalcount, w['broadcastTime'], w['alarmState'])
                    #     wells.append(w)



            except Exception as ex:
                logging.error("Error sending request to server")
                logging.error("Query {}".format(parsedPath))
                logging.error("Parameters {}".format(params))
                logging.error("Headers {}".format(headers))
                logging.error("Response {}".format(r))
                retries = retries + 1
                logging.error("Sleeping for {} seconds".format(retries))

    except Exception as ex:
        logging.error("Error sending request to server")
        logging.error("Query {}".format(parsedPath))
        logging.error("Parameters {}".format(params))
        logging.error("Headers {}".format(headers))
        logging.error("Response {}".format(r))
        retries = retries + 1
        logging.error("Sleeping for {} seconds".format(retries))
        time.sleep(retries)



    return wells

def getTimeData(wells, token, URLs, CFG, DataQueue):
    if CFG['TimeStep'] == 0:
        logging.info("Time Step set to Zero. Skipping time log download")
        return

    headers = {'Token': token, 'accept': 'application/json', 'Content-type': 'application/json'}
    params = {}

    datapointsPerRequest = 200000
    maxRetries = 10
    pp = pprint.PrettyPrinter(indent=12, width=80)

    for well in wells:

        # Check that we got a sane well with a name
        wellName = well.get('Name', None)
        if (wellName == None):
            logging.warning("Skipping well with empty name")
            continue

        # Check the zip file doesn't already exist

        zipfilename = zipFileName(well, 'Time', CFG['TimeStep'])

        if path.exists(zipfilename):
            logging.warning("Skipping well " + wellName + " because ZIP file " + zipfilename + " already exists.")
            continue

        # All good, continue processing

        logging.info("Processing well " + wellName + " well ID: " + str(well.get('WellID', None)))

        URL = URLs['getTimeData'].replace('{wellID}', str(well.get('WellID')))

        # Check for a valid start date
        tmpDT = well.get('StartDrDate', None)
        if tmpDT == None:
            tmpDT = well.get('SpudDate', None)

        if tmpDT == None:
            logging.warning("\tNo known start date for this well. Skipping")
            continue

        fromDT = str2dt(tmpDT)

        # Get for a valid end date
        tmpDT = well.get('LastDrDate', None)
        if tmpDT == None:
            tmpDT = well.get('ReleaseDate', None)
        if tmpDT == None:
            logging.warning("\tNo known end date for this well. Skipping")
            continue

        toDT = str2dt(tmpDT)

        # print (fromDT)
        # print (toDT)

        AllChannels = getWellChannels(well, token)
        # Check for returned tags. Well 27357 in .ca is erroing out with status 500
        if (AllChannels == None):
            logging.warning("\tNo tags returned for this well, skipping")
            continue

        Channels = []
        # channelCount = 0
        for channel in AllChannels:
            if (len(CFG['ChannelsToOutput']) > 0):
                if channel.get('Name') in CFG['ChannelsToOutput']:
                    Channels.append(channel)
            elif (channel.get('HasData')):
                Channels.append(channel)
            # channelCount = channelCount + 1
            # if ( channelCount > 2 ):
            #    break

        logging.info("\tTotal Channels {}, Output Channels (with data) {}".format(len(AllChannels), len(Channels)))

        DTinterval = int(datapointsPerRequest / len(Channels))
        logging.info("\tData request interval: {} seconds".format(DTinterval))

        Tags = []
        for tag in Channels:
            Tags.append({
                'Name': tag.get('ID'),
                'Mode': 'Last'
            })

        while (fromDT < toDT):
            nextDT = dtIncrement(fromDT, DTinterval - 1)

            # print("\tRequest interval: {} to {}".format(fromDT, nextDT))

            HistoricalRequest = json.dumps({
                'Tags': Tags,
                'Type': {
                    'From': dt2str(fromDT) + 'Z',
                    'To': dt2str(nextDT) + 'Z',
                    'Interval': CFG['TimeStep'],
                    'IsDifferential': False
                }

            })

            # print (HistoricalRequest)
            # break

            # Post request to get data, with added exception handling
            retries = 0
            successfulRequest = False
            errorMessage = ''
            while (retries < maxRetries and not successfulRequest):
                startRequestTime = time.time()
                try:
                    r = requests.post(URL, data=HistoricalRequest, headers=headers,
                                      auth=HTTPBasicAuth(CFG['username'], CFG['password']))
                except requests.exceptions.RequestException as e:  # This is the correct syntax
                    retries = retries + 1
                    if (retries == 1):
                        logging.error("\tRequest interval: {} to {} error".format(fromDT, nextDT))
                        errorMessage = str(e)
                    logging.info("\t\tPausing for {} sec".format(retries))
                    time.sleep(retries)
                    logging.info("\t\tRetrying # " + str(retries))

                if r.status_code != 200:
                    retries = retries + 1
                    time.sleep(1)
                    if (retries == 1):
                        logging.error("\t\tRequest interval: {} to {} error".format(fromDT, nextDT))
                    logging.error("\t\tError code " + str(r.status_code))
                    logging.error("\t\tError code " + str(r.reason))
                    if (retries < maxRetries):
                        logging.info("\t\tPausing for {} sec".format(retries))
                        time.sleep(retries)
                        logging.info("\t\tRetry {}, max {}".format(retries, maxRetries))
                    else:
                        logging.info("\t\tSkipping to next time range")
                        continue
                else:
                    successfulRequest = True
                    endRequestTime = time.time()
                    requestInterval = endRequestTime - startRequestTime
                    datapointsPerSecond = datapointsPerRequest / requestInterval

            # print (r.json())

            if (successfulRequest):
                if (retries > 0):
                    logging.info(
                        "\t\tRequest interval: {} to {} eventually succeeded after {} retries".format(fromDT, nextDT,
                                                                                                      retries))
                logging.info("\tRequest interval: {} - {}, Elapsed {:.3f}sec, samples/sec {:11n}".format(fromDT, nextDT,
                                                                                                         requestInterval,
                                                                                                         int(datapointsPerSecond)))
                mxAPI.SaveTimeData(well, r.json(), Channels)
                quit()
            else:
                logging.error("\t\tURL: " + URL)
                logging.error("\t\tRequest: " + HistoricalRequest)
                logging.error("\t\tResponse: " + r.text)
                if errorMessage != '':
                    logging.error("\t\t" + errorMessage)

            fromDT = dtIncrement(nextDT, 1)

            time.sleep(0.1)

        zipFile(well, 'Time', CFG['TimeStep'])

        removeFile(well, 'Time', CFG['TimeStep'])

    logging.info("Processing completed.")


def getDepthData(wells, token, URLs, CFG):
    if CFG['DepthStep'] == 0:
        logging.warning("Depth Step set to Zero. Skipping depth log download")
        return

    headers = {'Token': token, 'accept': 'application/json', 'Content-type': 'application/json'}
    params = {}

    for well in wells:

        # Check that we got a sane well with a name
        wellName = well.get('Name', None)
        if (wellName == None):
            logging.warning("Skipping well with empty name")
            continue

        # Check the zip file doesn't already exist

        zipfilename = zipFileName(well, 'Depth', CFG['DepthStep'])

        if path.exists(zipfilename):
            logging.warning("Skipping well " + wellName + " because ZIP file " + zipfilename + " already exists.")
            continue

        # All good, continue processing

        logging.info("Processing well " + wellName + " well ID: " + str(well.get('WellID', None)))

        URL = URLs['getDepthData'].replace('{wellID}', str(well.get('WellID')))

        fromDepth = 0

        # Check for a valid end depth
        tmpDepth = well.get('TotalDepth', None)
        if tmpDepth == None or tmpDepth == 0:
            logging.warning("\tNo known end depth for this well. Skipping")
            continue

        toDepth = float(tmpDepth)

        DepthInterval = 10000 * CFG['DepthStep']

        AllChannels = getWellChannels(well, token)
        # Check for returned tags. Well 27357 in .ca is erroing out with status 500
        if (AllChannels == None):
            logging.warning("\tNo tags returned for this well, skipping")
            continue

        Channels = []
        # channelCount = 0
        for channel in AllChannels:
            if (len(CFG['ChannelsToOutput']) > 0):
                if channel.get('Name') in CFG['ChannelsToOutput']:
                    Channels.append(channel)
            elif (channel.get('HasData')):
                Channels.append(channel)
            # channelCount = channelCount + 1
            # if ( channelCount > 2 ):
            #    break

        logging.info("\tTotal Channels {}, Output Channels (with data) {}".format(len(AllChannels), len(Channels)))

        Tags = []
        for tag in Channels:
            Tags.append({
                'Name': tag.get('ID'),
                'Mode': 'Last'
            })

        while (fromDepth < toDepth):
            nextDepth = fromDepth + DepthInterval - CFG['DepthStep']
            if nextDepth > 100000:
                logging.warning("\tDepth exceeds 100,000")
                return

            logging.info("\tRequest interval: {} - {}".format(fromDepth, nextDepth))

            HistoricalRequest = json.dumps({
                'Tags': Tags,
                'Type': {
                    'FromDepth': fromDepth,
                    'ToDepth': toDepth,
                    'Interval': CFG['DepthStep'],
                    'IsDifferential': False
                }

            })

            # print (HistoricalRequest)
            # break

            # Post request to get data, with added exception handling
            retries = 0
            successfulRequest = False
            while (retries < 3 and not successfulRequest):
                try:
                    r = requests.post(URL, data=HistoricalRequest, headers=headers,
                                      auth=HTTPBasicAuth(CFG['username'], CFG['password']))
                    successfulRequest = True
                except requests.exceptions.RequestException as e:  # This is the correct syntax
                    retries = retries + 1
                    logging.error("\tError when requesting data from " + URL)
                    logging.error("\t" + str(e))
                    logging.info("\tRetrying # " + str(retries))

            if r.status_code != 200:
                logging.error("Error code " + str(r.status_code))
                logging.error("\t\tError code " + str(r.reason))
                logging.error("URL: " + URL)
                pp = pprint.PrettyPrinter(indent=4)
                pp.pprint("Request: " + HistoricalRequest)
                logging.error("Response: " + r.text)

            # print (r.json())

            saveDepthData(well, r.json(), Channels)

            fromDepth = nextDepth + CFG['DepthStep']

        zipFile(well, 'Depth', CFG['DepthStep'])

        removeFile(well, 'Depth', CFG['DepthStep'])

    logging.info("Processing completed.")


def getWellChannels(well, token, URLs):
    wellID = well.get('WellID', None)
    values = None
    if (wellID != None):

        headers = {'Token': token, 'accept': 'application/json'}
        params = {}

        URL = URLs['getTags'].replace('{wellID}', str(wellID))

        r = requests.get(URL, params=params, headers=headers, auth=HTTPBasicAuth(CFG['username'], CFG['password']))
        if r.status_code != 200:
            logging.error("Error code " + str(r.status_code))
            logging.error("Error code " + str(r.reason))
            logging.error(URL)
            return values

        # print (r.text)
        # print (r.status_code)
        values = r.json()

    return values


def getRealtimeData(wells, token, URLs, CFG):
    maxRetries = 10
    pp = pprint.PrettyPrinter(indent=12, width=80)

    for well in wells:

        # Check that we got a sane well with a name
        wellName = well.get('Name', None)
        if (wellName == None):
            logging.warning("Skipping well with empty name")
            continue

        logging.info("Processing well " + wellName + " well ID: " + str(well.get('WellID', None)))

        URL = URLs['getRealtimeData'].replace('{wellID}', str(well.get('WellID')))

        AllChannels = getWellChannels(well, token)
        # Check for returned tags. Well 27357 in .ca is erroing out with status 500
        if (AllChannels == None):
            logging.warning("\tNo tags returned for this well, skipping")
            continue

        Channels = []
        # channelCount = 0
        for channel in AllChannels:
            if (len(CFG['ChannelsToOutput']) > 0):
                if channel.get('Name') in CFG['ChannelsToOutput']:
                    Channels.append(channel)
            elif (channel.get('HasData')):
                Channels.append(channel)

        logging.info("\tTotal Channels {}, Output Channels (with data) {}".format(len(AllChannels), len(Channels)))

        Tags = []
        for tag in Channels:
            Tags.append("\"" + tag.get('ID') + "\"")

        headers = {'Token': token, 'Content-type': 'application/json', 'accept': 'text/event-stream',
                   'taglist': ",".join(Tags)}
        params = {}

        # Get  request to get data, with added exception handling
        retries = 0
        successfulRequest = False
        errorMessage = ''

        while (retries < maxRetries and not successfulRequest):
            startRequestTime = time.time()
            try:
                # Note the stream=True parameters, which will open the websocket in streaming mode
                r = requests.get(URL, params=params, headers=headers, stream=True)
            except requests.exceptions.RequestException as e:  # This is the correct syntax
                retries = retries + 1
                if (retries == 1):
                    logging.error("\tRequest error")
                    logging.error("\t" + e)
                    errorMessage = str(e)
                logging.info("\t\tPausing for {} sec".format(retries))
                time.sleep(retries)
                logging.info("\t\tRetrying # " + str(retries))

            if r.status_code != 200:
                retries = retries + 1
                time.sleep(1)
                if (retries == 1):
                    logging.error("\t\tRequest error")
                    pp.pprint(r.request.headers)
                    pp.pprint(r.request.body)
                logging.error("\t\tError code " + str(r.status_code))
                logging.error("\t\tError code " + str(r.reason))
                if (retries < maxRetries):
                    logging.info("\t\tPausing for {} sec".format(retries))
                    time.sleep(retries)
                    logging.info("\t\tRetry {}, max {}".format(retries, maxRetries))
                else:
                    logging.info("\t\tSkipping")
                    continue
            else:
                successfulRequest = True
                endRequestTime = time.time()
                requestInterval = endRequestTime - startRequestTime

        if (successfulRequest):
            if (retries > 0):
                logging.info("\t\tRequest eventually succeeded after {} retries".format(retries))
            logging.info("\tRequest Elapsed {:.3f}sec".format(requestInterval))

            # Here we are starting a background processing thread for this wellID
            # Allowing us to work with multiple wells in parallel
            # We are using the daemon=True arameter to ensure the thread shuts download
            # when then calling program does.
            #
            t = threading.Thread(target=realtimeParse, args=(r, wellName), daemon=True)
            t.start()
        else:
            logging.info("\t\tURL: " + URL)
            if errorMessage != '':
                logging.error("\t\t" + errorMessage)

    print("Processing completed.")


def realtimeParse(r, name):
    #    events = 0
    #    maxEvents = 20
    previousLine = ""
    Channels = []

    Values = []
    for line in r.iter_lines(decode_unicode=True):
        if line:
            #            events += 1
            #            logging.debug ("{:>32}: {} of {}".format(name, events, maxEvents))
            #            logging.debug ("{:>32} {}".format(name, line))

            if line.startswith("data: "):
                data = json.loads(remove_prefix(line, "data: "))
                # Json 3.9 allows line.removeprefix("...")
                if previousLine == "event: header":
                    for c in data['Tags']:
                        Channels.append(c['Name'])
                        Values = [None] * len(Channels)
                elif previousLine == "event: update":
                    timestamp = data['Timestamp']
                    for v in data['Values']:
                        try:
                            Values[v[0]] = v[1]
                        except:
                            Values[v[0]] = None

                    debugLine = ""
                    for i in range(len(Channels)):
                        debugLine = debugLine + "  {:>12} {:8.1f}".format(Channels[i], Values[i])

                    logging.info("{:>32} {} {}".format(name, timestamp, debugLine))

            previousLine = line

    ################################################################################################################
    # PUT // POST // DELETE  Methods , need 1 of each for each type
    ################################################################################################################

#### OLD URLS ####

def URLs(serverURL, ContractorName='Patterson', OperatorName='', SpudYearStart=0, SpudYearEnd=0, JobStatus='ActiveJobs',
         Since=None):
    URL = {}
    # URL to the API Service to create the authentication token
    URL['getToken'] = serverURL;

    # URL to the API Service to retrieve active well information (Note the filter = ActiveOnly Parameter
    # For all wells, import filter = All
    AdditionalFilter = ''

    if OperatorName != '':
        print(OperatorName)
        quit()
        AdditionalFilter = AdditionalFilter + '&operatorNameFilter=' + OperatorName

    # 2022-03-07 v1.1 RRM Added filter for contractor
    if ContractorName != '':
        AdditionalFilter = AdditionalFilter + '&contractorNameFilter=' + ContractorName

    if Since != None:
        AdditionalFilter = AdditionalFilter + '&spudDateStart=' + Since

    elif SpudYearStart != '' and int(SpudYearStart) != 0:
        AdditionalFilter = AdditionalFilter + '&spudDateStart=' + "{0:0>4}-{1:02}-{2:02}T{3:02}:{4:02}:{5:06.3f}Z".format(
            SpudYearStart, 1, 1, 0, 0, 0.0)

    if SpudYearEnd != '' and int(SpudYearEnd) != 0:
        AdditionalFilter = AdditionalFilter + '&spudDateEnd=' + "{0:0>4}-{1:02}-{2:02}T{3:02}:{4:02}:{5:06.3f}Z".format(
            SpudYearEnd, 12, 31, 23, 59, 59.999)

    # TODO: Update these to the new Swagger 2.0, Add the additional ones such as GetJob, see instructions for the ones you need.

    # URL for getting wells/jobs
    # URL['getWells'] = serverURL + '/api/1.0/wells?filter=' + WellStatus + '&sort=WellID%20ASC&take=<take>&skip=<skip>' + AdditionalFilter
    # TODO: don't hard code Total as true
    # URL['getJobs'] = serverURL + '/jobs?JobStatus=' + JobStatus + '&includeCapabilities=false&sort=id%20ASC&take=50&skip=0&total=true'
    URL[
        'getJobs'] = serverURL + '/jobs?JobStatus=AllJobs&startDateMin=2021-06-01%205%3A13%3A48%20PM&endDateMax=2023-03-01%205%3A13%3A48%20PM&includeCapabilities=false&sort=id%20asc&take=14893&skip=0&total=true'

    # URL for getting Alarms
    # ex: https://data.welldata.net/api/v1/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL[
        'getAlarms'] = serverURL + '/jobs/jobid/alarm-events?broadcastTimeFrom=2022-01-01T00%3A00%3A00.000Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.000Z&take=<take>&skip=<skip>&total=true'

    # URL for getting Tags
    URL['getTags'] = serverURL + '/api/1.0/wells/{wellID}/Data/Drilling/Tags'

    # URL for getting real-time Time data
    URL['getRealtimeData'] = serverURL + '/api/1.0/wells/{wellID}/Data/Drilling/Time/Current?frequency=1&interval=0'

    # URL for getting Time Data
    URL['getTimeData'] = serverURL + '/api/1.0/wells/{wellID}/Data/Drilling/Time'

    # URL for getting DepthData
    URL['getDepthData'] = serverURL + '/api/1.0/wells/{wellID}/Data/Drilling/Depth'

    # URL for getting a well by ID
    URL['getWellByID'] = serverURL + '/api/1.0/wells/{wellID}'

    # 2022-03-07 RRM v.1.2 Added URLs for collecting RMS data
    URL['getAvailableReports'] = serverURL + '/api/1.0/wells/{wellID}/reports/RMS'

    # 2022-03-07 RRM v.1.2 Added URLs for collecting RMS data
    URL['getReportList'] = serverURL + '/api/1.0/wells/{wellID}/reports/RMS/{ReportGroupID}'

    # 2022-03-07 RRM v.1.2 Added URLs for collecting RMS data
    URL['getReport'] = serverURL + '/api/1.0/wells/{wellID}/reports/RMS/{ReportGroupID}/json'

    return URL


# testing  codes sameples
def URLs_v1(serverURL, OperatorName='', JobStatus='ActiveOnly', Since=None):
    # Variables:

    URL = {}
    jobId = ''
    runId = ""
    broadcastTimeTo = ""
    broadcastTimeFrom = ""
    take = 1
    skip = 0
    Total = True
    Format = ""
    attributeId = ""
    summaryReportId = ""
    fileFormatId = ""
    metakey = ""
    classification = ""
    reportGroupId = ""
    swabSurgeType = ""

    ################################################################################################################
    # Various URLs
    ################################################################################################################

    # URL to the API Service to create the authentication token
    URL['getToken'] = serverURL;

    # URL to the API Service to retrieve active well information (Note the filter = ActiveOnly Parameter
    # For all wells, import filter = All

    # TODO: Update these to the new Swagger 2.0, Add the additional ones such as GetJob, see instructions for the ones you need.

    # URL for getting wells/jobs
    # URL['getWells'] = serverURL + '/api/1.0/wells?filter=' + WellStatus + '&sort=WellID%20ASC&take=<take>&skip=<skip>' + AdditionalFilter
    # TODO: don't hard code Total as true
    # URL['getJobs'] = serverURL + '/jobs?JobStatus=' + JobStatus + '&includeCapabilities=false&sort=id%20ASC&take=50&skip=0&total=true'

    # Jobs Header:
    URL['getJobs'] = serverURL + "/jobs?jobStatus=<jobStatus>&startDateMin=<startDateMin>&startDateMax=<startDateMax>&endDateMin=<endDateMin>&endDateMax=<endDateMax>&includeCapabilities=<includeCapabilities>&sort=<sort>%20<sortOrder>&take=<take>&skip=<skip>&total=<total>"  # Fetches all jobs
    URL['getJobsCapabilities'] = serverURL + f'/jobs/capabilities'  # Fetches the capabilities of the jobs endpoint
    URL['getJobsId'] = serverURL + f'/jobs/jobId'  # Fetches a job by its id
    URL['getJobsIdCapabilities'] = serverURL + f'/jobs/<jobId>/capabilities'  # Fetches the capabilities of a job

    # Alarm Configurations
    #URL['getAlarmConfig'] = serverURL + f'/jobs/<jobId>/alarm-configurations?JobStatus=<JobStatus>&take=<take>&skip=<skip>&total=<Total>'  # Fetches alarm change log for a single job
    URL['getAlarmConfig'] = serverURL + f'/jobs/<jobId>/alarm-configurations'  # Fetches alarm change log for a single job
    URL['getAlarmsConfigCapabilities'] = serverURL + f'/jobs/<jobId>/alarm-configurations/capabilities'  # Fetches the capabilities for the alarm configurations specified by the Job ID.

    # Alarm Events
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getAlarms'] = serverURL + f'/jobs/<jobId>/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Z&take=1&skip=0&total=true'
    #URL['getAlarms'] = serverURL + f'/jobs/<jobId>/alarm-events?broadcastTimeFrom=<broadcastTimeFrom>&broadcastTimeTo=<broadcastTimeTo>&take=<take>&skip=<skip>&total=<total>'  # Fetches alarm events for a single job
    URL['getAlarmsCapabilities'] = serverURL + f'/jobs/<jobId>/alarm-events/capabilities?broadcastTimeFrom=<broadcastTimeFrom>&broadcastTimeTo=<broadcastTimeTo>&take=<take>&skip=<skip>&total=<total>'  # Fetches the alarms events capabilities for a single job

    # MudPumps
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getMudPumps'] = serverURL + f'/jobs/<jobId>/assets/mud-pumps'  # Fetches alarm events for a single job                                                                                                      #GetMudPumps gets all the mud pumps and properties for the specified Job ID
    URL['getMudPumpsCapabilities'] = serverURL + f'/jobs/<jobId>/assets/mud-pumps/capabilities'  # Fetches the alarms events capabilities for a single job                                                           #GetMudPumpsCapabilities fetches the capabilities for the mud pumps and properties specified by the Job ID.
    URL['postMudPumps'] = serverURL + f'/jobs/<jobId>/assets/mud-pumps'  # Fetches alarm events for a single job
    URL['deleteMudPumps'] = serverURL + f'/jobs/<jobId>/assets/mud-pumps'  # Fetches alarm events for a single job

    # Rigs
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getRigs'] = serverURL + f'/jobs/<jobId>/assets/rigs'  # Fetches alarm events for a single job                                                                                                                       #GetRigs gets all the Rigs for a specified jobId
    URL['getRigsCapabilities'] = serverURL + f'/jobs/<jobId>/assets/rigs/capabilities'  # Fetches the alarms events capabilities for a single job
    URL['postRigs'] = serverURL + f'/jobs/<jobId>/assets/rigs'  # Fetches alarm events for a single job
    URL['deleteRigs'] = serverURL + f'/jobs/<jobId>/assets/rigs'  # Fetches alarm events for a single job

    # Attributes
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getAttributes'] = serverURL + f'/jobs/<jobId>/attributes'  # Fetches the attributes for a single job
    URL['getAttributesCapabilities'] = serverURL + f'/jobs/<jobId>/attributes/capabilities'  # Fetches the attributes capabilities for a single job

    # KPIs
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['postKpiConnections'] = serverURL + f'/jobs/<jobId>/data/connections'  # Return the List of connections from the specified well
    URL['getKpiConnectionsCapabilities'] = serverURL + f'/jobs/<jobId>/data/connections/capabilities'  # Fetch the capabilities of the connections endpoint

    # DepthBased
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['postDepthBased'] = serverURL + f'/jobs/<jobId>/data/depth'  # Fetches historical depth data with request in BODY
    URL['getDepthBasedCapabilities'] = serverURL + f'/jobs/<jobId>/data/depth/capabilities'  # Fetches the capabilities for the GetDepthData endpoint

    # DepthBased Export
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getDepthBasedExport'] = serverURL + f'/jobs/<jobId>/data/depth/formats'  # Return different format types available for depth export
    URL['getDepthBasedExportCapabilities'] = serverURL + f'/jobs/<jobId>/data/depth/formats/capabilities'  # Return the capabilities of GetDepthExportFormatsCapabilities endpoint
    URL['getDepthBasedExportJobCapabilities'] = serverURL + f'/jobs/<jobId>/data/depth/formats/<Format>/capabilities'  # Return the capabilities of GetDepthExportFormatsCapabilities endpoint
    URL['postDepthBasedExportDelimited'] = serverURL + f'/jobs/<jobId>/data/depth/formats/delimited'  # Return depth data in a delimited file with request in BODY
    URL['postDepthBasedExportFormat'] = serverURL + f'/jobs/<jobId>/data/depth/formats/<Format>'  # Return depth data in a file in the selected format with request in BODY

    # Import Data
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['postImportData'] = serverURL + f'/jobs/<jobId>/data/import/third-party'  # Stores third party context data

    # MudCheck
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getMudCheck'] = serverURL + f'/jobs/<jobId>/data/mud-check'  # GetMudChecks gets the mud check for the specified Job ID
    URL['getMudCheckCapabilities'] = serverURL + f'/jobs/<jobId>/data/mud-check/capabilities'  # GetMudCheckCapabilities fetches the capabilities for the mud check specified by the Job ID.
    URL['putMudCheck'] = serverURL + f'/jobs/<jobId>/data/mud-check'  # Updates the mud check for the specified Job ID
    URL['deleteMudCheck'] = serverURL + f'/jobs/<jobId>/data/mud-check'  # DeleteMudChecks deletes the mud check for the specified Job ID

    # Notes
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getNotes'] = serverURL + f'/jobs/<jobId>/data/notes'  # Gets all the notes for a job
    URL['getNotesCapabilities'] = serverURL + f'/jobs/<jobId>/data/notes/capabilities'  # Fetches the capabilities for the historical notes specified by the Job ID.
    URL['getNotesChanges'] = serverURL + f'/jobs/<jobId>/data/notes/changes'  # gets a stream real-time notes change(s) specified by the Job ID
    URL['getNotesChangesCapabilities'] = serverURL + f'/jobs/<jobId>/data/notes/changes/capabilities'  # Fetches the capabilities for the notes changes specified by the Job ID.
    URL['putNotes'] = serverURL + f'/jobs/<jobId>/data/notes'  # UpdateNote creates or updates a note
    URL['postNotes'] = serverURL + f'/jobs/<jobId>/data/notes'  # CreateNote creates a note
    URL['deleteNotes'] = serverURL + f'/jobs/<jobId>/data/notes'  # DeleteNote deletes the notes for the specified Job ID

    # Runs
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getRuns'] = serverURL + f'/jobs/<jobId>/data/runs'  # Gets all the runs for a job
    URL['getRunsCapabilities'] = serverURL + f'/jobs/<jobId>/data/runs/capabilities'  # Fetches the capabilities for the historical runs specified by the Job ID.

    # Bhas
    URL['getBhas'] = serverURL + f'/jobs/<jobId>/data/runs/bhas'  # GetBhas gets all the Bha assembly for the specified Job ID
    URL['getBhasCapabilities'] = serverURL + f'/jobs/<jobId>/data/runs/bhas/capabilities'  # GetBhasCapabilities fetches the capabilities for the Bha specified by the Job ID.
    URL['putBhas'] = serverURL + f'/jobs/<jobId>/data/runs/bhas'  # UpdateBhas updates the Bha assembly for the specified Job ID
    URL['deleteBhas'] = serverURL + f'/jobs/<jobId>/data/runs/bhas'  # DeleteBhas deletes Bha assembly for the specified Job ID

    # Casings
    URL['getCasings'] = serverURL + f'/jobs/<jobId>/data/runs/casings'  # GetCasings gets all the casing assembly for the specified Job ID
    URL['getCasingsCapabilities'] = serverURL + f'/jobs/<jobId>/data/runs/casings/capabilities'  # GetCasingsCapabilities fetches the capabilities for the casing specified by the Job ID.
    URL['putCasings'] = serverURL + f'/jobs/<jobId>/data/runs/casings'  # UpdateCasings updates the casing assembly for the specified Job ID
    URL['deleteCasings'] = serverURL + f'/jobs/<jobId>/data/runs/casings'  # DeleteCasings deletes casing assembly for the specified Job ID

    # SwabSurge
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getSwagSurge'] = serverURL + f'/jobs/<jobId>/data/runs/<runId>/models/swab-surge/<swabSurgeType>'  # retrieves swab/surge emw points.
    URL['getSwagSurgeCapabilities'] = serverURL + f'/jobs/<jobId>/data/runs/<runId>/models/swab-surge/<swabSurgeType>/capabilities'  # retrieves capabilities for swab/surge emw.
    URL['getSwagSurgeTripSpeedCapabilities'] = serverURL + f'/jobs/<jobId>/data/runs/<runId>/models/swab-surge/<swabSurgeType>/trip-speed/capabilities'  # retrieves capabilities for swab/surge tripping speed model.
    URL['putSwagSurge'] = serverURL + f'/jobs/<jobId>/data/runs/<runId>/models/swab-surge/<swabSurgeType>'  # UpdateNote swab/surge data.
    URL['postSwagSurge'] = serverURL + f'/jobs/<jobId>/data/runs/<runId>/models/swab-surge/<swabSurgeType>/trip-speed'  # generates swab/surge tripping model for equivalent mud weights

    # Sections
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getSections'] = serverURL + f'/jobs/<jobId>/data/sections'  # Return all sections for a given job
    URL['getSectionsCapabilities'] = serverURL + f'/jobs/<jobId>/data/sections/capabilities'  # Fetches the capabilities for the sections by the Job ID.

    # Surveys
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getSurveys'] = serverURL + f'/jobs/<jobId>/data/surveys'  # Rgets all the surveys filtered by the optional parameters approved, unapproved, fromDepth, endDepth
    URL['getSurveysCapabilities'] = serverURL + f'/jobs/<jobId>/data/surveys/capabilities'  # Fetches the capabilities for the historical surveys specified by the Job ID.
    URL['getCurrentSurveysCapabilities'] = serverURL + f'/jobs/<jobId>/data/surveys/current/capabilities'  # Fetches the capabilities for the real-time survey specified by the Job ID.
    URL['postSwagSurge'] = serverURL + f'/jobs/<jobId>/data/runs/<runId>/models/swab-surge/<swabSurgeType>/trip-speed'  # generates swab/surge tripping model for equivalent mud weights

    # Time Based
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getTimeBasedCapabilities'] = serverURL + f'/jobs/<jobId>/data/surveys'  # Rgets all the surveys filtered by the optional parameters approved, unapproved, fromDepth, endDepth
    URL['getCurrentTimeBased'] = serverURL + f'/jobs/<jobId>/data/time/current'  # Fetches the capabilities for the historical surveys specified by the Job ID.
    URL['getCurrentTimeBasedCapabilities'] = serverURL + f'/jobs/<jobId>/data/surveys/current/capabilities'  # Fetches the capabilities for the real-time survey specified by the Job ID.
    URL['postTimeBased'] = serverURL + f'/jobs/<jobId>/data/time'  # generates swab/surge tripping model for equivalent mud weights
    URL['postCurrentTimeBased'] = serverURL + f'/jobs/<jobId>/data/time/current'  # generates swab/surge tripping model for equivalent mud weights

    # TimeOffsets
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getTimeOffsets'] = serverURL + f'/jobs/<jobId>/data/time-offsets'  # Returns a list of DateTimeOffsets for the specified job
    URL['getTimeOffsetsCapabilities'] = serverURL + f'/jobs/<jobId>/data/time-offsets/capabilities'  # Fetches the capabilities for the GetTimeOffsets endpoint

    # Events
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['postEvents'] = serverURL + f'/jobs/<jobId>/data/time/events'  # Fetches events for a single job
    URL['getEventsCapabilities'] = serverURL + f'/jobs/<jobId>/data/time/events/capabilities'  # Fetches the Events capabilities for a single job

    # Time Based Export
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getTimeBasedExport'] = serverURL + f'jobs/<jobId>/data/time/formats'  # Return different format types available for time export
    URL['getTimeBasedExportCapabilities'] = serverURL + f'/jobs/<jobId>/data/time/formats/capabilities'  # Return the capabilities of GetTimeExportFormatsCapabilities endpoint
    URL['getTimeBasedExportFormatCapabilities'] = serverURL + f'/jobs/<jobId>/data/time/formats/<Format>/capabilities'  # Return the capabilities of GetTimeExportFormatCapabilities endpoint
    URL['postTimeBasedExportDelimited'] = serverURL + f'/jobs/<jobId>/data/time/formats/delimited'  # Return time data in a delimited file with request in BODY
    URL['postTimeBasedExportFormat'] = serverURL + f'/jobs/<jobId>/data/time/formats/<Format>'  # Return time data in a file in the selected format with request in BODY

    # TimeSummary
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getTimeSummary'] = serverURL + f'/jobs/<jobId>/data/time/<attributeId>/time-summary'  # gets the time summary for the specified job and attribute IDs.
    URL['getTimeSummaryCapabilities'] = serverURL + f'/jobs/<jobId>/data/time/<attributeId>/time-summary/capabilitiess'  # gets the time summary for the specified job and attribute IDs.

    # Torque Drag
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getTorqueDragModelsCapabilities'] = serverURL + f'/jobs/<jobId>/data/torque-drag/capabilities'  # Fetches the capabilities for the torquedrag run data by the Job ID.
    URL['getTorqueDragDataCapabilities'] = serverURL + f'/jobs/<jobId>/models/torque-drag/capabilities'  # Fetches the capabilities for the torquedrag models by the Job ID.
    URL['postTorqueDragData'] = serverURL + f'jobs/<jobId>/data/torque-drag'  # Return run data with request in BODY
    URL['postTorqueDragModel'] = serverURL + f'/jobs/<jobId>/models/torque-drag'  # Return torque and drag model with request in BODY
    URL['putTorqueDragModel'] = serverURL + f'/jobs/<jobId>/models/torque-drag'  # creates the Torque and Drag model for a specified run

    # Reports
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getReports'] = serverURL + f'/jobs/<jobId>/reports'  # Fetches the list of report classifications with available reports for the given job
    URL['getReportsCapabilities'] = serverURL + f'/jobs/<jobId>/reports/capabilities'  # Fetches the capabilities of reports endpoint
    URL['getReportsSummaryReportCapabilities'] = serverURL + f'/jobs/<jobId>/reports/summary/<summaryReportId>/<fileFormatId>/capabilities'  # Fetches the capabilities of summary reports format endpoint
    URL['getReportsClassification'] = serverURL + f'/jobs/<jobId>/reports/<classification>'  # Fetches the list of report groups with available reports for the given job
    URL['getReportsClassificationCapabilities'] = serverURL + f'/jobs/<jobId>/reports/<classification>/capabilities'  # Fetches the capabilities of reports classification endpoint.
    URL['getReportsClassificationReportGroup'] = serverURL + f'/jobs/<jobId>/reports/<classification>/<reportGroupId>'  # Fetches a list of all the available reports within the given report classification and group for the specified job.
    URL['getReportsClassificationReportGroupCapabilities'] = serverURL + f'/jobs/<jobId>/reports/<classification>/<reportGroupId>/capabilities'  # Fetches the capabilities of reports group endpoint
    URL['getReportsClassificationReportGroupFileFormat'] = serverURL + f'/jobs/<jobId>/reports/<classification>/<reportGroupId>/<fileFormatId>'  # Fetches the specified reports via file download
    URL['getReportsClassificationReportGroupFileFormatCapabilities'] = serverURL + f'/jobs/<jobId>/reports/<classification>/<reportGroupId>/<fileFormatId>/capabilities'  # Fetches the capabilities of reports format endpoint
    URL['postReportsClassificationReportGroupFileFormat'] = serverURL + f'/jobs/<jobId>/reports/<classification>/<reportGroupId>/<fileFormatId>'  # Return the specified reports via file download

    # Summary Reports
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getSummaryReports'] = serverURL + f'/jobs/<jobId>/reports/summary'  # Return the list of available well summary reports for the specified well
    URL['getSummaryReportsCapabilities'] = serverURL + f'/jobs/<jobId>/reports/summary/capabilities'  # Return the capabilities of well summary reports
    URL['getSummaryReportsFileFormat'] = serverURL + f'/jobs/<jobId>/reports/summary/<summaryReportId>/<fileFormatId>'  # Fetches the specified summary report
    URL['postSummaryReportsFileFormat'] = serverURL + f'/jobs/<jobId>/reports/summary/<summaryReportId>/<fileFormatId>'  # Return the specified summary reports via file download

    # Meta
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getMeta'] = serverURL + f'/meta'  # Return a list of key/value pairs from meta data.
    URL['getMetaCapabilities'] = serverURL + f'/meta/capabilities'  # Fetches the capabilities for the Meta endpoint
    URL['getMetaKey'] = serverURL + f'/meta/<metakey>'  # Reads meta data.
    URL['getMetaKeyCapabilities'] = serverURL + f'/meta/<metakey>/capabilities'  # Fetches the capabilities for the Meta endpoint
    URL['putMetaKey'] = serverURL + f'/meta/<metakey>'  # Stores meta data. Meta data is not interpreted and will be returned when calling read with the same key. Overrides any previous data stored by the given key.
    URL['deleteMetaKey'] = serverURL + f'/meta/<metakey>'  # Deletes meta data. Deleting data for a key with no associated data silently does nothing.

    # Tokens
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getTokens'] = serverURL + f'/tokens/token'  # authenticates a user and returns an authentication token.

    # Units
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getUnits'] = serverURL + f'/units/definitions'  # retrieves unit of measure definitions
    URL['getUnitsCapabilities'] = serverURL + f'/units/definitions/capabilities'  # retrieves capabilities for unit definitions

    # Users
    # ex: https://data.welldata.net/jobs/net_176376/alarm-events?broadcastTimeFrom=2022-01-01T00%3A19%3A00.990Z&broadcastTimeTo=2023-01-01T00%3A00%3A00.990Ztake=1&skip=0&total=true
    URL['getUsers'] = serverURL + f'/users/current'  # Fetches the current user
    URL['getUsersCapabilities'] = serverURL + f'/users/current/capabilities'  # Fetches the capabilities of the current user endpoint

    #############################################################################################################################################################################################
    # Old URLS:
    #############################################################################################################################################################################################
    # URL for getting Tags
    URL['getTags'] = serverURL + '/api/1.0/wells/{wellID}/Data/Drilling/Tags'

    # URL for getting real-time Time data
    URL['getRealtimeData'] = serverURL + '/api/1.0/wells/{wellID}/Data/Drilling/Time/Current?frequency=1&interval=0'

    # URL for getting Time Data
    URL['getTimeData'] = serverURL + '/api/1.0/wells/{wellID}/Data/Drilling/Time'

    # URL for getting DepthData
    URL['getDepthData'] = serverURL + '/api/1.0/wells/{wellID}/Data/Drilling/Depth'

    # URL for getting a well by ID
    URL['getWellByID'] = serverURL + '/api/1.0/wells/{wellID}'

    # 2022-03-07 RRM v.1.2 Added URLs for collecting RMS data
    URL['getAvailableReports'] = serverURL + '/api/1.0/wells/{wellID}/reports/RMS'

    # 2022-03-07 RRM v.1.2 Added URLs for collecting RMS data
    URL['getReportList'] = serverURL + '/api/1.0/wells/{wellID}/reports/RMS/{ReportGroupID}'

    # 2022-03-07 RRM v.1.2 Added URLs for collecting RMS data
    URL['getReport'] = serverURL + '/api/1.0/wells/{wellID}/reports/RMS/{ReportGroupID}/json'

    # return the URL based on the call
    return URL
