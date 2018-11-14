#!/usr/bin/env python3.6

"""
   Download System for Google Analytics
"""

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from datetime import datetime
from datetime import timedelta
from datetime import date
import time
import calendar
import httplib2
import csv
import sys
import re
import os
import copy
import argparse
import configparser

LOCAL_DIR   = os.path.dirname(os.path.realpath(__file__))
CONFIG_FILE = "download.cfg"

# Maxium number of dimensions "batches" (can be increased if neeeded)
MAX_DIM_BATCHES = 20

# Operators for search
GA_SEARCH_OPS = [ 'REGEXP', 'BEGINS_WITH', 'ENDS_WITH', 'PARTIAL', 'EXACT' ]

PROG_DESC = """Download results from Google Analytics.  

The date format is YYYY-MM-DD or relative date (e.g. today, yesterday, NdaysAgo where N is 
a positive integer).

Filters are optional and follow the Reporting API v4 format, with multiple filters separated
by AND:
    --filter "ga:browser EXACT Firefox"
    --filter "ga:dimension1 BEGINS_WITH 0123"
    --filter "ga:dimension1 BEGINS_WITH 0123 AND ga:dimension2 EXACT abcdef"

Supported filter operators: %s

""" % (', '.join(GA_SEARCH_OPS))

def usage (sMsg = None):
    """Print the usage information and exit"""
    if sMsg != None:
        printStdError("error: " + sMsg + "\n")
    oParser = getArgParser()
    oParser.print_help()
    sys.exit(-1)

def printStdError (sOutput):
    """Print to standard error"""
    sys.stderr.write(sOutput + "\n")

def errorMsg (sMsg):
    """Print an error message and exit"""
    printStdError("Error: " + sMsg)
    sys.exit(-1)

class Download:
    def main (self):
        """Primary class method"""
        self.getCmdOptions()
        self.getConfig()
        
        # Get the users and the results, depending on the options
        if self.oCmdOptions.bUsers:
            aUsers   = self.processReport(self.USER_DIMENSIONS)
            self.outputRows(aUsers)
        elif self.oCmdOptions.bResults:
            aResults = self.processReport(self.RESULTS_DIMENSIONS)
            self.outputRows(aResults)
        elif self.oCmdOptions.bValidate:
            aUsers   = self.processReport(self.USER_DIMENSIONS)
            aResults = self.processReport(self.RESULTS_DIMENSIONS)
            print("Total number of users: %d" % (len(aUsers) - 1))
            print("Total number of results: %d" % (len(aResults) - 1))
        else:
            self.downloadCombined()
    
    def getConfig (self):
        """Get all configuration elements"""
        self.oConfig = configparser.RawConfigParser()
        self.oConfig.read(LOCAL_DIR + "/" +  CONFIG_FILE)

        self.SCOPES                = self.getConfigValue('common', 'SCOPES')
        self.DISCOVERY_URI         = self.getConfigValue('common', 'DISCOVERY_URI')
        self.KEY_FILE_LOCATION     = self.getConfigValue('common', 'KEY_FILE_LOCATION')
        self.SERVICE_ACCOUNT_EMAIL = self.getConfigValue('common', 'SERVICE_ACCOUNT_EMAIL')
        self.MAX_RESULTS           = self.getConfigValue('common', 'MAX_RESULTS')
        self.INVALID_VALUE         = self.getConfigValue('common', 'INVALID_VALUE')
        self.VIEW_ID               = self.getConfigValue('common', 'VIEW_ID')
        
        self.CUSTOM_DIMENSIONS     = self.getConfigDimensionDict('custom-dimensions')
        self.STITCH_DIMENSIONS     = self.getConfigDimensions('stitch-dimensions')
        self.USER_DIMENSIONS       = self.getConfigDimensions('user-dimensions')
        self.RESULTS_DIMENSIONS    = self.getConfigDimensions('results-dimensions')
        
        # Get each batch of dimensions as a separate array
        self.BATCH_DIMENSIONS = []
        for iSection in range(1, MAX_DIM_BATCHES):
            sSection = 'batch-dimensions-%d' % iSection
            if self.oConfig.has_section(sSection):
                self.BATCH_DIMENSIONS.append(self.getConfigDimensions(sSection))

        # Common batch parameters
        self.BATCH_PARAMS = {
            'reportRequests': [{
                'pageSize': self.MAX_RESULTS,
                'metrics':[{ 'expression': 'ga:users' }],
            }]
        }
                

    def getArgParser (self):
        """Management of the command-line argument parser"""
        oParser = argparse.ArgumentParser(description=PROG_DESC, formatter_class=argparse.RawTextHelpFormatter)
        oParser.add_argument('-b', '--browser-times', action='store_true', dest='bBrowserTime',
                             help='convert browser time from milliseconds to date/time')
        oParser.add_argument('-f', '--filter', action='store', dest='sFilter',
                             help='filter the results', metavar='FILTER')
        oParser.add_argument('-d', '--dimensions', action='store_true', dest='bShowDims',
                             help='add the dimension names in the header with the translations')
        oParser.add_argument('-r', '--results', action='store_true', dest='bResults',
                             help='get the results only')
        oParser.add_argument('-s', '--skip-header', action='store_true', dest='bSkipHeader',
                             help='skip the header row')
        oParser.add_argument('-u', '--users', action='store_true', dest='bUsers',
                             help='get the user information only')
        oParser.add_argument('-v', '--validate', action='store_true', dest='bValidate',
                             help='validate only, providing counts')
        oParser.add_argument('sStartDate', help='starting date (required)', metavar='START-DATE')
        oParser.add_argument('sEndDate', help='ending date (optional)', metavar='END-DATE', nargs='?')
        return oParser

    def getCmdOptions (self):
        """Get all command line args as an object, stored in a static variable"""

        # Return the attribute if set, otherwise set 
        oParser = self.getArgParser()
        self.oCmdOptions = oParser.parse_args()

        # End date is optional - if not set, use the start date (one day only)
        if self.oCmdOptions.sEndDate == None:
            self.oCmdOptions.sEndDate = self.oCmdOptions.sStartDate

        # Validate the days
        if not self.validDate(self.oCmdOptions.sStartDate):
            usage('invalid start date format: "%s"' % (self.oCmdOptions.sStartDate))
        elif not self.validDate(self.oCmdOptions.sEndDate):
            usage('invalid end date format: "%s"' % (self.oCmdOptions.sEndDate))

    def getConfigValue (self, sSection, sKey, bRequired = True):
        """Get a configuration value"""
        sValue = None
        if self.oConfig.has_section(sSection):
            if self.oConfig.has_option(sSection, sKey):
               sValue =  self.oConfig[sSection][sKey]
            elif bRequired:
                errorMsg("Missing configuration option: %s:%s" % (sSection, sKey))
        elif bRequired:
            errorMsg("Missing configuration section: " + sSection)
        return sValue

    def getConfigDimensions (self, sSection, bRequired = True):
        """Get a set of configuration dimensions"""
        aSectionConfig = self.getConfigSectionArray(sSection, bRequired)
        return [ { 'name': 'ga:{0}'.format(sElement) } for sElement in aSectionConfig ]
    
    def getConfigSectionArray (self, sSection, bRequired = True):
        """Get a configuration section as an array, ignoring the option keys"""
        aSectionConfig = []
        if self.oConfig.has_section(sSection):
            for sKey, sValue in self.oConfig.items(sSection):
                aSectionConfig.append(sValue)
        elif bRequired:
            errorMsg("Missing configuration section: " + sSection)
        return aSectionConfig

    def getConfigDimensionDict (self, sSection, bRequired = True):
        """Get a dictionary of configuration dimensions"""
        dSectionConfig = self.getConfigSectionDict(sSection, bRequired)
        return { 'ga:{0}'.format(sKey): sValue for sKey, sValue in dSectionConfig.items() }
    
    def getConfigSectionDict (self, sSection, bRequired = True):
        """Get a configuration section as a dictionary"""
        dSectionConfig = {}
        if self.oConfig.has_section(sSection):
            dSectionConfig = self.oConfig[sSection]
        elif bRequired:
            errorMsg("Missing configuration section: " + sSection)
        return dSectionConfig

    def y (self):
        try:
            print("trying")
            return self.z
        except AttributeError:
            print("not set")
            self.z = 2
            return self.z
        
    def getAnalytics (self):
        """Initializes an analyticsreporting service object"""
        try:
            return self.oAnalytics
        except AttributeError:
            oCredentials = ServiceAccountCredentials.from_p12_keyfile(self.SERVICE_ACCOUNT_EMAIL,
                                                                      self.KEY_FILE_LOCATION,
                                                                      scopes=self.SCOPES)
            oHttp = oCredentials.authorize(httplib2.Http())
            self.oAnalytics = build('analytics', 'v4', http=oHttp, discoveryServiceUrl=self.DISCOVERY_URI)
            return self.oAnalytics

    def getDimensionFilters (self):
        """Get optional dimension filters"""

        if self.oCmdOptions.sFilter == None:
            return None

        # Filters follow the format shown above in program description
        reFilter = re.compile(' *(ga:\w+) +(%s) (.*)' % '|'.join(GA_SEARCH_OPS))
        
        # Break apart each filter
        aFilters = []
        for sFilter in self.oCmdOptions.sFilter.split(' AND '):
            oMatch = reFilter.match(sFilter)
            if oMatch == None:
                errorMsg("Invalid filter arguments: " + self.oCmdOptions.sFilter)
            aFilters.append({
                'dimensionName': oMatch.group(1),
                'operator':      oMatch.group(2),
                'expressions':   [ oMatch.group(3).strip() ]
            })
            
        return aFilters
            
    def getReport (self, aDimensions, sPageToken = None):
        """Get the report with certain dimensions and starting element"""

        # Get any dimension filters, checking for errors immediately
        aDimFilters = self.getDimensionFilters()

        # Get the analytics connection
        oAnalytics = self.getAnalytics()

        # Add the dimensions and date range (from the command line options)
        aBatchParams = copy.deepcopy(self.BATCH_PARAMS)
        aBatchParams['reportRequests'][0]['viewId']     = self.VIEW_ID
        aBatchParams['reportRequests'][0]['dimensions'] = aDimensions
        aBatchParams['reportRequests'][0]['dateRanges'] = [ { 'startDate': self.oCmdOptions.sStartDate,
                                                              'endDate': self.oCmdOptions.sEndDate } ]
        if aDimFilters:
            aBatchParams['reportRequests'][0]['dimensionFilterClauses'] = [{ 'filters': aDimFilters }]

        if sPageToken != None:
            aBatchParams['reportRequests'][0]['pageToken'] = sPageToken
        else:
            aBatchParams['reportRequests'][0].pop('pageToken', None)
        return oAnalytics.reports().batchGet(body=aBatchParams).execute()

    def getResponse (self, oResponse, bHeader):
        """Gets all the rows from the response"""

        sNextPageToken = None
        aAllRows = []
        for oReport in oResponse.get('reports', []):
            oColumnHeader     = oReport.get('columnHeader', {})
            sNextPageToken    = oReport.get('nextPageToken', None)
            aDimensionHeaders = oColumnHeader.get('dimensions', [])
            aRows             = oReport.get('data', {}).get('rows', [])

            # Create the header row
            if bHeader == True:
                aAllRows.append(aDimensionHeaders)

            # Save all the rows, removing the non-ascii characters
            for oRow in aRows:
                aDimensions = oRow.get('dimensions', [])
                for i in range(0, len(aDimensions)):
                    aDimensions[i] = aDimensions[i].encode('ascii', 'ignore').decode('ascii')
                aAllRows.append(aDimensions)
        return { 'rows': aAllRows, 'nextPageToken': sNextPageToken }

    def outputRows (self, aRows, sFile = None):
        """CSV output, optionally saving to a file"""

        # Use standard output or write to a file
        if sFile == None:
            fpOutput = sys.stdout
        else:
            fpOutput = open(sFile, 'w')

        # Translate the header values unless skipping it altogether
        if self.oCmdOptions.bSkipHeader:
            aRows.pop(0)
        else:
            for n in range(0, len(aRows[0])):
                sOriginal = aRows[0][n]
                if sOriginal in self.CUSTOM_DIMENSIONS:
                    sTranslate = self.CUSTOM_DIMENSIONS[aRows[0][n]]
                    if self.oCmdOptions.bShowDims:
                        aRows[0][n] = '%s (%s)' % (sTranslate, sOriginal)
                    else:
                        aRows[0][n] = sTranslate

        # Get the CSV writer, using special options
        oFile = csv.writer(fpOutput)

        # Write all rows
        for aRow in aRows:
            oFile.writerow(aRow)

        # Close the file unless writing to standard output
        if sFile == None:
            fpOutput.close()


    def getStartDate (self, bReset = False):
        """Get the start date from the options, translating day referrals"""

        if not hasattr(getStartDate, 'oStartDate') or bReset == True:
            if re.search(r'^(today|yesterday|[0-9]+daysAgo)$', self.oCmdOptions.sStartDate):
                if self.oCmdOptions.sStartDate == 'today':
                    oStartDate = date.today()
                elif self.oCmdOptions.sStartDate == 'yesterday':
                    oStartDate = date.today() - timedelta(days=1)
                else:
                    oMatch = re.search(r'^([0-9]+)daysAgo', self.oCmdOptions.sStartDate)
                    iDaysAgo = int(oMatch.group(1))
                    oStartDate = date.today() - timedelta(days=iDaysAgo)
            else:
                oMatch = re.search(r'([0-9]{4})-([0-9]{2})-([0-9]{2})', self.oCmdOptions.sStartDate)
                oStartDate = date(int(oMatch.group(1)), int(oMatch.group(2)), int(oMatch.group(3)))
            getStartDate.oStartDate = oStartDate
        return getStartDate.oStartDate

    def processReport (self, aDimensions):
        """Get a full report, returning the rows"""

        # Get the first set
        oReport   = self.getReport(aDimensions)
        oResponse = self.getResponse(oReport, True)
        aRows     = oResponse.get('rows')

        # Add any additional sets
        while oResponse.get('nextPageToken') != None:
            oResponse = self.getReport(aDimensions, oResponse.get('nextPageToken'))
            oResponse = self.getResponse(oResponse, False)
            aRows.extend(oResponse.get('rows'))

        return aRows

    def addMiscDimensions (self, aResults):
        """Add the miscellaneous dimensions to the results"""

        # Loop over each miscellaneous dimension set
        for aBatchDimSet in self.BATCH_DIMENSIONS:

            # Create an empty result set
            aEmpty = []
            for sField in aBatchDimSet:
                aEmpty.append(INVALID_VALUE)

            # Build the batch elements with first 2 common elements for user and time
            aDimSet = [ { 'name': CUSTOM_DIM_USER }, { 'name':  CUSTOM_DIM_TIME } ]
            aDimSet.extend(aBatchDimSet)
            aBatchResults = processReport(aDimSet)

            # Add the header, removing the 2 common elements (time field may be "ga:nthMinute")
            aHeader = aBatchResults.pop(0)
            aHeader.pop(0)
            aHeader.pop(0)
            aResults[0].extend(aHeader)

            # Create 2-dimensional dictionary with the two common dimensions
            aMiscByUserTime = {}
            for aRow in aBatchResults:
                sUserId = aRow.pop(0)
                sTimeId = aRow.pop(0)
                if sUserId not in aMiscByUserTime:
                    aMiscByUserTime[sUserId] = {}
                aMiscByUserTime[sUserId][sTimeId] = aRow

            # Add the each row to the results, skipping the header row (n == 0)
            for n in range(1, len(aResults)):

                # Get the common dimensions for user and time
                aRow    = aResults[n]
                sUserId = aRow[0]
                iTimeId = int(aRow[1])

                # User exists in the miscellaneous results
                if sUserId in aMiscByUserTime:

                    # Loop through each element, using the element with an earlier or equal time
                    aMatch = None
                    for sRowMinutes in aMiscByUserTime[sUserId]:
                        if aMatch == None or int(sRowMinutes) <= iTimeId:
                            aMatch = aMiscByUserTime[sUserId][sRowMinutes]
                    aRow.extend(aMatch)
                else:
                    aRow.extend(aEmpty)


    def combineReports (self, aUsers, aResults):
        """Combine both reports into a single report"""

        # First column is user ID - throw out user ID from results and combine to create the complete header 
        aHeader        = aUsers.pop(0)
        aHeaderResults = aResults.pop(0)
        aHeaderResults.pop(0)
        aHeader.extend(aHeaderResults)

        # Start the rows, adding the header
        aAllRows = [ aHeader ]

        # Get all users by user ID
        aUsersById = { }
        for aRow in aUsers:
            aUsersById[aRow[0]] = aRow

        # Go through each result, adding the user information (session ID is first element in results)
        for aResult in aResults:
            sUserId = aResult.pop(0)
            if sUserId in aUsersById:
                aAllRows.append(aUsersById[sUserId] + aResult)
            else:
                errorMsg('result found without matching user information for user ID: %s' % (sUserId))

        return aAllRows

    def validDate (self, sDate):
        """Validate a date string"""
        if sDate == None:
            return False
        return (re.search(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$', sDate) or
                re.search(r'^(today|yesterday|[0-9]+daysAgo)$', sDate))

    def downloadCombined (self, sFile = None):
        """Download the users and results, adding the miscellaneous dimensions"""
        aUsers   = self.processReport(self.USER_DIMENSIONS)
        aResults = self.processReport(self.RESULTS_DIMENSIONS)
        self.addMiscDimensions(aResults)
        self.outputRows(combineReports(aUsers, aResults), sFile)

# Run the system
oDownload = Download()
oDownload.main()




            
