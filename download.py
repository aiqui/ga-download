#!/usr/bin/env python

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
from pathlib import Path
from OpenSSL import crypto
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
import pprint

LOCAL_DIR   = os.path.dirname(os.path.realpath(__file__))
CONFIG_FILE = "download.cfg"

# Maximum number of dimensions "batches" (can be increased if neeeded)
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

def getPprint ():
    """Get the pretty print object instance"""
    if not hasattr(getPprint, "pp"):
        getPprint.pp = pprint.PrettyPrinter(indent=2)
    return getPprint.pp

def getArgParser ():
    """Management of the command-line argument parser"""
    oParser = argparse.ArgumentParser(description=PROG_DESC, formatter_class=argparse.RawTextHelpFormatter)
    oParser.add_argument('-d', '--delimiter', action='store', dest='sDelimiter',
                         help='delimit the output with this character', metavar='DELIMITER')
    oParser.add_argument('-f', '--filter', action='store', dest='sFilter',
                         help='filter the results', metavar='FILTER')
    oParser.add_argument('-o', '--output-file', action='store', dest='sOutputFile',
                         help='output file (instead of standard output)', metavar='FILE')
    oParser.add_argument('-r', '--results', action='store_true', dest='bResults',
                         help='get the results only')
    oParser.add_argument('-s', '--skip-header', action='store_true', dest='bSkipHeader',
                         help='skip the header row')
    oParser.add_argument('-u', '--users', action='store_true', dest='bUsers',
                         help='get the user information only')
    oParser.add_argument('-v', '--validate', action='store_true', dest='bValidate',
                         help='validate only, providing counts')
    oParser.add_argument('-x', '--debug-mode', action='store_true', dest='bDebugMode',
                         help='debug mode that provides queries, counts and other information')
    oParser.add_argument('--dimension-names', action='store_true', dest='bAddDimNames',
                         help='add the dimension names in the header with the translations')
    oParser.add_argument('--skip-translation', action='store_true', dest='bSkipDimTranslate',
                         help='skip the translation of dimension names in the header')
    oParser.add_argument('sStartDate', help='starting date (required)', metavar='START-DATE')
    oParser.add_argument('sEndDate', help='ending date (optional)', metavar='END-DATE', nargs='?')
    return oParser

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

        # First dimension of users and results must be equal
        if self.USER_DIMENSIONS[0] != self.RESULTS_DIMENSIONS[0]:
            errorMsg("First dimension of user and result groups must be equal")
        
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
                

    def getCmdOptions (self):
        """Get all command line args as an object, stored in a static variable"""

        # Return the attribute if set, otherwise set 
        oParser = getArgParser()
        self.oCmdOptions = oParser.parse_args()

        # Output file option - validate path by creating and deleting
        if self.oCmdOptions.sOutputFile and not os.path.isfile(self.oCmdOptions.sOutputFile):
            try: 
                Path(self.oCmdOptions.sOutputFile).touch()
                os.remove(self.oCmdOptions.sOutputFile)
            except FileNotFoundError:
                errorMsg("Invalid output file: " + self.oCmdOptions.sOutputFile)
        
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

    def getAnalytics (self):
        """Initializes an analyticsreporting service object"""
        if hasattr(self, 'oAnalytics'):
            return self.oAnalytics
        try: 
            oCredentials = ServiceAccountCredentials.from_p12_keyfile(self.SERVICE_ACCOUNT_EMAIL,
                                                                      self.KEY_FILE_LOCATION,
                                                                      scopes=self.SCOPES)
            oHttp = oCredentials.authorize(httplib2.Http())
            self.oAnalytics = build('analytics', 'v4', http=oHttp, discoveryServiceUrl=self.DISCOVERY_URI)
            return self.oAnalytics
        except BaseException as e:
            errorMsg("Unable to build Google Analytics authorization: " + str(e))
            

    def getDimensionFilters (self):
        """Get optional dimension filters"""

        if self.oCmdOptions.sFilter == None:
            return None

        # Filters follow the format shown above in program description
        reFilter = re.compile(' *(ga:\w+) +(%s) (.*)' % '|'.join(GA_SEARCH_OPS))
        
        # Break apart each filter
        aFilters = []
        sFilter  = self.oCmdOptions.sFilter
        if re.search(r' AND ', sFilter):
            sOperator = 'AND'
            aSplit    = sFilter.split(' AND ')
        elif re.search(r' OR ', sFilter):
            sOperator = 'OR'
            aSplit    = sFilter.split(' OR ')
        else:
            sOperator = None
            aSplit    = [ sFilter ]

        for sFilter in aSplit:
            oMatch = reFilter.match(sFilter)
            if oMatch == None:
                errorMsg("Invalid filter arguments: " + self.oCmdOptions.sFilter)
            aFilters.append({
                'dimensionName': oMatch.group(1),
                'operator':      oMatch.group(2),
                'expressions':   [ oMatch.group(3).strip() ]
            })

        return { "operator": sOperator, "filters": aFilters }
            
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
            aBatchParams['reportRequests'][0]['dimensionFilterClauses'] = [ aDimFilters ]

        if sPageToken != None:
            aBatchParams['reportRequests'][0]['pageToken'] = sPageToken
        else:
            aBatchParams['reportRequests'][0].pop('pageToken', None)

        if self.oCmdOptions.bDebugMode:
            print("getReport - batch params: ")
            getPprint().pprint(aBatchParams)
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
                
        if self.oCmdOptions.bDebugMode:
            if sNextPageToken:
                print("Response: number of rows %d with next token %s" % (len(aAllRows) - 1, sNextPageToken))
            else:
                print("Response: number of rows %d (no next page)" % (len(aAllRows) - 1))
                
        return { 'rows': aAllRows, 'nextPageToken': sNextPageToken }

    def outputRows (self, aRows):
        """CSV output, optionally saving to a file"""

        # Use standard output or write to a file
        if self.oCmdOptions.sOutputFile == None:
            fpOutput = sys.stdout
        else:
            fpOutput = open(self.oCmdOptions.sOutputFile, 'w')

        # Translate the header values unless skipping it altogether
        if self.oCmdOptions.bSkipHeader:
            aRows.pop(0)
        elif not self.oCmdOptions.bSkipDimTranslate:
            for n in range(0, len(aRows[0])):
                sOriginal = aRows[0][n]
                if sOriginal in self.CUSTOM_DIMENSIONS:
                    sTranslate = self.CUSTOM_DIMENSIONS[aRows[0][n]]
                    if self.oCmdOptions.bAddDimNames:
                        aRows[0][n] = '%s (%s)' % (sTranslate, sOriginal)
                    else:
                        aRows[0][n] = sTranslate

        # Get the CSV writer, using special options
        if self.oCmdOptions.sDelimiter:
            oFile = csv.writer(fpOutput, delimiter=self.oCmdOptions.sDelimiter)
        else:
            oFile = csv.writer(fpOutput)

        # Write all rows
        for aRow in aRows:
            oFile.writerow(aRow)

        # If writing to a file, close and provide a status message
        if self.oCmdOptions.sOutputFile:
            fpOutput.close()
            print("Download complete, %d rows, output file: %s" % (len(aRows), self.oCmdOptions.sOutputFile))


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
                aEmpty.append(self.INVALID_VALUE)

            # Add the batch dimensions to the common "stitch" elements and get the report
            aDimSet = self.STITCH_DIMENSIONS.copy()
            aDimSet.extend(aBatchDimSet)
            aBatchResults = self.processReport(aDimSet)

            # Add the header to the results header, removing the stitch elements
            aHeader = aBatchResults.pop(0)
            for sStitchCol in self.STITCH_DIMENSIONS:
                aHeader.pop(0)
            aResults[0].extend(aHeader)

            # Create dictionary with the stitch elements forming the key
            aStitchElements = {}
            for aRow in aBatchResults:
                aKey = []
                for sStitchCol in self.STITCH_DIMENSIONS:
                    aKey.append("%s = %s" % (sStitchCol, aRow.pop(0)))
                sKey = ' && '.join(aKey)
                aStitchElements[sKey] = aRow

            # Add the each row to the results, skipping the header row (n == 0)
            for n in range(1, len(aResults)):

                # Get this row
                aRow = aResults[n]
                
                # Get the key using the stitch elements
                aKey = []
                for sStitchCol in self.STITCH_DIMENSIONS:
                    iResultCol = self.RESULTS_DIMENSIONS.index(sStitchCol)
                    aKey.append("%s = %s" % (sStitchCol, aRow[iResultCol]))
                sKey = ' && '.join(aKey)

                # Element exists - add to the results
                if sKey in aStitchElements:
                    aRow.extend(aStitchElements[sKey])
                else:
                    aRow.extend(aEmpty)


    def combineReports (self, aUsers, aResults):
        """Combine both reports into a single report"""

        # First column is common - throw out from results and combine to create the complete header 
        aHeader        = aUsers.pop(0)
        aHeaderResults = aResults.pop(0)
        aHeaderResults.pop(0)
        aHeader.extend(aHeaderResults)

        # Get the name of the common column
        sCommonColumn = aHeader[0]

        # Start the rows, adding the header
        aAllRows = [ aHeader ]

        # Get all users by the first column, which must be the same as the results
        aUsersByCommonId = { }
        for aRow in aUsers:
            aUsersByCommonId[aRow[0]] = aRow

        # Go through each result, adding the user information
        for aResult in aResults:

            # First element will be common with users
            sCommonId = aResult.pop(0)

            # User found with common ID - combine user and result information into a single row
            if sCommonId in aUsersByCommonId:
                aAllRows.append(aUsersByCommonId[sCommonId] + aResult)

            # No user found with common ID - this should never happen
            else:
                errorMsg('result but no user found with %s value of %s' % (sCommonColumn, sCommonId))

        return aAllRows

    def validDate (self, sDate):
        """Validate a date string"""
        if sDate == None:
            return False
        return (re.search(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$', sDate) or
                re.search(r'^(today|yesterday|[0-9]+daysAgo)$', sDate))

    def downloadCombined (self):
        """Download the users and results, adding the miscellaneous dimensions"""
        aUsers   = self.processReport(self.USER_DIMENSIONS)
        aResults = self.processReport(self.RESULTS_DIMENSIONS)
        self.addMiscDimensions(aResults)
        self.outputRows(self.combineReports(aUsers, aResults))

# Run the system
oDownload = Download()
oDownload.main()




            
