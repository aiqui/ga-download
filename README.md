# Google Analytics Download System
*ga-download*

**This python program will download multiple sets of dimensions from Google Analytics, combining them by using common dimensions.**

## Basic Concepts

Google Analytics (GA) limits a batch download to 7 dimensions.  This 
program stitches together multiple sets of dimensions using common 
"stitch" dimensions.

This program assumes that you have already defined all of your dimensions.
We are restricted from providing any personal information to GA, but you
can use a numerical user ID.  Two common custom "stitch" dimensions would be:
* numerical user ID (ideally encrypted)
* browser timestamp

For this program to work correctly, these dimensions must defined 
with every call to the [GA measurement protocol](https://developers.google.com/analytics/devguides/collection/protocol/v1/reference).

With every GA batch download, if one of the dimensions is not defined,
the row will not be returned even if all other dimensions are defined.
So you should be grouping similar dimensions, e.g. mobile platform dimensions.

### User Dimensions

This program assumes there are common dimensions for all users, for
example the user ID and country.  These dimensions will not change
throughout a given session.

### Result Dimensions

Unlike user dimensions, "result" dimensions will commonly record the 
different user actions that change with every event, for example:
* eventCategory
* eventAction
* eventLabel

The user and result dimensions are combined through a common dimension, e.g. the user ID.

### Additional dimensions

Additional batches of dimensions are grouped so that all of the dimension
are defined, because no row will be returned if one of the dimensions is
not defined.  The "stitch" dimensions will bring together each set with
all the others. 

### Defining a Session

You will need decide what defines a session.  One solution would be a 
custom dimension with a session ID.   Google Analytics has its own concept
of a session and the internal GA session ID is not available for downloading.

## API Access

Establish GA API access using the [API authorization guide](https://developers.google.com/analytics/devguides/reporting/core/v4/authorization).  From this you'll have:
* private key
* service account e-mail address
* GA view ID

## Basic Configuration

Copy and edit the configuration template:

    cp download.cfg.template download.cfg
    
Set each of these values in the configuration file:
* SERVICE_ACCOUNT_EMAIL
* KEY_FILE_LOCATION
* VIEW_ID

Provide translations for each of the custom dimensions (there is an option
to avoid the translations):

    [custom-dimensions]
    dimension1  = Example 1
    dimension2  = Example 2
    ...

Determine the user and result dimensions (review the concepts above):

    # Dimensions for user information only
    [user-dimensions]
    dim-1 = dimension1
    dim-2 = dimension2
    ...
    
    # Standard results dimensions - the first element must match 
    # the first element of user-dimensions
    [results-dimensions]
    dim-1 = dimension1
    dim-2 = dimension9
    ...
    
 You can define each of the sets of batch dimensions, for example:
 
     [batch-dimensions-1]
     dim-1 = userType
     dim-2 = browser
     dim-3 = browserSize
     dim-4 = browserVersion
     dim-5 = operatingSystem
     
 Determine the common dimensions that are used to "stitch" the results
 dimensions to each of the batch sets, for example:
 
     # Custom dimensions that can stitch multiple batch queries together
     [stitch-dimensions]
     dim-1 = dimension1
     dim-2 = dimension9
     dim-3 = dimension11
     

## Python Libraries

The program uses Python 3.  Call [pip](https://docs.python.org/3/installing/index.html) to install any missing libraries.  
You'll also need to install the [Google Analytics client library](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py).

## Running the Program

You can see a complete list of the options easily:
    
    ./download.py -h
    
To begin testing the configuration, download the list of users for today:

    ./download.py --users today
    
If that works, you can download the results separately:

    ./download.py --results today
    
Combining the users, results and separate batches:

    ./download.py today
    
You can filter for a specific user or session easily:

    ./download.py --filter "ga:dimension1 EXACT 123456" today
    
When you are first starting, you'll probably need to add the debug functionality, for example:

    ./download.py --debug-mode today
