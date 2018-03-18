# Logging service for events

### Requirements
- Python 3.6+ (for async features)
- Python libraries in the `requirements.txt`
- MongoDB 3.6.3

### Installation

`pip install -r requirements.txt`

### Feature
- Use Tornado asynchronous framework. By using non-blocking network I/O, Tornado can scale to tens of thousands of open connections
- Motor (MongoDB driver) presents a callback- or Future-based API for non-blocking access to MongoDB from Tornado or asyncio.
- The process could be run independently on the same machine (or on other machines) and we can fork it (i.e as the number of CPUs) and use same or different listening port.


### Usage:
- Support POST requests in JSON

###### Single event request:

    POST /events HTTP/1.1
    Host: localhost:8888
    Content-Type: application/json

    {
    	"event_type": "click",
    	"ad_type": "Interstitial",
    	"time_to_click": "4sec"
    }

###### Multiple event request:

    POST /events HTTP/1.1
    Host: localhost:8888
    Content-Type: application/json

    [
    	{
    	"event_type": "click",
    	"ad_type": "Interstitial",
    	"time_to_click": "4sec",
    	"transaction_id": "5aada9520ec4d7a123be3f19"
    	},
    	{
    	"event_type": "impression",
    	"ad_type": "InBanner",
    	"transaction_id": "6aada9520ec4d7a123be3f31"
    	},
    	{
    	"event_type": "completion",
    	"date_time": "2017-05-29T19:30:03.283Z",
    	"ad_type": "InTop",
    	"transaction_id": "7aada9520ec4d7a123be3f19"
    	}
    ]

![architecture](https://github.com/ychek/events-logging-service/blob/master/Suggested_Architecture.png)

#### Enjoy!
