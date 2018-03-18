import datetime
import json
import time
import motor
from bson import ObjectId
from dateutil import parser
import tornado.ioloop
import tornado.web
from pymongo.errors import AutoReconnect, DuplicateKeyError


class EventsHandler(tornado.web.RequestHandler):
    """
    Main application request handler
    """

    def prepare(self):
        """override the method from the superclass to support requests with JSON content type"""
        if self.request.headers.get("Content-Type", "").startswith("application/json"):
            self.json_args = json.loads(self.request.body)
        else:
            self.json_args = None

    async def post(self):
        """ handle asynchronously multiples POST requests from devices/desktop/SDKs"""
        if not self.json_args:
            self.write({"status": "error", "details": "not valid json"})
        events = await self.process_requests(self.json_args)
        if not events:
            self.write({"status": "error"})
        else:
            print("POST request received with: {}".format(events))
            self.sent_to_db(events)
            self.write({"status": "success", "received": len(events)})

    async def process_requests(self, data):
        """Process a single request containing multiple or single event(s)"""

        if not data:
            return []

        if isinstance(data, list):
            multiple_events_list = []
            for event in data:
                created_event = self.create_event(event)
                if created_event:
                    multiple_events_list.append(created_event)
            return multiple_events_list
        # Single event
        single_event = self.create_event(data)
        return [single_event] if single_event else []

    def create_event(self, event):
        """"Create an event to be inserted to the db and add an Mongo Id to it"""

        created_event = self.validate_and_format(event)

        if created_event:
            # Generate a Mongo ObjectId (on the application side)
            created_event['_id'] = ObjectId()
            return created_event

    def validate_and_format(self, event):
        """Validate the event and set default values"""
        formatted_event = {}

        if 'event_type' in event:
            event_type = event.get('event_type', None)
            if event_type and isinstance(event_type, str) and event_type.lower() in ['impression', 'click', 'completion']:
                formatted_event['event_type'] = event_type.lower()
        else:
            formatted_event['event_type'] = 'undetected event type'

        if 'user_id' in event:
            user_id = event.get('user_id', None)
            if user_id:
                formatted_event['user_id'] = user_id.lower().strip()
        else:
            formatted_event['user_id'] = str(ObjectId())

        if 'transaction_id' in event:
            transaction_id = event.get('transaction_id', None)
            if transaction_id:
                formatted_event['transaction_id'] = transaction_id.lower().strip()
        else:
            # Create a transaction Id if not present
            formatted_event['transaction_id'] = str(ObjectId())

        if 'ad_type' in event:
            ad_type = event.get('ad_type', None)
            if ad_type:
                formatted_event['ad_type'] = ad_type.lower().strip()

        if 'date_time' in event:
            date_time = event.get('date_time', None)
            if date_time:
                try:
                    # from iso date '2012-05-29T19:30:03.283Z' to python date time
                    date_time = datetime.datetime.strptime(date_time, '%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError:
                    # can handle most date formats but could be less efficient in high scale
                    date_time = parser.parse(date_time)
                formatted_event['date_time'] = date_time
        else:
            formatted_event['date_time'] = datetime.datetime.utcnow()

        if 'time_to_click' in event:
            time_to_click = event.get('time_to_click')
            if time_to_click:
                formatted_event['time_to_click'] = time_to_click

        return formatted_event

    def sent_to_db(self, events_list):
        """ Insert event one by one to the db and if is failing invoke the failover mechanism,
            Here I didn't use bulk insert because if it fail is more complicated to handle (we need to rollback the inserted data and retry)
        """

        def insert_event(result, error):
            """ callback function for insertion to db """
            if error:
                print('retry to insert event to db error {}'.format(repr(error)))
                self.failover_insertion_mechanism(db, event)
            else:
                print('event inserted in the db {}'.format(repr(result)))

        for event in events_list:

            db_number = self.determine_shard_db_number(event)
            # access the db connections list from the request handler
            db = self.settings['db'][db_number]

            db.events.insert_one(event, callback=insert_event)

    def determine_shard_db_number(self, event):
        """
        Determine the db number for the writing the Key-Based (or Hash-Based) Partitioning (Sharding)

        Tradeoff: Adding additionnal db servers means having a different number than the previous for a given data.
        A workaround for this problem is to use Consistent Hashing.
        """

        # Used transaction_id because it's present in all event types.
        key = hash(str(event.get('transaction_id')))

        return key % len(self.settings['db'])

    async def failover_insertion_mechanism(self, db, document_to_reinsert):
        """
        Try to send the request in the next 60 sec and try to reconnect to the db for 5 min
        """
        while True:
            time.sleep(1)

            # Try for five minutes to recover from a failed db
            for _ in range(60):
                try:
                    await db.events.insert_one(document_to_reinsert)
                    print('try to write')
                    break  # Exit the retry loop
                except AutoReconnect as e:
                    print('Warning', e)
                    time.sleep(5)
                except DuplicateKeyError:
                    # It worked the first time
                    break
            else:
                raise Exception("Couldn't write!")


db_list = [
    motor.motor_tornado.MotorClient("mongodb://localhost:27017").database_1,
    motor.motor_tornado.MotorClient("mongodb://localhost:27017").database_2,
    motor.motor_tornado.MotorClient("mongodb://localhost:27017").database_3,
    motor.motor_tornado.MotorClient("mongodb://localhost:27017").database_4,
]


def make_app():
    return tornado.web.Application([
        (r"/events", EventsHandler),
    ],
        # pass the dbs connections list to the application constructor and makes it available to request handlers
        db=db_list
    )


if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
