#!/usr/local/bin/python

import subprocess
import json
import yaml
import dateutil.parser
import datetime
import boto

# Temporary settings that will be sent in via command line
app_id = "fdyc2rm7kvqcnftgyjzsrbawer"
event_types = ["entity_create","entity_update"]
timestamp = "2015-08-05 22:09:02.832285 +0000"
uuid = "a9e13c44-343c-4bf5-8849-2af66c2b147c"
time_range = 15
s3_bucket_name = "janrain.analytics"
check_user = True

# For a single user, builds the s3 urls we need to retrieve analytics events
# Looks ahead if the timestamp is within 15 minutes of a different hour
# Also calls get_s3_keys, which actually gets the data
def build_capture_event_url(timestamp,event_types):
	global app_id
	global time_range

	url_string_array = []

	temp_date = dateutil.parser.parse(timestamp)
	date_array = [temp_date]
	if temp_date.minute <= time_range:
		date_array.append(temp_date + datetime.timedelta(hours=-1))
	if temp_date.minute + time_range >= 60:
		date_array.append(temp_date + datetime.timedelta(hours=1))
	
	for date in date_array:	
		date_string_array = [
			str(date.year),
			str(date.month),
			str(date.day),
			str(date.hour),
			'00',
			'00'
		]

		for event in event_types:
			url_string = (
				'capture/' + event + '/'
				+ reduce(lambda x,y: x + '/' + y, date_string_array) 
				+ '/' + app_id + '/'
			)
			url_string_array.append(url_string)
			
	return url_string_array

# For each s3 url, goes and checks the analytics
def get_s3_keys(s3_url):
	global s3_bucket_name
	
	s3_key_array = []
	s3 = boto.s3.connect_to_region('us-east-1')
	s3_bucket = s3.get_bucket(s3_bucket_name)

	for item in s3_bucket.list(prefix=s3_url):
		object_array = []
		item_time = dateutil.parser.parse(item.name[-28:-9] + ' +0000')

		temp_string = item.get_contents_as_string()
		num_objects = temp_string.count('\n')
		for j in range(num_objects):
			if temp_string.find('\n') > -1:
				result_object = yaml.load(temp_string[temp_string.find('{'):temp_string.find('\n')])
				
				# Checks whether the uuid of the analytics event matches the user

				if check_user:
					if user_object['uuid'] == result_object['value']['uuid']:
						s3_key_array.append(result_object)
				else:
					s3_key_array.append(result_object)

				temp_string = temp_string[temp_string.find('\n')+1:-1]
	return s3_key_array

# master function to pass into multithreads
def find_user_events(user):

	url_string_array = build_s3_url(user)
	s3_results = []
	for url in url_string_array:
		s3_results += get_s3_keys(url,user)

	return update_array

def main(argv):

	user_list = calculate_last_user_events(user_list)

	for user in user_list:
		user_response = find_user_events(user)
		for response in user_response:
			json_file.write(str(response)+'\n')

	return

if __name__ == "__main__":
    main(sys.argv[1:])

