import concurrent.futures
import datetime
import json
import os
import uuid
from datetime import datetime, timedelta
from pprint import pprint

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from pymongo import MongoClient
from pypebbleapi import Timeline
from requests.auth import HTTPBasicAuth

mongo_client = MongoClient(os.getenv('MONGODB_URL'))
print("Created mongo_client")

database = mongo_client['transmission-remote']
print("Created database")

usercredentials = database.usercredentials
print("Created usercredentials")

timeline = Timeline()


def torrent_action(torrent):
    threshold_days = timedelta(days=3)
    if (torrent['eta'] < 0 and torrent['doneDate'] == 0):
        return "delete"

    if (torrent['eta'] >= 0):
        return "put"

    is_showable = datetime.utcfromtimestamp(torrent['doneDate']) >= datetime.today() - threshold_days

    if is_showable:
        return "showable"
    else:
        return "not_showable"
    return "delete"

def update_timeline():
    futures = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=50) as executor:
        for user in usercredentials.find():
            # print(user)
            futures.append(executor.submit(contact_transmission, user=user, xTransmissionSessionId=""))
    for future in concurrent.futures.as_completed(futures):
        future.result()
        # contact_transmission(user=user, xTransmissionSessionId="")


def convert_value_with_unit(value):
    val_kbyte = 1024
    val_mbyte = val_kbyte * val_kbyte
    val_gbyte = val_mbyte * val_kbyte

    if (value < val_mbyte):
        return '{:.1f} KB/s'.format(value / val_kbyte)
    elif (value >= val_mbyte and value < val_gbyte):
        return '{:.2f} MB/s'.format(value / val_mbyte)
    elif (value >= val_gbyte):
        return '{:.2f} GB/s'.format(value / val_gbyte)


def create_pin_from_torrent(torrent, uuid):
    pin = dict(
        id=uuid,
        actions=[dict(
            title="Open WatchApp",
            type="openWatchApp"
        )],
        layout=dict(
            type='calendarPin',
            tinyIcon="system://images/SCHEDULED_EVENT",
            title=torrent['name'],
        )
    )

    if (torrent['eta'] >= 0):                                # Downloading torrent
        time = datetime.utcnow() + timedelta(seconds=torrent['eta'])
        pin['time'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')
        pin['layout']['headings'] = ["Downloading", "Uploading", "ETA"]
        pin['layout']['paragraphs'] = [
            convert_value_with_unit(torrent['rateDownload']),
            convert_value_with_unit(torrent['rateUpload']),
            str(timedelta(seconds=torrent['eta']))
        ]
    elif (torrent['doneDate'] != 0):             # Torrent in seed
        time = datetime.utcfromtimestamp(torrent['doneDate'])
        pin['time'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')
        pin['layout']['headings'] = ["Status", "on"]
        pin['layout']['paragraphs'] = ["Completed", time.strftime('%d/%m/%Y')]
        pin['reminders'] = [dict(
            time=time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            layout=dict(
                type="genericReminder",
                tinyIcon="system://images/SCHEDULED_EVENT",
                title="You have finished to download " + torrent['name'],
            )
        )]

    return pin


def contact_transmission(user, xTransmissionSessionId):
    form = {
            'arguments': {
                'fields': ['id', 'name', 'eta', 'doneDate', 'hashString', 'rateDownload' , 'rateUpload', 'doneDate']
            },
            'method': 'torrent-get',
            'tag': 39693
        }
    headers = {
            'X-Transmission-Session-Id': xTransmissionSessionId
        }
    try:
        r = requests.post(user['url'], auth=HTTPBasicAuth(user['username'], user['password']),
                      headers=headers, data=json.dumps(form), timeout=5)
    except Exception:
        print("{} not reachable".format(user['url']))
        return
    if (r.status_code == 409):
        xTransmissionSessionId = r.headers["x-transmission-session-id"]
        contact_transmission(user, xTransmissionSessionId)
        return
    if r.status_code != 200:
        print("ERROR: status_code is {}: url {}".format(r.status_code, user['url']))
        return

    torrents = r.json()['arguments']['torrents']
    user.setdefault('pins', dict())
    for torrent in torrents:

        user['pins'].setdefault(torrent['hashString'], [str(uuid.uuid4()), 1])
        # usercredentials.find_one_and_update({'token': user['token']}, {'$set': {'pins': user['pins']}})

        action = torrent_action(torrent)

        if action == "put":
            pin = create_pin_from_torrent(torrent, user['pins'][torrent['hashString']][0])

            try:
                timeline.send_user_pin(
                    user_token=user['token'],
                    pin=pin,
                )
                print("User: {}, sent pin {} successfully!".format(user['token'], torrent['name']))
                usercredentials.find_one_and_update({'token': user['token']}, {'$set': {'pins': user['pins']}})

            except Exception as e:
                if e.response.status_code == 410:
                    print("User: {} invalid, removing it from database".format(user['token']))
                    usercredentials.delete_one({'token': user['token']})
                    break
                print("Send pin failed to user {}".format(user['token']))

        elif action == "delete":
            try:
                timeline.delete_user_pin(user['token'], user['pins'][torrent['hashString']][0])
            except Exception as e:
                if e.response.status_code == 410:
                    print("User: {} invalid, removing it from database".format(user['token']))
                    usercredentials.delete_one({'token': user['token']})
                    break
                if e.response.status_code == 429:
                    print("User {}, rate limit exceeded!".format(user['token']))
                    break
                print("Delete pin failed of user {}, status_code= {}".format(user['token'], e.response.status_code))
            else:
                print("User: {}, pin {} deleted successfully!".format(user['token'], torrent['name']))
                del user['pins'][torrent['hashString']]
                usercredentials.find_one_and_update({'token': user['token']}, {'$set': {'pins': user['pins']}})

        elif action == "showable":
            is_showable = user['pins'][torrent['hashString']][1]
            if not is_showable:
                print("User: {}, pin {} already sent".format(user['token'], torrent['name']))
                continue

            pin = create_pin_from_torrent(torrent, user['pins'][torrent['hashString']][0])
            try:
                timeline.send_user_pin(
                    user_token=user['token'],
                    pin=pin
                )
            except Exception as e:
                if e.response.status_code == 410:
                    print("User: {} invalid, removing it from database".format(user['token']))
                    usercredentials.delete_one({'token': user['token']})
                    break
                print("Send pin failed to user {}".format(user['token']))
            else:
                print("User: {}, sent pin {} successfully!".format(user['token'], torrent['name']))
                user['pins'][torrent['hashString']][1] = 0
                usercredentials.find_one_and_update({'token': user['token']}, {'$set': {'pins': user['pins']}})

        elif action == "not_showable":
            print("User: {}, pin {} is not showable".format(user['token'], torrent['name']))

    if 'pins' in user:
        pins_sent = set(user['pins'].keys())
        pins_in_transmission = set(torrent['hashString'] for torrent in torrents)
        pins_to_eliminate = pins_sent - pins_in_transmission

        for hashstring in pins_to_eliminate:
            timeline.delete_user_pin(user['token'], user['pins'][hashstring][0])

            del user['pins'][hashstring]
            usercredentials.find_one_and_update({'token': user['token']}, {'$set': {'pins': user['pins']}})

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    update_timeline()
    scheduler.add_job(update_timeline, 'interval', seconds=120)
    scheduler.start()
