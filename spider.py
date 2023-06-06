#!/usr/bin/env python3
import asyncio
import json
import re
import sqlite3
import time

from nio import (AsyncClient, CallEvent, JoinError, MatrixRoom, MegolmEvent, PowerLevelsEvent, RedactionEvent, RoomAvatarEvent, RoomCreateEvent, RoomEncryptedAudio, RoomEncryptedFile, RoomEncryptedImage, RoomEncryptedVideo, RoomEncryptionEvent, RoomGuestAccessEvent, RoomHistoryVisibilityEvent,
                 RoomJoinRulesEvent, RoomMemberEvent, RoomMessageAudio, RoomMessageEmote, RoomMessageFile, RoomMessageImage, RoomMessageNotice, RoomMessageText, RoomMessageUnknown, RoomMessageVideo, RoomMessagesError, RoomNameEvent, RoomTopicEvent, RoomUpgradeEvent, StickerEvent,
                 UnknownEncryptedEvent, UnknownEvent)

# SQLite database setup
conn = sqlite3.connect("matrix_rooms.db")
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS rooms (
                room_id TEXT PRIMARY KEY, 
                server_hostname TEXT,
                room_name TEXT,
                topic TEXT,
                snapshot_timestamp INTEGER
                )''')
c.execute('''CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY,
                event_id TEXT UNIQUE,
                room_id TEXT,
                sender TEXT,
                event_type TEXT,
                content TEXT,
                timestamp INTEGER
            )''')
c.execute('''CREATE TABLE IF NOT EXISTS room_members (
                id INTEGER PRIMARY KEY,
                room_id TEXT,
                user_id TEXT,
                server_hostname TEXT,
                snapshot_timestamp INTEGER,
                UNIQUE (room_id, user_id)
            )''')
conn.commit()

# Matrix account credentials
USERNAME = "user1"
PASSWORD = "jkldskjldsajklsda"
HOMESERVER = "https://matrix-client.matrix.org"

# Regular expression to match room IDs
ROOM_ID_REGEX = re.compile(r"([!#][A-Za-z0-9]+:[A-Za-z0-9.-]+)")


def handle_room_message(event, room_id, client):
    event_type = None
    content = None

    if isinstance(event, UnknownEvent):
        event_type = 'unknown'
        content = event.source
    elif isinstance(event, UnknownEncryptedEvent):
        event_type = 'unknown_encrypted'
        content = event.source
    elif isinstance(event, MegolmEvent):
        event_type = 'megolm'
        content = event.source
    elif isinstance(event, CallEvent):
        event_type = 'call'
        content = event.source
    elif isinstance(event, RoomEncryptionEvent):
        event_type = 'encryption_enabled'
        content = event.source
    elif isinstance(event, RoomCreateEvent):
        event_type = 'room_create'
        content = event.source
    elif isinstance(event, RoomGuestAccessEvent):
        event_type = 'guest_access'
        content = event.guest_access
    elif isinstance(event, RoomJoinRulesEvent):
        event_type = 'join_rules'
        content = event.join_rule
    elif isinstance(event, RoomHistoryVisibilityEvent):
        event_type = 'history_visibility'
        content = event.history_visibility
    elif isinstance(event, RoomNameEvent):
        event_type = 'room_name'
        content = event.source
    elif isinstance(event, RoomTopicEvent):
        event_type = 'room_name'
        content = event.source
    elif isinstance(event, RoomAvatarEvent):
        event_type = 'room_avatar'
        content = event.avatar_url
    elif isinstance(event, RoomMemberEvent):
        event_type = 'member'
        content = event.source
    elif isinstance(event, RoomMessageText):
        event_type = 'message'
        content = event.body
        # Add any room IDs we find in the message to our database
        room_ids = re.findall(ROOM_ID_REGEX, event.body)
        for new_room_id in room_ids:
            print('Found a room!')
            insert_room(client.rooms[new_room_id])
            # new_room_id = new_room_id[0] if new_room_id[0] else new_room_id[1]
            if new_room_id not in client.rooms:
                asyncio.create_task(join_room(client, new_room_id))
    elif isinstance(event, RoomMessageEmote):
        event_type = 'emote'
        content = event.body
    elif isinstance(event, RoomMessageNotice):
        event_type = 'notice'
        content = event.body
    elif isinstance(event, RoomMessageUnknown):
        event_type = 'unknown_msg'
        content = event.source
    elif isinstance(event, PowerLevelsEvent):
        event_type = 'power_levels'
        content = str(event.power_levels)
    elif isinstance(event, RedactionEvent):
        event_type = 'redaction'
        content = {'redacts': event.redacts, 'reason': event.reason}
    elif isinstance(event, StickerEvent):
        event_type = 'sticker'
        content = event.source
    elif isinstance(event, RoomUpgradeEvent):
        event_type = 'room_upgrade'
        content = {'body': event.body, 'replacement_room': event.replacement_room}
    elif isinstance(event, RoomMessageImage):
        event_type = 'message_image'
        content = {'url': event.url, 'body': event.body}
    elif isinstance(event, RoomMessageAudio):
        event_type = 'message_audio'
        content = {'url': event.url, 'body': event.body}
    elif isinstance(event, RoomMessageVideo):
        event_type = 'message_video'
        content = {'url': event.url, 'body': event.body}
    elif isinstance(event, RoomMessageFile):
        event_type = 'message_file'
        content = {'url': event.url, 'body': event.body}
    elif isinstance(event, RoomEncryptedImage):
        event_type = 'message_enc_image'
        content = event.source
    elif isinstance(event, RoomEncryptedAudio):
        event_type = 'message_enc_audio'
        content = event.source
    elif isinstance(event, RoomEncryptedVideo):
        event_type = 'message_enc_video'
        content = event.source
    elif isinstance(event, RoomEncryptedFile):
        event_type = 'message_enc_file'
        content = event.source
    else:
        event_type = 'event_type_not_found'
        content = event.source

    if isinstance(content, dict):
        content = json.dumps(content)

    room_id, server_hostname = sanitize_room_id(room_id)

    # print(event.event_id, room_id, event.sender, event_type, content, event.server_timestamp)

    if event_type:
        try:
            c.execute("INSERT INTO messages (event_id, room_id, sender, event_type, content, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                      (event.event_id, room_id, event.sender, event_type, content, event.server_timestamp))
            conn.commit()
            print(f"Added message: {event.event_id}")
        except sqlite3.IntegrityError:
            # print(f"Message {event.event_id} already exists in the database.")
            pass


def store_room_members(room_id, room):
    snapshot_timestamp = int(time.time())
    for user_id in room.users:
        server_hostname = user_id.split(":")[1]
        try:
            c.execute("INSERT INTO room_members (room_id, user_id, server_hostname, snapshot_timestamp) VALUES (?, ?, ?, ?)",
                      (room_id, user_id, server_hostname, snapshot_timestamp))
            conn.commit()
            print(f"Added member {user_id} to room {room_id}")
        except sqlite3.IntegrityError:
            pass


async def join_room(client, room_id):
    response = await client.join(room_id)
    if isinstance(response, JoinError):
        print(f"Error while joining room {room_id}: {response.message}")
    else:
        print(f"Joined room {room_id}")


def sanitize_room_id(room_id: str):
    if isinstance(room_id, tuple):
        room_id = list(filter(None, list(room_id)))
        if len(room_id) == 1:
            room_id = room_id[0]
            if room_id.startswith('#/#'):
                room_id.replace('#/#', '#')
            elif room_id.startswith('#/!'):
                room_id.replace('#/!', '!')
            server_hostname = room_id.split(":")[1]
        else:
            server_hostname = room_id[0].split(":")[1]
            room_id = json.dumps(room_id)
    else:
        server_hostname = room_id.split(":")[1]
    return room_id, server_hostname


def insert_room(room: MatrixRoom):
    snapshot_timestamp = int(time.time())
    room_name = room.display_name
    topic = room.topic
    room_id, server_hostname = sanitize_room_id(room.room_id)
    try:
        c.execute("INSERT INTO rooms (room_id, server_hostname, room_name, topic, snapshot_timestamp) VALUES (?, ?, ?, ?, ?)", (room_id, server_hostname, room_name, topic, snapshot_timestamp))
        conn.commit()
        print(f"Added room: {room_id}")
    except sqlite3.IntegrityError:
        pass


async def crawl_room_history(client, room_id):
    prev_batch = ''
    while True:
        response = await client.room_messages(room_id=room_id, start=prev_batch, limit=100)

        if isinstance(response, RoomMessagesError):
            print(f"Error while fetching room messages: {response.message}")
            break

        if not response.chunk:
            break

        # Store room members in the database
        room = client.rooms[room_id]
        store_room_members(room_id, room)

        insert_room(room)

        for event in response.chunk:
            handle_room_message(event, room_id, client)

        prev_batch = response.end


async def main():
    client = AsyncClient(HOMESERVER, USERNAME)
    try:
        await client.login(PASSWORD)
        print(f"Logged in as {USERNAME}")

        while True:
            # Sync with the server to get the joined rooms
            await client.sync(timeout=30000)

            # Crawl through the history of each joined room
            for room_id in client.rooms:
                print(f"Crawling room history: {room_id}")
                await crawl_room_history(client, room_id)

            # TODO: parse https://matrix-client.matrix.org/_matrix/client/r0/publicRooms?limit=1000 to get more rooms to crawl
            # TODO: thread the crawlers for each room

            print('Crawl complete!')
            print('===============================')
            print('Sleeping 1 minute...')
            time.sleep(60)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
