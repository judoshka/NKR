from telethon.sync import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from config import (API_ID, API_HASH, SESSION_STRING)
from report.channels import channels as sources
import async_database
import sync_database
import json
from datetime import datetime, timedelta, timezone
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel
from time import sleep

SOURCE_CHANNELS_IDS = list(sources.values())
SOURCE_CHANNELS = list(sources.keys())
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
client.start()


@client.on(events.NewMessage())
async def handler_new_message(event):
    message = event.message.to_dict()
    await async_database.add_item(message)


async def subscribe(channels_to_subscribe):
    for channel in channels_to_subscribe:
        print(channel)
        try:
            await client(JoinChannelRequest(channel))
        except Exception:
            pass


async def scrape_all(last_record_id_for_channels):
    class DateTimeEncoder(json.JSONEncoder):
        '''Класс для сериализации записи дат в JSON'''
        def default(self, o):
            if isinstance(o, datetime):
                return o.isoformat()
            if isinstance(o, bytes):
                return list(o)
            return json.JSONEncoder.default(self, o)

    for number, channel in enumerate(SOURCE_CHANNELS):
        offset_msg = 0
        limit_msg = 100
        channel_id = int((str(SOURCE_CHANNELS_IDS[number])[3:]))
        i = 0
        flag = True
        while True:
            history = await client(GetHistoryRequest(
                peer=channel,
                offset_id=offset_msg,
                offset_date=None, add_offset=0,
                limit=limit_msg, max_id=0, min_id=0,
                hash=0))
            if not history.messages:
                break
            messages = history.messages
            for message in messages:
                json_message = message.to_dict()
                if json_message["id"] <= last_record_id_for_channels[channel_id]:
                    flag = False
                    break
                if json_message['date'] < datetime(2021, 1, 1, tzinfo=timezone(offset=timedelta())):
                    flag = False
                    break
                json_message["type"] = "new"
                await async_database.add_item(json_message)

            offset_msg = messages[-1].id
            i += 1
            if not flag:
                break


async def get_info(channel):
    entity = await client.get_input_entity(PeerChannel(channel))
    info = await client.get_entity(entity)
    print(info.stringify())


if __name__ == '__main__':
    cases = ["JOIN", "WORK", "SCRAPE", "GET_INfO"]
    case = "SCRAPE"
    if case == "GET_INFO":
        with client:
            client.loop.run_until_complete(get_info())
    elif case == 'JOIN':
        with client:
            client.loop.run_until_complete(subscribe(SOURCE_CHANNELS))
    elif case == 'SCRAPE':
        last_record_id_for_channels = sync_database.get_last_record_id_for_channels(SOURCE_CHANNELS_IDS)
        sleep(3)
        with client:
            client.loop.run_until_complete(scrape_all(last_record_id_for_channels))
    elif case == 'WORK':
        with client:
            client.run_until_disconnected()
