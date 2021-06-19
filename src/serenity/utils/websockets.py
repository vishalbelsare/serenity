import asyncio
import json
import logging
import socket

import websockets
from tau.core import NetworkScheduler, MutableSignal


async def websocket_subscribe_with_retry(ws_uri, timeout: int, logger: logging.Logger, subscribe_msg: dict,
                                         scheduler: NetworkScheduler, messages: MutableSignal, subscription_desc: str,
                                         msg_type: str):
    while True:
        try:
            async with websockets.connect(ws_uri) as sock:
                subscribe_msg_txt = json.dumps(subscribe_msg)
                logger.info(f'sending {msg_type} subscription request for {subscription_desc}')
                await sock.send(subscribe_msg_txt)
                while True:
                    try:
                        async for message in sock:
                            scheduler.schedule_update(messages, message)
                    except BaseException as error:
                        logger.error(f'disconnected; attempting to reconnect after {timeout} '
                                     f'seconds: {error}')
                        await asyncio.sleep(timeout)

                        # exit inner loop
                        break
        except socket.gaierror as error:
            logger.error(f'failed with socket error; attempting to reconnect after {timeout} '
                         f'seconds: {error}')
            await asyncio.sleep(timeout)
            continue
        except ConnectionRefusedError as error:
            logger.error(f'connection refused; attempting to reconnect after {timeout} '
                         f'seconds: {error}')
            await asyncio.sleep(timeout)
            continue
        except BaseException as error:
            logger.error(f'unknown connection error; attempting to reconnect after {timeout} '
                         f'seconds: {error}')
            await asyncio.sleep(timeout)
            continue
