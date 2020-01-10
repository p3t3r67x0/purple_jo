#!/usr/bin/env python3

import asyncio

from aiohttp import web
from aiohttp_wsgi import WSGIHandler
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from api import app


async def init_mongo(loop):
    conn = AsyncIOMotorClient('mongodb://127.0.0.1:27017', io_loop=loop)

    return conn['stats_data']


async def setup_mongo(aioapp, loop):
    mongo = await init_mongo(loop)

    async def close_mongo(aioapp):
        mongo.client.close()

    aioapp.on_cleanup.append(close_mongo)

    return mongo


async def stats(req, res):
    if req.method != 'OPTIONS':
        stats = {}

        stats['host'] = req.url.host
        stats['port'] = req.url.port
        stats['path'] = req.url.path
        stats['scheme'] = req.url.scheme
        stats['query'] = req.url.query_string
        stats['fragment'] = req.url.fragment

        if 'origin' in req.headers:
            stats['origin'] = req.headers['origin']

        if 'accept' in req.headers:
            stats['accept'] = req.headers['accept']

        if 'referer' in req.headers:
            stats['referer'] = req.headers['referer']

        if 'connection' in req.headers:
            stats['connection'] = req.headers['connection']

        if 'user-agent' in req.headers:
            stats['user_agent'] = req.headers['user-agent']

        if 'accept-language' in req.headers:
            stats['accept_language'] = req.headers['accept-language']

        if 'accept-encoding' in req.headers:
            stats['accept_encoding'] = req.headers['accept-encoding']

        stats['remote_address'] = req.remote
        stats['request_method'] = req.method
        stats['status_code'] = res.status

        await mongo.entries.insert_one(stats)


async def on_prepare(req, res):
    await stats(req, res)


async def init(app):
    wsgi_handler = WSGIHandler(app)
    aioapp = web.Application()
    aioapp.on_response_prepare.append(on_prepare)
    aioapp.router.add_route('*', '/{path_info:.*}', wsgi_handler)
    mongo = await setup_mongo(aioapp, asyncio.get_running_loop())

    return aioapp, mongo


loop = asyncio.get_event_loop()
app, mongo = loop.run_until_complete(init(app))
