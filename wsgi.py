#!/usr/bin/env python3

import asyncio
import aiohttp_cors

from aiohttp import web
from aiohttp_wsgi import WSGIHandler
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from api import app


@asyncio.coroutine
def setup_db():
    return AsyncIOMotorClient(app.config['MONGO_STATS_URI']).stats_data


class RestHandler:
    @asyncio.coroutine
    def trends(self, req):
        mongo = req.app['db']

        document = yield from mongo.entries.aggregate([{'$project': {'status_code': 1, 'path': 1}},
            {'$match': {'$and': [{'status_code': 200}, {'path': {'$regex': '^\/match'}}]}},
            {'$sortByCount': '$path'}, {'$project': {'count': 1, 'trend': '$_id', '_id': 0}}]).to_list(length=300)

        if not document:
            return web.HTTPNotFound(text='Page not found, yolo!')

        return web.json_response(document)


async def stats(req, res):
    mongo = req.app['db']

    if req.method != 'OPTIONS':
        stats = {}

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

        if 'x-forwarded-for' in req.headers:
            stats['remote_address'] = req.headers['x-forwarded-for']

        if 'x-forwarded-proto' in req.headers:
            stats['scheme'] = req.headers['x-forwarded-proto']

        if 'x-forwarded-host' in req.headers:
            stats['host'] = req.headers['x-forwarded-host']

        if 'x-forwarded-port' in req.headers:
            stats['port'] = req.headers['x-forwarded-port']

        stats['path'] = req.url.path
        stats['query'] = req.url.query_string
        stats['fragment'] = req.url.fragment

        stats['request_method'] = req.method
        stats['status_code'] = res.status
        stats['created'] = datetime.utcnow()

        await mongo.entries.insert_one(stats)


async def on_prepare(req, res):
    await stats(req, res)


loop = asyncio.get_event_loop()

wsgi_handler = WSGIHandler(app)
aio_handler = RestHandler()
aioapp = web.Application()

aioapp['db'] = loop.run_until_complete(setup_db())
aioapp.on_response_prepare.append(on_prepare)

cors = aiohttp_cors.setup(aioapp, defaults={
    "*": aiohttp_cors.ResourceOptions(
        allow_credentials=True,
        expose_headers="*",
        allow_headers="*",
    )
})

trends_resource = cors.add(aioapp.router.add_resource('/trends'))
cors.add(trends_resource.add_route('GET', aio_handler.trends))

aioapp.router.add_route('*', '/{path_info:.*}', wsgi_handler)
