import logging
'''logging用于输出运行日志，与print不同，logging可以设置输出日志的等级，日志保
存路径，日志文件回滚等'''
logging.basicConfig(level=logging.INFO)
'''设置logging消息级别（level），如：debug,info,warning,error,critical.随后，
可以按照级别输出日志'''

import asyncio,os,json,time
from datetime import datetime

from aiohttp import web

def index(request):
    return web.Response(body=b'<h1>Index hello</h1>',content_type='text/html')

async def init():
    app=web.Application()
    app.add_routes([web.get('/',index)])

    runner=web.AppRunner(app)
    await runner.setup()#初始化应用程序应在添加网站前调用
    
    site=web.TCPSite(runner,"127.0.0.1",8000)
    await site.start()

    logging.info('server started at http://127.0.0.1:9000...')
    

loop=asyncio.get_event_loop()
loop.run_until_complete(init())
loop.run_forever()


