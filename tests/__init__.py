__author__ = 'nico'

import asyncio

if not hasattr(asyncio, 'run'):
	def run(coro):
		loop = asyncio.get_event_loop()
		return loop.run_until_complete(coro)
	asyncio.run = run
