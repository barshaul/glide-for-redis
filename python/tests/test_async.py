import glide
import asyncio

async def test():
     r = await glide.GlideClient.create(glide.GlideClientConfiguration([glide.NodeAddress("localhost", 6379)]))
     print(await r.set("goo", "ge"))
     print(await r.get("goo"))


asyncio.run(test())
