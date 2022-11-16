import dynaconf
import asyncio
import vast

async def historical_query(config: dynaconf.Dynaconf):
    v = vast.VAST()
    result = await v.export(expression = '#type == \"suricata.alert\"', limit = 10)
    print(result)

def run():
    print("hello TheHive")
    asyncio.run(historical_query(dynaconf.Dynaconf()))
    print("hello, again")
