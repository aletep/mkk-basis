import asyncio
from uuid import uuid4

import httpx

from mkk_basis.bootstrap.demo import app


async def test_acceptance_gate_blocks_until_explicit_release():
    gate = "test-" + uuid4().hex
    event = {"event_id": str(uuid4()), "payment_id": str(uuid4()), "status": "succeeded"}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://demo"
    ) as client:
        task = asyncio.create_task(client.post(f"/webhook?gate={gate}", json=event))
        try:
            async with asyncio.timeout(2):
                # Observe the HTTP contract used by the external acceptance runner.
                while not (await client.get(f"/gates/{gate}")).json()["entered"]:  # noqa: ASYNC110
                    await asyncio.sleep(0)
            assert not task.done()
            assert (await client.get(f"/gates/{gate}")).json() == {
                "entered": 1,
                "released": False,
            }
            await client.post(f"/gates/{gate}/release")
            assert (await task).status_code == 200
            assert (await client.get(f"/gates/{gate}")).json()["released"]
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
