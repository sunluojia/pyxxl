import pytest
from aiohttp import web
from pytest_aiohttp.plugin import AiohttpClient

from pyxxl import XXL


@pytest.mark.asyncio
async def test_param_admin_url():
    # 兼容用户常见的 admin 地址填写方式。
    clients = [
        XXL("http://localhost:8080/xxl-job-admin/api/"),
        XXL("https://localhost:8080/xxl-job-admin/api/"),
        XXL("http://localhost:8080/xxl-job-admin"),
        XXL("http://localhost:8080/xxl-job-admin/api"),
        XXL("http://localhost:8080/xxl-job-admin,http://127.0.0.1:8081/xxl-job-admin/api"),
    ]
    for client in clients:
        await client.close()

    with pytest.raises(ValueError):
        XXL("htp://localhost:8080/xxl-job-admin/api/")


@pytest.mark.asyncio
async def test_client(aiohttp_client: AiohttpClient) -> None:
    async def moke_registry_api(request: web.Request):
        data = await request.json()
        if data.get("registryKey") == "server_test":
            return web.json_response({"code": 500, "msg": "1"}, status=500)

        if data.get("registryKey") == "status_test":
            return web.json_response({"code": 500, "msg": "1"}, status=200)

        return web.json_response({"code": 200, "msg": "1"})

    async def moke_callback_api(request: web.Request):
        return web.json_response({"code": 200, "msg": "1"})

    app = web.Application()
    app.router.add_post("/xxl-job-admin/api/registry", moke_registry_api)
    app.router.add_post("/xxl-job-admin/api/callback", moke_callback_api)
    session = await aiohttp_client(app)
    xxl_client = XXL("http://localhost:8080/xxl-job-admin/api/", session=session)
    # 注册执行器
    assert await xxl_client.registry("key", "value")
    assert not (await xxl_client.registry("server_test", "value"))
    assert not (await xxl_client.registry("status_test", "value"))
    # 回调执行结果
    await xxl_client.callback(123, 123123123)
    await xxl_client.close()


@pytest.mark.asyncio
async def test_client_multi_admin_failover(aiohttp_client: AiohttpClient, unused_tcp_port_factory) -> None:
    # 多 admin 地址应按顺序故障转移，并在找到可用节点后立即停止。
    calls = []

    async def ok_registry_api(request: web.Request):
        calls.append(("registry", await request.json()))
        return web.json_response({"code": 200, "msg": "1"})

    async def ok_registry_remove_api(request: web.Request):
        calls.append(("registryRemove", await request.json()))
        return web.json_response({"code": 200, "msg": "1"})

    async def ok_callback_api(request: web.Request):
        calls.append(("callback", await request.json()))
        return web.json_response({"code": 200, "msg": "1"})

    app = web.Application()
    app.router.add_post("/xxl-job-admin/api/registry", ok_registry_api)
    app.router.add_post("/xxl-job-admin/api/registryRemove", ok_registry_remove_api)
    app.router.add_post("/xxl-job-admin/api/callback", ok_callback_api)
    session = await aiohttp_client(app)
    unavailable_url = f"http://127.0.0.1:{unused_tcp_port_factory()}/xxl-job-admin"
    available_url = str(session.make_url("/xxl-job-admin"))
    xxl_client = XXL([unavailable_url, available_url], retry_times=1)

    assert await xxl_client.registry("key", "value")
    await xxl_client.callback(123, 123123123, code=200, msg="ok")
    await xxl_client.registryRemove("key", "value")
    await xxl_client.close()

    assert calls[0] == (
        "registry",
        {"registryGroup": "EXECUTOR", "registryKey": "key", "registryValue": "value"},
    )
    assert calls[1] == (
        "callback",
        [
            {
                "logId": 123,
                "logDateTim": 123123123,
                "handleCode": 200,
                "handleMsg": "ok",
                "executeResult": {"code": 200, "msg": "ok"},
            }
        ],
    )
    assert calls[2] == (
        "registryRemove",
        {"registryGroup": "EXECUTOR", "registryKey": "key", "registryValue": "value"},
    )
