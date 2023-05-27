#
# Copyright (c) 2023, Jinhua Luo (kingluo) luajit.io@gmail.com
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
from cffi import FFI

ffi = FFI()
ffi.cdef(
    """
void* malloc(size_t);
void *memcpy(void *dest, const void *src, size_t n);
void* ngx_http_lua_ffi_task_poll(void *p);
char* ngx_http_lua_ffi_get_req(void *tsk, int *len);
void ngx_http_lua_ffi_respond(void *tsk, int rc, char* rsp, int rsp_len);
"""
)
C = ffi.dlopen(None)

import traceback
from enum import Enum
from ariadne import graphql, gql, ObjectType, make_executable_schema
import httpx
import json
import asyncio
import threading
import jq


class RESTResolver:
    def __init__(self, cfg):
        self.cfg = cfg

    async def __call__(self, obj, info, **params):
        cfg = self.cfg
        data = None
        if cfg.get("send_json_body"):
            data = json.dumps(params)
            params = None
        method = cfg.get("method") or "get"
        datasource = cfg["datasource"]
        client = datasource["client"]
        r = await client.request(
            method,
            datasource["host"] + cfg["uri"],
            params=params,
            json=data,
            headers=cfg.get("headers"),
        )
        res = r.json()
        if cfg.get("jq"):
            return jq.compile(cfg["jq"]).input(res).first()
        return res


def create_schema(cfg):
    type_defs = gql(cfg["schema"])

    tts = []
    for typ, fields in cfg["resolvers"].items():
        tt = ObjectType(typ)
        tts.append(tt)
        for f, c in fields.items():
            ds = c["datasource"] = cfg["datasources"][c["datasource"]]
            assert ds["@type"] == "http"
            ds["client"] = httpx.AsyncClient(verify=ds.get("verify") or True)
            tt.set_field(f, RESTResolver(c))

    schema = make_executable_schema(type_defs, *tts)
    return schema


class Schema:
    def __init__(self, schema, cfg):
        self.schema = schema
        self.cfg = cfg

    async def close(self):
        for _, d in self.cfg["datasources"].items():
            await d["client"].aclose()


class CMD(Enum):
    NEW_SCHEMA = 1
    CLOSE_SCHEMA = 2
    QUERY = 3


class State:
    def __init__(self, cfg):
        self.schemas = {}
        self.idx = 0
        self.loop = asyncio.new_event_loop()
        t = threading.Thread(target=self.loop.run_forever)
        t.daemon = True
        t.start()
        self.event_loop_thread = t

    async def close_schema(self, req, task):
        idx = req["schema"]
        schema = self.schemas[idx]
        del self.schemas[idx]
        await schema.close()
        C.ngx_http_lua_ffi_respond(task, 0, ffi.NULL, 0)

    async def new_schema(self, req, task):
        self.idx += 1
        idx = self.idx

        cfg = req["data"]
        schema = create_schema(cfg)
        schema = Schema(schema, cfg)

        self.schemas[idx] = schema
        data = json.dumps({"schema": idx})
        res = C.malloc(len(data))
        C.memcpy(res, data.encode(), len(data))
        C.ngx_http_lua_ffi_respond(task, 0, res, len(data))

    async def query(self, req, task):
        idx = req["schema"]
        schema = self.schemas[idx]

        res = await graphql(schema.schema, req["data"])

        data = json.dumps(res)
        res = C.malloc(len(data))
        C.memcpy(res, data.encode(), len(data))
        C.ngx_http_lua_ffi_respond(task, 0, res, len(data))

    async def dispatch(self, req, task):
        try:
            cmd = CMD(req["cmd"]).name.lower()
            return await getattr(self, cmd)(req, task)
        except Exception as exc:
            tb = traceback.format_exc()
            print(tb)
            res = C.malloc(len(tb))
            C.memcpy(res, tb.encode(), len(tb))
            C.ngx_http_lua_ffi_respond(task, 1, res, 0)

    async def close(self, req, task):
        for _, schema in self.schemas:
            await schema.close()
        self.loop.stop()

    def poll(self, tq):
        while True:
            task = C.ngx_http_lua_ffi_task_poll(ffi.cast("void*", tq))
            if task == ffi.NULL:
                asyncio.run_coroutine_threadsafe(self.close(req, task), self.loop)
                self.event_loop_thread.join()
                break
            r = C.ngx_http_lua_ffi_get_req(task, ffi.NULL)
            req = json.loads(ffi.string(r))
            asyncio.run_coroutine_threadsafe(self.dispatch(req, task), self.loop)


def init(cfg, tq):
    data = ffi.string(ffi.cast("char*", cfg))
    cfg = json.loads(data)
    st = State(cfg)
    t = threading.Thread(target=st.poll, args=(tq,))
    t.daemon = True
    t.start()
    return 0
