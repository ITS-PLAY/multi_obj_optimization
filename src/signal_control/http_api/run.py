import json
import os

from fastapi import FastAPI, File, UploadFile, Form
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse

from algorithm import traffic_flow, traffic_timing
from signal_control.http_api.exception import ServerError
from signal_control.http_api.util import gen_uuid
from signal_control.http_api.config import DATA_PATH
from signal_control.log import LOG

app = FastAPI()


class Response(object):
    def __init__(self, data=None, code=200, message='success'):
        self.code = code
        self.message = message
        self.data = data


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    error = exc.errors()[0]
    msg = '.'.join(error['loc']) + ': ' + error['msg']
    return JSONResponse(
        jsonable_encoder(Response(code=400, message=msg)), 400)


@app.exception_handler(ServerError)
async def validation_exception_handler(request, exc):
    return JSONResponse(
        jsonable_encoder(Response(code=500, message='服务器错误')), 500)


@app.post("/api/recommendation")
async def create_recommendation(
        cross_id: str = Form(...),
        config: str = Form(...),
        flow_file: UploadFile = File(...),
        traffic_light_file: UploadFile = File(...)
):
    # 存入文件
    light_fn = os.path.join(DATA_PATH, gen_uuid())
    with open(light_fn, 'x') as f:
        f.write((await traffic_light_file.read()).decode('utf-8'))
    flow_fn = os.path.join(DATA_PATH, gen_uuid())
    with open(flow_fn, 'x') as f:
        f.write((await flow_file.read()).decode('utf-8'))
    params = json.loads(config)
    LOG.info("file save done")

    # 调用算法
    vehicle_flow = traffic_flow.Traffic_Flow(
        flow_fn, light_fn, params, cross_id)
    vehicle_flow.generate_flow()
    traffic_time = traffic_timing.TrafficTiming(
        vehicle_flow.flows, light_fn, params, cross_id)
    traffic_time.auto_timing()
    plan_no, cycle, result = traffic_time.return_phase_plan()
    LOG.info("algorithm done")

    # 删除文件
    os.remove(light_fn)
    os.remove(flow_fn)
    LOG.info("file removed")
    return result
