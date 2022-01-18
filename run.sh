#!/bin/bash
uvicorn signal_control.http_api.run:app --host 0.0.0.0 --port 8080
