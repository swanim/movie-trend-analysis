#!/usr/bin/bash

source project/venv/bin/activate
python3 project/create_daily_movie_data.py
python3 project/create_pred_data.py
python3 project/make_pred.py