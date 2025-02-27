import pytest
from main import app, db, EStatus
from flask_sqlalchemy import SQLAlchemy

app.testing = True

def test_create_demand():
    response = app.test_client().post('/demand', json={'demand_floor': 5})
    assert response.status_code == 200
    assert response.get_json() == {'SUCESS': 'Demand created'}

    response = app.test_client().post('/demand', json={'demand_floor': -5})
    assert response.status_code == 400
    assert response.get_json() == {'FAILED': 'floor is either negative or a non integer'}


def test_create_state():
    response = app.test_client().post('/state', json={"current_floor":0,"vacant":True,"demand_id":1})
    assert response.status_code == 200
    assert response.get_json() == {'SUCESS': 'Status created'}
    response = app.test_client().post('/state', json={"current_floor":7,"vacant":True,"demand_id":1})
    assert response.status_code == 200
    assert response.get_json() == {'SUCESS': 'Status created'}

    response = app.test_client().post('/state', json={"current_floor":-1,"vacant":True,"demand_id":1})
    assert response.status_code == 400
    assert response.get_json() == {'FAILED': 'current_floor is either negative or a non integer'}
    response = app.test_client().post('/state', json={"current_floor":0,"vacant":"true","demand_id":1})
    assert response.status_code == 400
    assert response.get_json() == {'FAILED': 'vacant is not boolean type'}

    

def test_analytics():
    response = app.test_client().get('/data')
    assert response.status_code == 200
    assert response.text == '{"demand_id": 1, "demand_time": "2025-02-26 21:44:49.331687", "status_timediff": 8, "floordiff": 7, "resting_floor": 0, "end_floor": 7}'

def test_parquet():
    response = app.test_client().get('/prqt')
    assert response.status_code == 200
    assert response.mimetype == "application/octet-stream"

